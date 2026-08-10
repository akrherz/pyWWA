"""Process an SSE feed from the FAA.

The feed is emitted as SSE-style text blocks over HTTP. This script collects
product payloads in memory, converts them into NOAAPort-like text, and
periodically writes batches to a local file for testing.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import subprocess
import tempfile
from collections.abc import Iterable, Iterator
from pathlib import Path
from urllib.request import Request, urlopen

from pyiem.util import noaaport_text

LOG = logging.getLogger(__name__)
DEFAULT_BATCH_SIZE = 100
DEFAULT_OUTPUT = Path("wmscr_ingest.txt")
DEFAULT_URL = os.environ.get(
    "FAA_WMSCR_SSE_URL",
    os.environ.get("FAA_WMSCR_WEBSOCKET_URL", ""),
)


def iter_events(message: str) -> Iterator[dict]:
    """Yield JSON payloads from an SSE event block."""
    message = message.replace("\r\n", "\n").replace("\r", "\n")
    for block in message.split("\n\n"):
        lines = [
            line for line in block.splitlines() if line.startswith("data:")
        ]
        if not lines:
            continue
        payload = "\n".join(line[5:].lstrip() for line in lines)
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            LOG.debug("Skipping non-JSON payload: %s", payload[:120])
            continue
        if parsed.get("heartbeat"):
            continue
        yield parsed


def iter_stream_events(stream: Iterable[bytes | str]) -> Iterator[dict]:
    """Yield parsed JSON payloads from a streaming SSE response."""
    lines: list[str] = []
    for raw_line in stream:
        if isinstance(raw_line, bytes):
            line = raw_line.decode("utf-8", errors="replace")
        else:
            line = raw_line
        line = line.rstrip("\n").rstrip("\r")
        if not line:
            if lines:
                yield from iter_events("\n".join(lines))
                lines.clear()
            continue
        lines.append(line)
    if lines:
        yield from iter_events("\n".join(lines))


def _write_temp_product(text: str, encoding: str) -> Path:
    """Write a product to a temporary file and return its path."""
    with tempfile.NamedTemporaryFile(mode="wb", delete=False) as tmp:
        tmp.write(text.encode(encoding, errors="replace"))
        tmp.flush()
        return Path(tmp.name)


async def send_to_ldm(record: dict) -> bool:
    """Convert an FAA JSON payload into NOAAPort-like text."""
    payload = record.get("payload")
    if not payload:
        return False
    # Strip the start of message payload
    if not payload[0].isascii():
        payload = payload[1:]
    # Rectify to Unix newlines
    lines = payload.replace("\r", "").split("\n")
    # Remove any empty lines
    lines = [line for line in lines if line.strip()]
    # Minimal required line count
    if len(lines) < 2:
        return False
    # prepare the text
    text = noaaport_text("\n".join(lines))
    encoding = record.get("encoding", "utf-8")
    tmp_path = await asyncio.to_thread(_write_temp_product, text, encoding)
    try:
        proc = await asyncio.create_subprocess_exec(
            "/home/meteor_ldm/bin/pqinsert",
            "-f",
            "EXP",
            "-i",
            "-p",
            lines[0].strip(),
            str(tmp_path),
        )
        await proc.wait()
        if proc.returncode:
            raise subprocess.CalledProcessError(
                proc.returncode,
                [
                    "/home/meteor_ldm/bin/pqinsert",
                    "-f",
                    "EXP",
                    "-i",
                    "-p",
                    lines[0].strip(),
                    str(tmp_path),
                ],
            )
    finally:
        await asyncio.to_thread(tmp_path.unlink, True)
    return True


def write_batch(batch: list[tuple[str, str]], output: Path) -> None:
    """Append a batch of NOAAPort-like payloads to disk."""
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("ab") as fh:
        for text, encoding in batch:
            fh.write(text.encode(encoding, errors="replace"))


def _next_record(records: Iterator[dict]) -> dict | None:
    """Return the next record or ``None`` when exhausted."""
    return next(records, None)


async def ingest(url: str, output: Path, batch_size: int) -> None:
    """Connect, buffer payloads, and flush them in batches."""
    batch: list[tuple[str, str]] = []
    request = Request(
        url,
        headers={
            "Accept": "text/event-stream",
            "Cache-Control": "no-cache",
        },
    )
    with urlopen(request) as response:
        LOG.info(
            "Connected to %s with content-type %s",
            url,
            response.headers.get("Content-Type", "unknown"),
        )
        try:
            records = iter_stream_events(response)
            while True:
                record = await asyncio.to_thread(_next_record, records)
                if record is None:
                    break
                if not await send_to_ldm(record):
                    LOG.debug(
                        "Skipping payload-less record %s",
                        record.get("msg_id", "unknown"),
                    )
                    continue
                if len(batch) >= batch_size:
                    write_batch(batch, output)
                    LOG.info("Flushed %s records to %s", len(batch), output)
                    batch.clear()
        finally:
            if batch:
                write_batch(batch, output)
                LOG.info("Flushed final %s records to %s", len(batch), output)


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--url",
        default=DEFAULT_URL,
        help="HTTP SSE URL for the FAA feed.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Local file to append NOAAPort-like payloads to.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Number of payloads to buffer before flushing.",
    )
    return parser


def main() -> None:
    """Go main go."""
    parser = build_parser()
    args = parser.parse_args()
    if not args.url:
        parser.error(
            "--url is required or set FAA_WMSCR_SSE_URL "
            "(FAA_WMSCR_WEBSOCKET_URL also supported)"
        )
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    asyncio.run(ingest(args.url, args.output, args.batch_size))


if __name__ == "__main__":
    main()
