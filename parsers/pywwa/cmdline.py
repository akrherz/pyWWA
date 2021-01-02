"""Our standardized pyWWA command line."""
# stdlib
import argparse
from datetime import datetime, timezone
import os


def parse_cmdline(argv):
    """Parse command line for context settings."""
    parser = argparse.ArgumentParser(description="pyWWA Parser.")
    parser.add_argument(
        "-c",
        "--custom-args",
        type=str,
        nargs="+",
        help="Pass custom arguments to this parser.",
    )
    parser.add_argument(
        "-d",
        "--disable-dbwrite",
        action="store_true",
        help=(
            "Disable any writing to databases, still may need read access "
            "to initialize metadata tables."
        ),
    )
    parser.add_argument(
        "-e",
        "--disable-email",
        action="store_true",
        help="Disable sending any emails.",
    )
    parser.add_argument(
        "-l",
        "--stdout-logging",
        action="store_true",
        help="Also log to stdout.",
    )
    parser.add_argument(
        "-s",
        "--shutdown-delay",
        type=int,
        help=(
            "Number of seconds to wait before shutting down process when "
            "STDIN is closed to the process.  0 is immediate."
        ),
    )

    def _parsevalid(val):
        """Convert to datetime."""
        v = datetime.strptime(val[:16], "%Y-%m-%dT%H:%M")
        return v.replace(tzinfo=timezone.utc)

    parser.add_argument(
        "-u",
        "--utcnow",
        type=_parsevalid,
        metavar="YYYY-MM-DDTHH:MI",
        help="Provide the current UTC Timestamp (defaults to realtime.).",
    )
    parser.add_argument(
        "-x",
        "--disable-xmpp",
        action="store_true",
        help="Disable all XMPP functionality.",
    )
    # HACK not to do things during testing.
    args = argv[1:]
    if os.path.basename(argv[0]) == "pytest":
        args = []
    return parser.parse_args(args)
