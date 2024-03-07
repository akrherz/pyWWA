"""Test the CLI main function on all workflows."""

import importlib
import pkgutil

import pytest
import pywwa.workflows
from click.testing import CliRunner
from pywwa.workflows.cli import main as cli_main


def find_mains():
    """yield all main functions within pywwa.workflows."""
    package = pywwa.workflows
    for _importer, modname, _ispkg in pkgutil.iter_modules(package.__path__):
        module = importlib.import_module(package.__name__ + "." + modname)
        for name in dir(module):
            if name.startswith("_"):
                continue
            obj = getattr(module, name)
            if not callable(obj):
                continue
            if not hasattr(obj, "main"):
                continue
            yield obj


@pytest.mark.parametrize("mainmethod", find_mains())
def test_exercise_api(mainmethod):
    """Exercise the main() method via click."""
    runner = CliRunner()
    result = runner.invoke(mainmethod, ["-x"])
    if result.exception:
        raise result.exception
    assert result.exit_code == 0


def test_cli_with_jabber():
    """Test the CLI main function."""
    runner = CliRunner()
    result = runner.invoke(cli_main)
    if result.exception:
        raise result.exception
    assert result.exit_code == 0
