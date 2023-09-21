"""Test pywwa/__init__.py"""
import os
import shutil

import pytest
import pywwa


def test_invalid_filename():
    """Test that exception is raised"""
    with pytest.raises(FileNotFoundError):
        pywwa.get_table_filepath("foo.txt")


def test_load_settings():
    """Test that we can load settings."""
    if os.path.isfile("settings.json"):
        shutil.move("settings.json", "settings.json.save")
    res = pywwa.load_config()
    assert isinstance(res, dict)
    if os.path.isfile("settings.json.save"):
        shutil.move("settings.json.save", "settings.json")
