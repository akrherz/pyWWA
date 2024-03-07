"""Test pywwa/__init__.py"""

import os
import shutil

import pytest
import pywwa


def test_invalid_filename():
    """Test that exception is raised"""
    with pytest.raises(FileNotFoundError):
        pywwa.get_data_filepath("foo.txt")


def test_load_settings():
    """Test that we can load settings."""
    if os.path.isfile(pywwa.SETTINGS_FN):
        shutil.move(pywwa.SETTINGS_FN, f"{pywwa.SETTINGS_FN}.save")
    pywwa.load_settings()
    assert isinstance(pywwa.SETTINGS, dict)
    if os.path.isfile(f"{pywwa.SETTINGS_FN}.save"):
        shutil.move(f"{pywwa.SETTINGS_FN}.save", pywwa.SETTINGS_FN)
