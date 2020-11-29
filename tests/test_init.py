"""Test pywwa/__init__.py"""
# stdlin
import os
import shutil

# Local
import pywwa


def test_load_settings():
    """Test that we can load settings."""
    if os.path.isfile("settings.json"):
        shutil.move("settings.json", "settings.json.save")
    res = pywwa.load_config()
    assert isinstance(res, dict)
    if os.path.isfile("settings.json.save"):
        shutil.move("settings.json.save", "settings.json")
