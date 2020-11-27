"""pyWWA local module."""
# stdlib
import json
import os

# Shared configuration?
CONFIG = json.load(
    open(os.path.join(os.path.dirname(__file__), "../../settings.json"))
)
SETTINGS = {}
