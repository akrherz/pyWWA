"""pyWWA local module."""
# stdlib
import json
import os

# Shared configuration
SETTINGS = {}

try:
    CONFIG = json.load(
        open(os.path.join(os.path.dirname(__file__), "../../settings.json"))
    )
except FileNotFoundError:
    CONFIG = {}
