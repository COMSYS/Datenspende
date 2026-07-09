"""Constants for the PowerCollect integration."""

from datetime import timedelta

DOMAIN = "powercollect"
NAME = "COMSYS Datenspende"

# Offline submission cache
MAX_CACHED_READINGS = 10_000
CACHE_FLUSH_INTERVAL = timedelta(minutes=5)
STORAGE_VERSION = 1
