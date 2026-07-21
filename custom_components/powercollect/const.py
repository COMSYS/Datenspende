"""Constants for the PowerCollect integration."""

DOMAIN = "powercollect"
NAME = "COMSYS Datenspende"

# Offline submission cache
MAX_CACHED_READINGS = 10_000
STORAGE_VERSION = 1

# Readings are submitted in batches rather than one request per reading. The interval is
# drawn anew from this range before every flush, which spreads the submissions of all
# installations out over time instead of aligning them on a shared tick.
BATCH_INTERVAL_MIN = 60
BATCH_INTERVAL_MAX = 90

# Readings per request. The server rejects batches above 10000.
MAX_BATCH_READINGS = 5_000
