"""Runtime data structures for the PowerCollect integration."""

from dataclasses import dataclass

from homeassistant.config_entries import ConfigEntry

from .api import PowerCollectAPI
from .cache import SubmissionCache


@dataclass
class PowerCollectData:
    """Runtime data held by a PowerCollect config entry."""

    api: PowerCollectAPI
    cache: SubmissionCache


type PowerCollectConfigEntry = ConfigEntry[PowerCollectData]
