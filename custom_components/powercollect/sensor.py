"""Sensor platform for the PowerCollect integration."""

from homeassistant.components.sensor import SensorEntity, SensorStateClass
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceEntryType, DeviceInfo
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from .cache import SubmissionCache
from .const import DOMAIN, NAME
from .data import PowerCollectConfigEntry


async def async_setup_entry(
    hass: HomeAssistant,
    entry: PowerCollectConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up the PowerCollect sensors."""
    async_add_entities([DonatedDataPointsSensor(entry)])


class DonatedDataPointsSensor(SensorEntity):
    """Counts the readings the PowerCollect server has accepted."""

    _attr_has_entity_name = True
    _attr_should_poll = False
    _attr_state_class = SensorStateClass.TOTAL_INCREASING
    _attr_translation_key = "donated_data_points"

    def __init__(self, entry: PowerCollectConfigEntry) -> None:
        """Initialize the sensor."""
        self._cache: SubmissionCache = entry.runtime_data.cache
        self._attr_unique_id = f"{entry.entry_id}_donated_data_points"
        self._attr_device_info = DeviceInfo(
            name=NAME,
            identifiers={(DOMAIN, entry.entry_id)},
            entry_type=DeviceEntryType.SERVICE,
        )

    @property
    def native_value(self) -> int:
        """Return the number of donated data points."""
        return self._cache.donated

    async def async_added_to_hass(self) -> None:
        """Subscribe to donation count updates."""
        self.async_on_remove(self._cache.async_add_listener(self.async_write_ha_state))
