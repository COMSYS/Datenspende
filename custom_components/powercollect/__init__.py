"""The PowerCollect integration."""

from datetime import datetime
import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import STATE_UNAVAILABLE, STATE_UNKNOWN, Platform, UnitOfPower
from homeassistant.core import Event, EventStateChangedData, HomeAssistant, callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.event import async_call_later, async_track_state_change_event
from homeassistant.util.unit_conversion import PowerConverter

from .api import (
    POWERCOLLECT_BASE_URL,
    PowerCollectAPI,
    PowerCollectAuthError,
    PowerCollectConnError,
    PowerCollectError,
)
from .config_flow import MeterListEntry

_LOGGER = logging.getLogger(__name__)

_PLATFORMS: list[Platform] = []

type PowerCollectConfigEntry = ConfigEntry[PowerCollectAPI]


async def async_setup_entry(
    hass: HomeAssistant, entry: PowerCollectConfigEntry
) -> bool:
    """Set up PowerCollect from a config entry."""

    api = PowerCollectAPI(
        base_url=POWERCOLLECT_BASE_URL,
        api_key=entry.data["api_key"],
        client_id=entry.data["clientId"],
        session=async_get_clientsession(hass),
    )

    try:
        await api.get_client_id()
    except PowerCollectAuthError as e:
        _LOGGER.error("Authentication error: %s", e)
        return False
    except PowerCollectConnError as e:
        _LOGGER.error("Connection error: %s", e)
        return False

    entry.runtime_data = api
    await hass.config_entries.async_forward_entry_setups(entry, _PLATFORMS)

    meters = [
        MeterListEntry(
            meter_id=m["meter_id"],
            name=m["name"],
            vendor=m["vendor"],
            model=m["model"],
            entity_ids=m["entity_ids"],
        )
        for m in entry.data.get("meters", [])
    ]

    # Holds the active 2-second countdown timers for each meter to group rapid updates
    debounce_timers = {}

    @callback
    def state_handler(event: Event[EventStateChangedData]) -> None:
        """Handle state changes for observed entities."""
        entity_id = event.data.get("entity_id")
        state = event.data.get("new_state")

        if state is None or state.state in (STATE_UNAVAILABLE, STATE_UNKNOWN):
            return

        # 1. Find which meter triggered this event
        target_meter = None
        for meter_entry in meters:
            if entity_id in meter_entry.entity_ids:
                target_meter = meter_entry
                break

        if not target_meter:
            return

        meter_id = target_meter.meter_id

        # 2. DEBOUNCE: If a timer is already running for this meter, cancel it!
        if meter_id in debounce_timers:
            debounce_timers[meter_id]()  # Calling the unsubscribe function

        # 3. Build and send the payload
        @callback
        def _collect_and_send(now: datetime) -> None:
            # Clear the timer record
            debounce_timers.pop(meter_id, None)

            current_payload = {}

            # Loop through ALL entities assigned to this meter to build a complete snapshot
            for e_id in target_meter.entity_ids:
                state_obj = hass.states.get(e_id)
                if state_obj and state_obj.state not in (
                    STATE_UNAVAILABLE,
                    STATE_UNKNOWN,
                ):
                    try:
                        val = float(state_obj.state)
                        d_class = state_obj.attributes.get("device_class")

                        if d_class == "power":
                            raw_unit = state_obj.attributes.get(
                                "unit_of_measurement", UnitOfPower.WATT
                            )
                            current_payload["power"] = PowerConverter.convert(
                                val, raw_unit, UnitOfPower.WATT
                            )
                        elif d_class == "energy":
                            current_payload["energy"] = val
                        elif d_class == "voltage":
                            current_payload["voltage"] = val

                    except ValueError:
                        pass  # Ignore non-numeric states

            if not current_payload:
                return

            # 4. Send the background API task
            async def _submit():
                try:
                    # Format: 2024-06-01T12:00:00Z
                    iso_timestamp = (
                        now.replace(microsecond=0).isoformat().replace("+00:00", "Z")
                    )

                    await api.submit_data(
                        meterId=meter_id, timestamp=iso_timestamp, **current_payload
                    )
                except PowerCollectError as e:
                    _LOGGER.error("Failed to submit data: %s", e)

            entry.async_create_background_task(
                hass, _submit(), name=f"powercollect_push_{meter_id}"
            )

        # Start the 2-second countdown timer
        debounce_timers[meter_id] = async_call_later(hass, 0.1, _collect_and_send)

    # Build the master list of entities to listen to
    entities_to_track = [
        entity_id for meter in meters for entity_id in meter.entity_ids
    ]

    if entities_to_track:
        unsubscribe = async_track_state_change_event(
            hass,
            entities_to_track,
            state_handler,
        )
        entry.async_on_unload(unsubscribe)

    # Clean up any timers if the integration is unloaded
    @callback
    def _cancel_timers() -> None:
        for cancel_timer in debounce_timers.values():
            cancel_timer()

    entry.async_on_unload(_cancel_timers)

    return True


async def async_unload_entry(
    hass: HomeAssistant, entry: PowerCollectConfigEntry
) -> bool:
    """Unload a config entry."""
    return await hass.config_entries.async_unload_platforms(entry, _PLATFORMS)
