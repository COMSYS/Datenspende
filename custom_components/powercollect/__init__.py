"""The PowerCollect integration."""

from datetime import datetime
import logging
from random import uniform

from homeassistant.const import STATE_UNAVAILABLE, STATE_UNKNOWN, Platform, UnitOfPower
from homeassistant.core import (
    CALLBACK_TYPE,
    Event,
    EventStateChangedData,
    HomeAssistant,
    callback,
)
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.event import (
    async_call_later,
    async_track_state_change_event,
)
from homeassistant.util.unit_conversion import PowerConverter

from .api import (
    POWERCOLLECT_BASE_URL,
    PowerCollectAPI,
    PowerCollectAuthError,
    PowerCollectConnError,
)
from .cache import SubmissionCache, async_remove_cache
from .config_flow import MeterListEntry
from .const import BATCH_INTERVAL_MAX, BATCH_INTERVAL_MIN
from .data import PowerCollectConfigEntry, PowerCollectData

_LOGGER = logging.getLogger(__name__)

_PLATFORMS: list[Platform] = [Platform.SENSOR]


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

    # Readings that could not be submitted (offline/server down) wait here
    # and are resubmitted once the server is reachable again.
    cache = SubmissionCache(hass, entry.entry_id, api)
    await cache.async_load()

    entry.runtime_data = PowerCollectData(api=api, cache=cache)
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

    # Holds the active debounce timers for each meter to group rapid updates
    debounce_timers: dict[str, CALLBACK_TYPE] = {}
    # Last timestamp submitted per meter, to skip same-second resubmissions
    # the server would reject as duplicates
    last_submitted: dict[str, str] = {}

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
                        elif d_class == "current":
                            current_payload["current"] = val

                    except ValueError:
                        pass  # Ignore non-numeric states

            if not current_payload:
                return

            # Format: 2024-06-01T12:00:00Z
            iso_timestamp = (
                now.replace(microsecond=0).isoformat().replace("+00:00", "Z")
            )

            if last_submitted.get(meter_id) == iso_timestamp:
                return
            last_submitted[meter_id] = iso_timestamp

            # 4. Queue the reading. The queue is drained by the batch timer below,
            # so a reading never causes a request of its own. Enqueueing also means
            # the reading survives (on disk) if the connection or the server is down.
            cache.async_add(meter_id, iso_timestamp, current_payload)

        # Start the 0.1-second countdown timer
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

    # Queued readings are submitted as one batch per interval. The interval is drawn
    # anew every time so that installations do not drift into a shared tick and hit
    # the server in bursts.
    cancel_flush: CALLBACK_TYPE | None = None

    async def _flush(now: datetime) -> None:
        nonlocal cancel_flush
        cancel_flush = None
        await cache.async_flush()
        _schedule_flush(uniform(BATCH_INTERVAL_MIN, BATCH_INTERVAL_MAX))

    @callback
    def _schedule_flush(delay: float) -> None:
        nonlocal cancel_flush
        cancel_flush = async_call_later(hass, delay, _flush)

    # Readings restored from disk are submitted by the first flush rather than right
    # away, and that first delay covers the whole interval, so that many installations
    # restarting at once do not all submit at the same moment.
    _schedule_flush(uniform(0, BATCH_INTERVAL_MAX))

    @callback
    def _cancel_flush() -> None:
        if cancel_flush is not None:
            cancel_flush()

    entry.async_on_unload(_cancel_flush)

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


async def async_remove_entry(
    hass: HomeAssistant, entry: PowerCollectConfigEntry
) -> None:
    """Delete the cached readings when the config entry is removed."""
    await async_remove_cache(hass, entry.entry_id)
