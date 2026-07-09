"""Persistent offline cache for PowerCollect data submissions."""

import asyncio
import logging
from typing import Any, TypedDict

from homeassistant.core import CALLBACK_TYPE, HomeAssistant, callback
from homeassistant.helpers.storage import Store

from .api import (
    PowerCollectAPI,
    PowerCollectAuthError,
    PowerCollectConnError,
    PowerCollectDuplicateError,
    PowerCollectRequestError,
)
from .const import DOMAIN, MAX_CACHED_READINGS, STORAGE_VERSION

_LOGGER = logging.getLogger(__name__)

SAVE_DELAY = 10


class CacheData(TypedDict):
    """Payload of the cache's storage file."""

    pending: list[dict[str, Any]]
    donated: int


def _storage_key(entry_id: str) -> str:
    return f"{DOMAIN}.{entry_id}"


async def async_remove_cache(hass: HomeAssistant, entry_id: str) -> None:
    """Delete a config entry's cache file from disk."""
    store: Store[CacheData] = Store(hass, STORAGE_VERSION, _storage_key(entry_id))
    await store.async_remove()


class SubmissionCache:
    """Disk-backed FIFO queue of meter readings awaiting submission.

    Every reading is enqueued instead of being sent directly; a flush then
    drains the queue oldest-first. Readings collected while the server is
    unreachable are therefore delivered in order, with their original
    timestamps, once it is back. The queue survives Home Assistant restarts.
    """

    def __init__(
        self, hass: HomeAssistant, entry_id: str, api: PowerCollectAPI
    ) -> None:
        """Initialize the cache for one config entry."""
        self._api = api
        self._store: Store[CacheData] = Store(
            hass, STORAGE_VERSION, _storage_key(entry_id)
        )
        self._pending: list[dict[str, Any]] = []
        self._donated = 0
        self._listeners: set[CALLBACK_TYPE] = set()
        self._lock = asyncio.Lock()

    async def async_load(self) -> None:
        """Restore pending readings and the donation counter from disk."""
        if data := await self._store.async_load():
            self._pending = data["pending"]
            self._donated = data["donated"]

    @property
    def has_pending(self) -> bool:
        """Return True if readings are waiting to be submitted."""
        return bool(self._pending)

    @property
    def donated(self) -> int:
        """Return the number of readings the server has accepted."""
        return self._donated

    @callback
    def async_add_listener(self, listener: CALLBACK_TYPE) -> CALLBACK_TYPE:
        """Notify listener when the donation count changes; returns unsubscriber."""
        self._listeners.add(listener)

        @callback
        def _unsubscribe() -> None:
            self._listeners.discard(listener)

        return _unsubscribe

    @callback
    def async_add(
        self, meter_id: str, timestamp: str, fields: dict[str, float]
    ) -> None:
        """Queue a reading and schedule it to be written to disk."""
        self._pending.append({"meter_id": meter_id, "timestamp": timestamp, **fields})
        if len(self._pending) > MAX_CACHED_READINGS:
            del self._pending[: len(self._pending) - MAX_CACHED_READINGS]
            _LOGGER.warning(
                "Submission cache is full (%s readings), dropping oldest reading",
                MAX_CACHED_READINGS,
            )
        self._schedule_save()

    async def async_flush(self) -> None:
        """Submit queued readings oldest-first until empty or the server fails."""
        # A running drain already picks up newly enqueued readings, so don't
        # queue up on the lock; this keeps flush tasks from piling up when
        # submissions fail slowly during an outage.
        if self._lock.locked():
            return
        async with self._lock:
            donated_before = self._donated
            while self._pending:
                item = self._pending[0]
                try:
                    await self._api.submit_data(
                        meterId=item["meter_id"],
                        timestamp=item["timestamp"],
                        power=item.get("power"),
                        energy=item.get("energy"),
                        voltage=item.get("voltage"),
                        current=item.get("current"),
                    )
                except PowerCollectDuplicateError:
                    _LOGGER.debug("Server already has reading, dropping it: %s", item)
                except PowerCollectRequestError as err:
                    _LOGGER.error(
                        "Dropping reading the server permanently rejects (%s): %s",
                        err,
                        item,
                    )
                except (PowerCollectConnError, PowerCollectAuthError) as err:
                    # Server unreachable or rejecting the client as a whole:
                    # keep everything queued and retry on the next flush.
                    _LOGGER.debug(
                        "Submission failed, %s reading(s) remain cached: %s",
                        len(self._pending),
                        err,
                    )
                    break
                else:
                    self._donated += 1
                # The cap in async_add may drop head items while a submission
                # is in flight; only pop if this item is still the head.
                if self._pending and self._pending[0] is item:
                    del self._pending[0]
            self._schedule_save()
            if self._donated != donated_before:
                for listener in list(self._listeners):
                    listener()

    @callback
    def _schedule_save(self) -> None:
        self._store.async_delay_save(
            lambda: CacheData(pending=self._pending, donated=self._donated), SAVE_DELAY
        )
