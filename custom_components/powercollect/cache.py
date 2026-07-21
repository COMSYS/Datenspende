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
    PowerCollectRequestError,
)
from .const import DOMAIN, MAX_BATCH_READINGS, MAX_CACHED_READINGS, STORAGE_VERSION

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
        """Submit queued readings oldest-first in batches until empty or the server fails."""
        # A running drain already picks up newly enqueued readings, so don't
        # queue up on the lock; this keeps flush tasks from piling up when
        # submissions fail slowly during an outage.
        if self._lock.locked():
            return
        async with self._lock:
            donated_before = self._donated
            while self._pending:
                batch = self._pending[:MAX_BATCH_READINGS]
                unknown_meters: list[str] = []
                try:
                    accepted, unknown_meters = await self._api.submit_batch(batch)
                except PowerCollectRequestError as err:
                    # The server will reject this batch the same way every time, so
                    # keeping it would block everything queued behind it.
                    _LOGGER.error(
                        "Dropping %s reading(s) the server permanently rejects: %s",
                        len(batch),
                        err,
                    )
                    accepted = 0
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
                    self._donated += accepted

                if unknown_meters:
                    _LOGGER.warning(
                        "Server does not know meter(s) %s, dropping their readings",
                        ", ".join(unknown_meters),
                    )

                # Identity rather than slicing, because the cap in async_add may have
                # dropped head items while the submission was in flight.
                submitted = {id(item) for item in batch}
                unknown = set(unknown_meters)
                self._pending = [
                    reading
                    for reading in self._pending
                    if id(reading) not in submitted
                    and reading["meter_id"] not in unknown
                ]
            self._schedule_save()
            if self._donated != donated_before:
                for listener in list(self._listeners):
                    listener()

    @callback
    def _schedule_save(self) -> None:
        self._store.async_delay_save(
            lambda: CacheData(pending=self._pending, donated=self._donated), SAVE_DELAY
        )
