"""API client for PowerCollect service.

This module provides the PowerCollectAPI class for interacting with the PowerCollect API,
including device registration and power data submission.
"""

import logging
from typing import NoReturn

import aiohttp

_LOGGER = logging.getLogger(__name__)

POWERCOLLECT_BASE_URL = "https://datenspende.comsys.rwth-aachen.de"


class PowerCollectError(Exception):
    """Base exception for PowerCollect API errors."""


class PowerCollectConnError(PowerCollectError):
    """Exception raised when there is a connection error."""


class PowerCollectRequestError(PowerCollectError):
    """Exception raised for permanent request errors that a retry cannot fix."""


class PowerCollectAuthError(PowerCollectError):
    """Exception raised when authentication fails."""


class PowerCollectDuplicateError(PowerCollectError):
    """Exception raised when data for this meter and timestamp already exists."""


async def handle_api_error(response: aiohttp.ClientResponse) -> NoReturn:
    """Handle API errors based on the response status code."""
    status = response.status
    try:
        data = await response.json(content_type=None)
        if not isinstance(data, dict):
            data = {}
    except ValueError, aiohttp.ContentTypeError:
        data = {}

    code = data.get("error", "unknown_error")
    message = data.get("message", "Unknown error")

    if status == 400 and code == "bad_request":
        raise PowerCollectRequestError(f"Bad request: {message}")

    if status == 400 and code == "unexpected_resource_id":
        raise PowerCollectRequestError(f"Unexpected resource ID: {message}")

    if status == 400 and code == "missing_fields":
        raise PowerCollectRequestError(f"Missing required fields: {message}")

    if status == 404 and code == "not_found":
        raise PowerCollectRequestError(f"Resource not found: {message}")

    if status == 405 and code == "method_not_allowed":
        raise PowerCollectRequestError(f"Method not allowed: {message}")

    if code == "duplicate_entry":
        raise PowerCollectDuplicateError(f"Duplicate entry: {message}")

    if status == 500 and code == "internal_server_error":
        raise PowerCollectConnError(f"Internal server error: {message}")

    # AUTH errors
    if status == 400 and code == "password_too_short":
        raise PowerCollectAuthError(f"Password too short: {message}")

    if status == 400 and code == "password_too_long":
        raise PowerCollectAuthError(f"Password too long: {message}")

    if status == 400 and code == "username_too_short":
        raise PowerCollectAuthError(f"Username too short: {message}")

    if status == 400 and code == "username_too_long":
        raise PowerCollectAuthError(f"Username too long: {message}")

    if status == 400 and code == "invalid_username":
        raise PowerCollectAuthError(f"Invalid username: {message}")

    if status == 400 and code == "invalid_secret":
        raise PowerCollectAuthError(f"Invalid secret: {message}")

    if status == 401 and code == "authentication_error":
        raise PowerCollectAuthError(f"Authentication failed: {message}")

    if status == 401 and code == "invalid_credentials":
        raise PowerCollectAuthError(f"Invalid credentials: {message}")

    if status == 401 and code == "unauthorized":
        raise PowerCollectAuthError(f"Unauthorized access: {message}")

    if status == 409 and code == "taken_username":
        raise PowerCollectAuthError(f"Username already taken: {message}")

    if status == 409 and code == "taken_email":
        raise PowerCollectAuthError(f"Email already taken: {message}")

    if status == 409 and code == "validation_email":
        raise PowerCollectAuthError(f"Email validation error: {message}")

    raise PowerCollectConnError(
        f"Failed to connect: {response.status}, {code}, {message}"
    )


class PowerCollectAPI:
    """Client for interacting with the PowerCollect API."""

    def __init__(
        self,
        base_url: str,
        api_key: str | None,
        client_id: str | None,
        session: aiohttp.ClientSession,
        session_token: str | None = None,
    ) -> None:
        """Initialize the API client with the base URL and API key."""
        self.base_url = base_url.rstrip("/") + "/api/v1"
        self.api_key = api_key
        self.client_id = client_id
        self.session_token = session_token
        self.session = session

        self.web_header = (
            {"Authorization": f"Bearer {self.session_token}"}
            if self.session_token
            else {}
        )

        self.timeout = aiohttp.ClientTimeout(total=5)  # Set a timeout for API requests

    @property
    def _api_headers(self) -> dict[str, str]:
        """Return headers for API-key authenticated requests."""
        if self.api_key is None:
            raise PowerCollectAuthError("API key is not set")
        return {"x-api-key": self.api_key}

    async def get_client_id(self) -> str:
        """Get the client ID associated with the API key."""

        if self.api_key is None:
            raise PowerCollectAuthError("API key is not set")

        if self.client_id is not None:
            return self.client_id

        url = f"{self.base_url}/clients"
        try:
            async with self.session.get(
                url, headers=self._api_headers, timeout=self.timeout
            ) as response:
                if response.status == 201:
                    data = await response.json(content_type=None)
                    self.client_id = data["clientId"]
                    if self.client_id is None:
                        raise PowerCollectError("Client ID not found in response")
                    return self.client_id
                await handle_api_error(response)
        except (aiohttp.ClientError, TimeoutError) as e:
            raise PowerCollectConnError(f"Connection error: {e}") from e

    async def register_meter(self, name: str, vendor: str, model: str) -> str:
        """Register a new meter and return its ID."""
        url = f"{self.base_url}/clients/{self.client_id}/meters"
        payload = {"name": name, "vendor": vendor, "model": model}

        try:
            async with self.session.post(
                url, headers=self._api_headers, json=payload, timeout=self.timeout
            ) as response:
                if response.status == 201:
                    # content_type=None because the API does not yet return a
                    # JSON content type header on this endpoint.
                    data = await response.json(content_type=None)
                    _LOGGER.info("Data received from API: %s", data)
                    return data["meterId"]
                await handle_api_error(response)
        except (aiohttp.ClientError, TimeoutError) as e:
            raise PowerCollectConnError(f"Connection error: {e}") from e

    async def unregister_meter(self, meterId: str) -> None:
        """Unregister a meter."""
        url = f"{self.base_url}/clients/{self.client_id}/meters/{meterId}"

        try:
            async with self.session.delete(
                url, headers=self._api_headers, timeout=self.timeout
            ) as response:
                if response.status == 200:
                    return
                await handle_api_error(response)
        except (aiohttp.ClientError, TimeoutError) as e:
            raise PowerCollectConnError(f"Connection error: {e}") from e

    async def change_meter_details(
        self, meterId: str, name: str | None, vendor: str | None, model: str | None
    ) -> None:
        """Change the details of a registered meter."""
        url = f"{self.base_url}/clients/{self.client_id}/meters/{meterId}"

        payload = {}
        if name is not None:
            payload["name"] = name
        if vendor is not None:
            payload["vendor"] = vendor
        if model is not None:
            payload["model"] = model

        try:
            async with self.session.patch(
                url, headers=self._api_headers, json=payload, timeout=self.timeout
            ) as response:
                if response.status == 200:
                    return
                await handle_api_error(response)
        except (aiohttp.ClientError, TimeoutError) as e:
            raise PowerCollectConnError(f"Connection error: {e}") from e

    async def get_registered_meters(self) -> list:
        """Get a list of all registered meters."""
        url = f"{self.base_url}/clients/{self.client_id}/meters"

        try:
            async with self.session.get(
                url, headers=self._api_headers, timeout=self.timeout
            ) as response:
                if response.status == 200:
                    return await response.json(content_type=None)
                if response.status == 404:
                    return []
                await handle_api_error(response)
        except (aiohttp.ClientError, TimeoutError) as e:
            raise PowerCollectConnError(f"Connection error: {e}") from e

    async def submit_data(
        self,
        meterId: str,
        timestamp: str,
        power: float | None = None,
        energy: float | None = None,
        voltage: float | None = None,
        current: float | None = None,
    ) -> None:
        """Submit power data for a device."""
        if power is None and energy is None and voltage is None and current is None:
            raise ValueError(
                "At least one of power, energy, voltage, or current must be provided"
            )

        url = f"{self.base_url}/clients/{self.client_id}/meters/{meterId}/data"
        payload: dict[str, str | float] = {"timestamp": timestamp}
        if power is not None:
            payload["power"] = power
        if energy is not None:
            payload["energy"] = energy
        if voltage is not None:
            payload["voltage"] = voltage
        if current is not None:
            payload["current"] = current

        try:
            async with self.session.post(
                url, json=payload, headers=self._api_headers, timeout=self.timeout
            ) as response:
                if response.status == 201:
                    return
                await handle_api_error(response)
        except (aiohttp.ClientError, TimeoutError) as e:
            raise PowerCollectConnError(f"Connection error: {e}") from e

    # WEB endpoints
    async def sign_up(
        self,
        username: str,
        email: str | None,
        password: str,
        secret: str | None,
        consent: bool,
    ) -> str:
        """Sign up a new user."""
        if username is None or password is None:
            raise ValueError("Both username and password must be provided")

        url = f"{self.base_url}/web/auth/sign-up/username"
        payload: dict[str, str | bool] = {"username": username, "password": password}

        if email is not None and email != "":
            payload["email"] = email

        if secret is not None and secret != "":
            payload["secret"] = secret

        if not consent:
            raise PowerCollectAuthError("User must provide consent to sign up")
        payload["consent"] = consent

        try:
            async with self.session.post(
                url, json=payload, timeout=self.timeout
            ) as response:
                if response.status == 201:
                    response_data = await response.json(content_type=None)
                    self.session_token = response_data["token"]
                    return response_data["user"]["id"]
                await handle_api_error(response)
        except (aiohttp.ClientError, TimeoutError) as e:
            raise PowerCollectConnError(f"Connection error: {e}") from e

    async def sign_in(self, username: str, password: str) -> str:
        """Sign in an existing user."""
        if username is None or password is None:
            raise ValueError("Both username and password must be provided")

        url = f"{self.base_url}/web/auth/sign-in/username"
        payload = {"username": username, "password": password}

        try:
            async with self.session.post(
                url, json=payload, headers=self.web_header, timeout=self.timeout
            ) as response:
                if response.status == 200:
                    response_data = await response.json(content_type=None)
                    self.session_token = response_data["token"]
                    return response_data["user"]["id"]
                await handle_api_error(response)
        except (aiohttp.ClientError, TimeoutError) as e:
            raise PowerCollectConnError(f"Connection error: {e}") from e

    async def sign_out(self) -> None:
        """Sign out the current user."""
        if self.session_token is None:
            raise ValueError("No user is currently signed in")

        url = f"{self.base_url}/web/auth/sign-out"
        headers = {**self.web_header, "Authorization": f"Bearer {self.session_token}"}

        try:
            async with self.session.post(
                url, headers=headers, timeout=self.timeout
            ) as response:
                if response.status == 200:
                    return
                await handle_api_error(response)
        except (aiohttp.ClientError, TimeoutError) as e:
            raise PowerCollectConnError(f"Connection error: {e}") from e

    async def create_api_key(self, name: str | None) -> str:
        """Create a new API key for the current user."""
        if self.session_token is None:
            raise PowerCollectAuthError("No user is currently signed in")

        url = f"{self.base_url}/web/auth/api-key/create"
        headers = {**self.web_header, "Authorization": f"Bearer {self.session_token}"}
        payload = {"clientId": self.client_id}

        if name is not None:
            payload["name"] = name

        try:
            async with self.session.post(
                url, json=payload, headers=headers, timeout=self.timeout
            ) as response:
                if response.status == 201:
                    data = await response.json(content_type=None)
                    return data["key"]
                await handle_api_error(response)
        except (aiohttp.ClientError, TimeoutError) as e:
            raise PowerCollectConnError(f"Connection error: {e}") from e

    async def create_household(
        self,
        userId: str,
        name: str | None,
        numberInhabitants: int | None,
        zip: str | None,
        country: str | None,
    ) -> str:
        """Create a new household for the current user."""
        if self.session_token is None:
            raise PowerCollectAuthError("No user is currently signed in")

        url = f"{self.base_url}/web/households"
        headers = {**self.web_header, "Authorization": f"Bearer {self.session_token}"}
        payload: dict[str, str | int] = {
            "userId": userId,
        }
        if name is not None:
            payload["name"] = name
        if numberInhabitants is not None:
            payload["numberInhabitants"] = numberInhabitants
        if zip is not None:
            payload["zip"] = zip
        if country is not None:
            payload["country"] = country

        try:
            async with self.session.post(
                url, json=payload, headers=headers, timeout=self.timeout
            ) as response:
                if response.status == 201:
                    data = await response.json(content_type=None)
                    return data["id"]
                await handle_api_error(response)
        except (aiohttp.ClientError, TimeoutError) as e:
            raise PowerCollectConnError(f"Connection error: {e}") from e

    async def get_households(self) -> list:
        """Get a list of households for the current user."""
        if self.session_token is None:
            raise PowerCollectAuthError("No user is currently signed in")

        url = f"{self.base_url}/web/households"
        headers = {**self.web_header, "Authorization": f"Bearer {self.session_token}"}

        try:
            async with self.session.get(
                url, headers=headers, timeout=self.timeout
            ) as response:
                if response.status == 200:
                    return await response.json(content_type=None)
                await handle_api_error(response)
        except (aiohttp.ClientError, TimeoutError) as e:
            raise PowerCollectConnError(f"Connection error: {e}") from e

    async def create_client(self, householdId: str, name: str | None, type: str) -> str:
        """Create a new client for the current household."""
        if self.session_token is None:
            raise PowerCollectAuthError("No user is currently signed in")

        url = f"{self.base_url}/web/clients"
        headers = {**self.web_header, "Authorization": f"Bearer {self.session_token}"}
        payload = {"householdId": householdId, "type": type}

        if name is not None:
            payload["name"] = name

        try:
            async with self.session.post(
                url, json=payload, headers=headers, timeout=self.timeout
            ) as response:
                if response.status == 201:
                    data = await response.json(content_type=None)
                    return data["id"]
                await handle_api_error(response)
        except (aiohttp.ClientError, TimeoutError) as e:
            raise PowerCollectConnError(f"Connection error: {e}") from e
