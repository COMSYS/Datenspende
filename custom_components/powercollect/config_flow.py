"""Config flow for the PowerCollect integration."""

import logging
from typing import Any

import voluptuous as vol

from homeassistant.components.sensor import SensorDeviceClass
from homeassistant.config_entries import (
    ConfigEntry,
    ConfigEntryBaseFlow,
    ConfigFlow,
    ConfigFlowResult,
    OptionsFlow,
)
from homeassistant.const import EntityCategory
from homeassistant.core import callback
from homeassistant.data_entry_flow import section
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import device_registry as dr, entity_registry as er, selector
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.util.location import async_detect_location_info

from .api import (
    POWERCOLLECT_BASE_URL,
    PowerCollectAPI,
    PowerCollectAuthError,
    PowerCollectConnError,
    PowerCollectError,
)
from .const import DOMAIN, NAME

_LOGGER = logging.getLogger(__name__)


async def validate_input(api: PowerCollectAPI, data: dict[str, Any]) -> dict[str, Any]:
    """Validate the user input allows us to connect."""
    try:
        await api.get_client_id()
    except PowerCollectAuthError as e:
        raise InvalidAuth(e) from e
    except PowerCollectConnError as e:
        raise CannotConnect(e) from e

    return {"title": f"{NAME}: {data['api_key']}"}


class MeterListEntry:
    """Helper class to represent a meter entry in the config flow."""

    def __init__(
        self,
        meter_id: str,
        name: str,
        vendor: str,
        model: str,
        entity_ids: list[str],
    ) -> None:
        """Initialize a meter list entry."""
        self.meter_id = meter_id
        self.name = name
        self.vendor = vendor
        self.model = model
        self.entity_ids = entity_ids

    def as_dict(self) -> dict[str, Any]:
        """Convert the custom object to a standard dictionary for Home Assistant storage."""
        return {
            "meter_id": self.meter_id,
            "name": self.name,
            "vendor": self.vendor,
            "model": self.model,
            "entity_ids": self.entity_ids,
        }


class PowerCollectBaseFlow(ConfigEntryBaseFlow):
    """Shared Hub and Spoke logic for both Config and Options flows."""

    api_key: str | None
    client_id: str | None

    meters: list[MeterListEntry]
    observed_devices: list[str]
    manual_meters: dict[str, list[str]]
    custom_meter_map: dict[str, str]

    current_editing_meter: str | None
    is_options_flow: bool

    async def async_step_main_menu(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Step 2 (Hub Menu): Dynamic visual choice loop."""
        menu_options = []

        if getattr(self, "is_options_flow", False):
            menu_options.append("change_api_key")

        menu_options.extend(["device_selection", "custom_meters_overview"])

        if self.observed_devices or self.manual_meters:
            menu_options.append("finish_flow")

        return self.async_show_menu(
            step_id="main_menu",
            menu_options=menu_options,
            description_placeholders={"name": NAME},
        )

    async def async_step_device_selection(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Spoke A: Hardware selection."""
        errors: dict[str, str] = {}

        if user_input is not None:
            self.observed_devices = user_input.get("observed_devices", [])
            return await self.async_step_main_menu()

        ent_reg = er.async_get(self.hass)
        dev_reg = dr.async_get(self.hass)

        core_energy_devices = set()
        voltamperes_devices = set()
        battery_devices = set()

        for entity in ent_reg.entities.values():
            if entity.domain != "sensor" or not entity.device_id:
                continue

            d_class = str(entity.device_class or entity.original_device_class).lower()

            if d_class in {"power", "energy"}:
                core_energy_devices.add(entity.device_id)
            elif d_class == "battery":
                battery_devices.add(entity.device_id)
            elif d_class in {"voltage", "current"}:
                if entity.entity_category != EntityCategory.DIAGNOSTIC:
                    voltamperes_devices.add(entity.device_id)

        valid_device_ids = set()
        valid_device_ids.update(core_energy_devices)

        for dev_id in voltamperes_devices:
            if dev_id not in battery_devices:
                valid_device_ids.add(dev_id)

        device_options: list[selector.SelectOptionDict] = []
        for dev_id in valid_device_ids:
            device = dev_reg.async_get(dev_id)
            if device:
                name = device.name_by_user or device.name or "Unknown Device"
                device_options.append({"value": dev_id, "label": name})

        # Auto-select all valid devices by default only on first visit
        if not self.observed_devices and not self.is_options_flow:
            self.observed_devices = list(valid_device_ids)

        schema = vol.Schema(
            {
                vol.Optional(
                    "observed_devices", default=self.observed_devices
                ): selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=device_options,
                        multiple=True,
                        mode=selector.SelectSelectorMode.DROPDOWN,
                    )
                ),
            }
        )

        return self.async_show_form(
            step_id="device_selection",
            data_schema=schema,
            errors=errors,
            description_placeholders={"name": NAME},
        )

    async def async_step_custom_meters_overview(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Spoke B1: The Virtual Meters Overview using native Menu."""
        menu_options = ["create_manual_meter"]

        if self.manual_meters:
            menu_options.extend(
                ["select_edit_custom_meter", "select_delete_custom_meter"]
            )

        menu_options.append("main_menu")

        return self.async_show_menu(
            step_id="custom_meters_overview",
            menu_options=menu_options,
        )

    async def async_step_create_manual_meter(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Spoke B2a: Form to create a new virtual meter."""
        errors: dict[str, str] = {}

        if user_input is not None:
            meter_name = user_input["meter_name"]
            power_entity = user_input.get("power_entity")
            energy_entity = user_input.get("energy_entity")
            voltage_entity = user_input.get("voltage_entity")
            current_entity = user_input.get("current_entity")

            selected_entities = []
            if power_entity:
                selected_entities.append(power_entity)
            if energy_entity:
                selected_entities.append(energy_entity)
            if voltage_entity:
                selected_entities.append(voltage_entity)
            if current_entity:
                selected_entities.append(current_entity)

            if meter_name in self.manual_meters:
                errors["meter_name"] = "name_already_exists"
            elif (
                not power_entity
                and not energy_entity
                and not voltage_entity
                and not current_entity
            ):
                errors["base"] = "at_least_one_entity_required"
            else:
                self.manual_meters[meter_name] = selected_entities
                return await self.async_step_custom_meters_overview()

        # Find all voltage/current sensors belonging to battery-powered devices
        # to exclude them
        ent_reg = er.async_get(self.hass)

        # Find devices that have a battery sensor
        battery_device_ids = {
            entity.device_id
            for entity in ent_reg.entities.values()
            if entity.device_id
            and entity.domain == "sensor"
            and str(entity.device_class or entity.original_device_class).lower()
            == "battery"
        }

        # Any voltage/current sensor belonging to a battery-powered device is excluded
        exclude_battery_entities = [
            entity.entity_id
            for entity in ent_reg.entities.values()
            if entity.device_id
            and entity.device_id in battery_device_ids
            and entity.domain == "sensor"
            and str(entity.device_class or entity.original_device_class).lower()
            in {"voltage", "current"}
        ]

        # Preserve the values the user submitted so a validation error does not
        # clear the form.
        suggested = user_input or {}

        schema = vol.Schema(
            {
                vol.Required(
                    "meter_name", default=suggested.get("meter_name", "")
                ): selector.TextSelector(),
                vol.Optional(
                    "power_entity",
                    description={"suggested_value": suggested.get("power_entity")},
                ): selector.EntitySelector(
                    selector.EntitySelectorConfig(
                        filter=selector.EntityFilterSelectorConfig(
                            domain="sensor",
                            device_class=[SensorDeviceClass.POWER],
                        ),
                        multiple=False,
                    )
                ),
                vol.Optional(
                    "energy_entity",
                    description={"suggested_value": suggested.get("energy_entity")},
                ): selector.EntitySelector(
                    selector.EntitySelectorConfig(
                        filter=selector.EntityFilterSelectorConfig(
                            domain="sensor",
                            device_class=[SensorDeviceClass.ENERGY],
                        ),
                        multiple=False,
                    )
                ),
                vol.Optional(
                    "voltage_entity",
                    description={"suggested_value": suggested.get("voltage_entity")},
                ): selector.EntitySelector(
                    selector.EntitySelectorConfig(
                        filter=selector.EntityFilterSelectorConfig(
                            domain="sensor",
                            device_class=[SensorDeviceClass.VOLTAGE],
                        ),
                        exclude_entities=exclude_battery_entities,
                        multiple=False,
                    )
                ),
                vol.Optional(
                    "current_entity",
                    description={"suggested_value": suggested.get("current_entity")},
                ): selector.EntitySelector(
                    selector.EntitySelectorConfig(
                        filter=selector.EntityFilterSelectorConfig(
                            domain="sensor",
                            device_class=[SensorDeviceClass.CURRENT],
                        ),
                        exclude_entities=exclude_battery_entities,
                        multiple=False,
                    )
                ),
            }
        )

        return self.async_show_form(
            step_id="create_manual_meter", data_schema=schema, errors=errors
        )

    async def async_step_select_edit_custom_meter(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Spoke B2b: Dropdown to select which existing meter to edit."""
        if user_input is not None:
            self.current_editing_meter = user_input["selected_meter"]
            return await self.async_step_edit_custom_meter()

        options: list[selector.SelectOptionDict] = [
            {"value": k, "label": k} for k in self.manual_meters
        ]

        schema = vol.Schema(
            {
                vol.Required("selected_meter"): selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=options,
                        mode=selector.SelectSelectorMode.DROPDOWN,
                    )
                )
            }
        )

        return self.async_show_form(
            step_id="select_edit_custom_meter", data_schema=schema
        )

    async def async_step_edit_custom_meter(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Spoke B2c: Form to modify an existing virtual meter."""
        errors: dict[str, str] = {}

        if user_input is not None:
            meter_name = user_input["meter_name"]
            power_entity = user_input.get("power_entity")
            energy_entity = user_input.get("energy_entity")
            voltage_entity = user_input.get("voltage_entity")
            current_entity = user_input.get("current_entity")

            selected_entities = []
            if power_entity:
                selected_entities.append(power_entity)
            if energy_entity:
                selected_entities.append(energy_entity)
            if voltage_entity:
                selected_entities.append(voltage_entity)
            if current_entity:
                selected_entities.append(current_entity)

            if (
                not power_entity
                and not energy_entity
                and not voltage_entity
                and not current_entity
            ):
                errors["base"] = "at_least_one_entity_required"
            elif (
                meter_name != self.current_editing_meter
                and meter_name in self.manual_meters
            ):
                errors["meter_name"] = "name_already_exists"
            else:
                if (
                    self.current_editing_meter is not None
                    and meter_name != self.current_editing_meter
                ):
                    del self.manual_meters[self.current_editing_meter]
                    # Securely track renames so we don't lose the meter ID!
                    if self.current_editing_meter in self.custom_meter_map:
                        meter_id = self.custom_meter_map.pop(self.current_editing_meter)
                        self.custom_meter_map[meter_name] = meter_id

                self.manual_meters[meter_name] = selected_entities
                return await self.async_step_custom_meters_overview()

        default_name = self.current_editing_meter or ""
        default_entities = (
            self.manual_meters.get(self.current_editing_meter, [])
            if self.current_editing_meter
            else []
        )

        # Match entities to the correct selector type based on their device class
        ent_reg = er.async_get(self.hass)
        default_power_entity = None
        default_energy_entity = None
        default_voltage_entity = None
        default_current_entity = None

        for entity_id in default_entities:
            entity = ent_reg.entities.get(entity_id)
            if entity:
                device_class = str(
                    entity.device_class or entity.original_device_class or ""
                ).lower()
                if device_class == "power":
                    default_power_entity = entity_id
                elif device_class == "energy":
                    default_energy_entity = entity_id
                elif device_class == "voltage":
                    default_voltage_entity = entity_id
                elif device_class == "current":
                    default_current_entity = entity_id

        # Find all voltage/current sensors belonging to battery-powered devices
        # to exclude them
        battery_device_ids = {
            entity.device_id
            for entity in ent_reg.entities.values()
            if entity.device_id
            and entity.domain == "sensor"
            and str(entity.device_class or entity.original_device_class).lower()
            == "battery"
        }
        exclude_battery_entities = [
            entity.entity_id
            for entity in ent_reg.entities.values()
            if entity.device_id
            and entity.device_id in battery_device_ids
            and entity.domain == "sensor"
            and str(entity.device_class or entity.original_device_class).lower()
            in {"voltage", "current"}
        ]

        schema = vol.Schema(
            {
                vol.Required(
                    "meter_name", default=default_name
                ): selector.TextSelector(),
                vol.Optional(
                    "power_entity",
                    description={"suggested_value": default_power_entity},
                ): selector.EntitySelector(
                    selector.EntitySelectorConfig(
                        filter=selector.EntityFilterSelectorConfig(
                            domain="sensor",
                            device_class=[SensorDeviceClass.POWER],
                        ),
                        multiple=False,
                    )
                ),
                vol.Optional(
                    "energy_entity",
                    description={"suggested_value": default_energy_entity},
                ): selector.EntitySelector(
                    selector.EntitySelectorConfig(
                        filter=selector.EntityFilterSelectorConfig(
                            domain="sensor",
                            device_class=[SensorDeviceClass.ENERGY],
                        ),
                        multiple=False,
                    )
                ),
                vol.Optional(
                    "voltage_entity",
                    description={"suggested_value": default_voltage_entity},
                ): selector.EntitySelector(
                    selector.EntitySelectorConfig(
                        filter=selector.EntityFilterSelectorConfig(
                            domain="sensor",
                            device_class=[SensorDeviceClass.VOLTAGE],
                        ),
                        exclude_entities=exclude_battery_entities,
                        multiple=False,
                    )
                ),
                vol.Optional(
                    "current_entity",
                    description={"suggested_value": default_current_entity},
                ): selector.EntitySelector(
                    selector.EntitySelectorConfig(
                        filter=selector.EntityFilterSelectorConfig(
                            domain="sensor",
                            device_class=[SensorDeviceClass.CURRENT],
                        ),
                        exclude_entities=exclude_battery_entities,
                        multiple=False,
                    )
                ),
            }
        )

        return self.async_show_form(
            step_id="edit_custom_meter", data_schema=schema, errors=errors
        )

    async def async_step_select_delete_custom_meter(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Spoke B2d: Dropdown to select which existing meter to delete."""
        if user_input is not None:
            meter_to_delete = user_input["selected_meter"]
            if meter_to_delete in self.manual_meters:
                del self.manual_meters[meter_to_delete]
            # Securely track deletions
            if meter_to_delete in self.custom_meter_map:
                del self.custom_meter_map[meter_to_delete]
            return await self.async_step_custom_meters_overview()

        options: list[selector.SelectOptionDict] = [
            {"value": k, "label": k} for k in self.manual_meters
        ]

        schema = vol.Schema(
            {
                vol.Required("selected_meter"): selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=options,
                        mode=selector.SelectSelectorMode.DROPDOWN,
                    )
                )
            }
        )

        return self.async_show_form(
            step_id="select_delete_custom_meter", data_schema=schema
        )


class PowerCollectConfigFlow(PowerCollectBaseFlow, ConfigFlow, domain=DOMAIN):
    """Handle the initial config flow for Power Collect."""

    VERSION = 1

    def __init__(self) -> None:
        """Initialize the Power Collect config flow."""
        self.api_key: str | None = None
        self.client_id: str | None = None

        self.observed_devices: list[str] = []
        self.manual_meters: dict[str, list[str]] = {}
        self.custom_meter_map: dict[str, str] = {}
        self.meters: list[MeterListEntry] = []

        self.current_editing_meter: str | None = None
        self.is_options_flow = False

        self._temp_username: str | None = None
        self._temp_email: str | None = None
        self._temp_password: str | None = None
        self._temp_secret: str | None = None
        self._temp_user_id: str | None = None
        self._temp_session_token: str | None = None

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: ConfigEntry) -> OptionsFlow:
        """Create the options flow."""
        return PowerCollectOptionsFlow(config_entry)

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle Step 1: Entry menu for Sign Up, Sign In, or API Key."""
        self.context["title_placeholders"] = {"name": NAME}
        return self.async_show_menu(
            step_id="user",
            menu_options=["signup", "signin", "api_key"],
            description_placeholders={"name": NAME},
        )

    async def async_step_signup(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Step to sign up a new user."""
        errors: dict[str, str] = {}
        if user_input is not None:
            advanced_options = user_input.pop("advanced_options", {})
            user_input.update(advanced_options)

            self._temp_username = user_input["username"]
            self._temp_email = user_input.get("email")
            self._temp_password = user_input["password"]
            self._temp_secret = user_input.get("secret")

            # The user must explicitly agree to participate and donate data.
            if not user_input.get("consent"):
                errors["base"] = "consent_required"

            api = PowerCollectAPI(
                base_url=POWERCOLLECT_BASE_URL,
                api_key=None,
                client_id=None,
                session=async_get_clientsession(self.hass),
            )
            try:
                if not errors:
                    self._temp_user_id = await api.sign_up(
                        username=self._temp_username,
                        email=self._temp_email,
                        password=self._temp_password,
                        secret=self._temp_secret,
                    )
                    self._temp_session_token = api.session_token
                    return await self.async_step_household()
            except PowerCollectAuthError as e:
                _LOGGER.error("Auth error during sign up: %s", e)
                err_msg = str(e)
                if "Password too short" in err_msg:
                    errors["base"] = "password_too_short"
                elif "Password too long" in err_msg:
                    errors["base"] = "password_too_long"
                elif "Username too short" in err_msg:
                    errors["base"] = "username_too_short"
                elif "Username too long" in err_msg:
                    errors["base"] = "username_too_long"
                elif "Invalid username" in err_msg:
                    errors["base"] = "invalid_username"
                elif "Username already taken" in err_msg:
                    errors["base"] = "username_taken"
                elif "Invalid secret" in err_msg:
                    errors["base"] = "invalid_secret"
                elif "Invalid email" in err_msg:
                    errors["base"] = "invalid_email"
                elif "Email already taken" in err_msg:
                    errors["base"] = "email_taken"
                else:
                    errors["base"] = "invalid_auth"
            except PowerCollectConnError as e:
                _LOGGER.error("Connection error during sign up: %s", e)
                errors["base"] = "cannot_connect"
            except PowerCollectError as e:
                _LOGGER.error("PowerCollect error during sign up: %s", e)
                errors["base"] = "unknown"

        schema = vol.Schema(
            {
                vol.Required(
                    "username", default=self._temp_username or ""
                ): selector.TextSelector(
                    selector.TextSelectorConfig(type=selector.TextSelectorType.TEXT)
                ),
                vol.Required(
                    "password", default=self._temp_password or ""
                ): selector.TextSelector(
                    selector.TextSelectorConfig(type=selector.TextSelectorType.PASSWORD)
                ),
                vol.Optional(
                    "email", default=self._temp_email or ""
                ): selector.TextSelector(
                    selector.TextSelectorConfig(type=selector.TextSelectorType.EMAIL)
                ),
                vol.Required("consent", default=False): selector.BooleanSelector(),
                vol.Optional("advanced_options"): section(
                    vol.Schema(
                        {
                            vol.Optional(
                                "secret", default=self._temp_secret or ""
                            ): selector.TextSelector(
                                selector.TextSelectorConfig(
                                    type=selector.TextSelectorType.TEXT
                                )
                            ),
                        }
                    )
                ),
            }
        )
        return self.async_show_form(
            step_id="signup",
            data_schema=schema,
            errors=errors,
            description_placeholders={"name": NAME},
        )

    async def async_step_signin(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Step to sign in an existing user."""
        errors: dict[str, str] = {}
        if user_input is not None:
            self._temp_username = user_input["username"]
            self._temp_password = user_input["password"]

            api = PowerCollectAPI(
                base_url=POWERCOLLECT_BASE_URL,
                api_key=None,
                client_id=None,
                session=async_get_clientsession(self.hass),
            )
            try:
                self._temp_user_id = await api.sign_in(
                    username=self._temp_username,
                    password=self._temp_password,
                )
                self._temp_session_token = api.session_token
                return await self.async_step_household()
            except PowerCollectAuthError as e:
                _LOGGER.error("Auth error during sign in: %s", e)
                errors["base"] = "invalid_auth"
            except PowerCollectConnError as e:
                _LOGGER.error("Connection error during sign in: %s", e)
                errors["base"] = "cannot_connect"
            except PowerCollectError as e:
                _LOGGER.error("PowerCollect error during sign in: %s", e)
                errors["base"] = "unknown"

        schema = vol.Schema(
            {
                vol.Required(
                    "username", default=self._temp_username or ""
                ): selector.TextSelector(),
                vol.Required(
                    "password", default=self._temp_password or ""
                ): selector.TextSelector(
                    selector.TextSelectorConfig(type=selector.TextSelectorType.PASSWORD)
                ),
            }
        )
        return self.async_show_form(
            step_id="signin",
            data_schema=schema,
            errors=errors,
            description_placeholders={"name": NAME},
        )

    async def async_step_api_key(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle option to provide an existing API key directly."""
        schema = vol.Schema({vol.Required("api_key"): selector.TextSelector()})
        errors: dict[str, str] = {}

        if user_input is not None:
            api = PowerCollectAPI(
                base_url=POWERCOLLECT_BASE_URL,
                api_key=user_input["api_key"],
                session=async_get_clientsession(self.hass),
                client_id=None,
            )

            try:
                await validate_input(api, user_input)
            except PowerCollectError as e:
                if isinstance(e, PowerCollectAuthError):
                    errors["base"] = "invalid_auth"
                elif isinstance(e, PowerCollectConnError):
                    errors["base"] = "cannot_connect"
                else:
                    errors["base"] = "unknown_error"

            if errors:
                return self.async_show_form(
                    step_id="api_key",
                    data_schema=schema,
                    errors=errors,
                    description_placeholders={"name": NAME},
                )

            unique_id = api.client_id
            await self.async_set_unique_id(unique_id)
            self._abort_if_unique_id_configured()

            self.api_key = user_input["api_key"]
            self.client_id = api.client_id

            return await self.async_step_main_menu()

        return self.async_show_form(
            step_id="api_key",
            data_schema=schema,
            description_placeholders={"name": NAME},
        )

    async def async_step_household(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Step to collect household information and set up the client/API key."""
        errors: dict[str, str] = {}

        if user_input is not None:
            api = PowerCollectAPI(
                base_url=POWERCOLLECT_BASE_URL,
                api_key=None,
                client_id=None,
                session=async_get_clientsession(self.hass),
                session_token=self._temp_session_token,
            )

            try:
                # 1. Create a household
                assert self._temp_user_id is not None
                household_id = await api.create_household(
                    userId=self._temp_user_id,
                    name=user_input["household_title"],
                    numberInhabitants=user_input["number_inhabitants"],
                    zip=user_input["zip"],
                    country=user_input["country"],
                )

                # 2. Automatically create a client in this household
                client_id = await api.create_client(
                    householdId=household_id,
                    name="Home Assistant",
                    type="home_assistant",
                )
                api.client_id = client_id

                # 3. Create an API key
                api_key = await api.create_api_key(name="Home Assistant API Key")

                # 4. Sign out
                await api.sign_out()

                # Save variables and verify unique ID
                unique_id = client_id
                await self.async_set_unique_id(unique_id)
                self._abort_if_unique_id_configured()

                self.api_key = api_key
                self.client_id = client_id

                return await self.async_step_main_menu()

            except PowerCollectAuthError as e:
                _LOGGER.error("Auth error during household/client setup: %s", e)
                errors["base"] = "invalid_auth"
            except PowerCollectConnError as e:
                _LOGGER.error("Connection error during household/client setup: %s", e)
                errors["base"] = "cannot_connect"
            except PowerCollectError as e:
                _LOGGER.error("PowerCollect error during household/client setup: %s", e)
                errors["base"] = "unknown"

        # Get household name from Home Assistant location name
        default_household_title = (
            self.hass.config.location_name
            if hasattr(self.hass.config, "location_name")
            else "Home"
        )

        # Get number of persons as approximation for inhabitants
        default_inhabitants = 1
        try:
            ent_reg = er.async_get(self.hass)
            person_entities = [
                entity
                for entity in ent_reg.entities.values()
                if entity.domain == "person"
            ]
            default_inhabitants = max(1, len(person_entities))  # Ensure at least 1
        except ValueError, TypeError, AttributeError:
            # If we can't get persons, use default of 1
            pass

        # Get default country value from Home Assistant configuration
        default_country = (
            self.hass.config.country if hasattr(self.hass.config, "country") else ""
        )

        # Try to get location info including zip code if HA has latitude/longitude configured
        default_zip = ""
        if (
            hasattr(self.hass.config, "latitude")
            and hasattr(self.hass.config, "longitude")
            and self.hass.config.latitude is not None
            and self.hass.config.longitude is not None
        ):
            try:
                session = async_get_clientsession(self.hass)
                location_info = await async_detect_location_info(session)
                if location_info:
                    default_country = (
                        location_info.country_code
                        if default_country == ""
                        else default_country
                    )
                    default_zip = location_info.zip_code
            except ValueError, TypeError, AttributeError:
                # If location detection fails, we'll use the defaults from config
                pass

        schema = vol.Schema(
            {
                vol.Optional(
                    "household_title", default=default_household_title
                ): selector.TextSelector(),
                vol.Optional(
                    "number_inhabitants", default=default_inhabitants
                ): selector.NumberSelector(
                    selector.NumberSelectorConfig(
                        min=1, mode=selector.NumberSelectorMode.BOX
                    )
                ),
                vol.Optional("zip", default=default_zip): selector.TextSelector(),
                vol.Optional(
                    "country", default=default_country
                ): selector.CountrySelector(),
            }
        )

        return self.async_show_form(
            step_id="household",
            data_schema=schema,
            errors=errors,
            description_placeholders={"name": NAME},
        )

    async def async_step_finish_flow(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Finalize the configuration flow and save data entries."""
        if not self.observed_devices and not self.manual_meters:
            return self.async_abort(reason="no_configured_items")

        api = PowerCollectAPI(
            base_url=POWERCOLLECT_BASE_URL,
            api_key=self.api_key,
            session=async_get_clientsession(self.hass),
            client_id=self.client_id,
        )

        dev_reg = dr.async_get(self.hass)
        ent_reg = er.async_get(self.hass)

        self.meters = []
        meter_ids = {}

        for device_id in self.observed_devices:
            device = dev_reg.async_get(device_id)
            name = (
                device.name_by_user or device.name or "Unknown Device"
                if device
                else "Unknown Device"
            )
            vendor = (device.manufacturer or "") if device else ""
            model = (device.model or "") if device else ""

            entries = er.async_entries_for_device(ent_reg, device_id)
            entity_ids = [e.entity_id for e in entries if e.domain == "sensor"]

            try:
                meter_id = await api.register_meter(name, vendor, model)
                meter_ids[device_id] = meter_id

                self.meters.append(
                    MeterListEntry(
                        meter_id=meter_id,
                        name=name,
                        vendor=vendor,
                        model=model,
                        entity_ids=entity_ids,
                    )
                )
            except PowerCollectError as e:
                _LOGGER.error("Failed to register hardware meter %s: %s", name, e)

        for meter_name, assigned_entities in self.manual_meters.items():
            try:
                meter_id = await api.register_meter(meter_name, "", "")

                # Keep map synced
                self.custom_meter_map[meter_name] = meter_id

                self.meters.append(
                    MeterListEntry(
                        meter_id=meter_id,
                        name=meter_name,
                        vendor="",
                        model="",
                        entity_ids=assigned_entities,
                    )
                )
            except PowerCollectError as e:
                _LOGGER.error("Failed to register virtual meter %s: %s", meter_name, e)

        saved_data = {
            "api_key": self.api_key,
            "clientId": self.client_id,
            "observed_devices": meter_ids,
            "manual_meters": self.manual_meters,
            "meters": [meter.as_dict() for meter in self.meters],
        }
        assert self.client_id is not None
        return self.async_create_entry(
            title=f"{NAME}: {self.client_id[:4]}", data=saved_data
        )


class PowerCollectOptionsFlow(PowerCollectBaseFlow, OptionsFlow):
    """Handle options flow for Power Collect."""

    def __init__(self, config_entry: ConfigEntry) -> None:
        """Initialize the Power Collect options flow."""
        self.api_key = config_entry.data.get("api_key")
        self.client_id = config_entry.data.get("clientId")

        self.observed_devices = list(
            config_entry.data.get("observed_devices", {}).keys()
        )
        self.manual_meters = dict(config_entry.data.get("manual_meters", {}))

        # Link existing virtual meters to their meter_ids so we can PATCH them correctly
        self.custom_meter_map = {}
        for m in config_entry.data.get("meters", []):
            if m["name"] in self.manual_meters:
                self.custom_meter_map[m["name"]] = m["meter_id"]

        self.meters = [
            MeterListEntry(
                meter_id=m["meter_id"],
                name=m["name"],
                vendor=m["vendor"],
                model=m["model"],
                entity_ids=m["entity_ids"],
            )
            for m in config_entry.data.get("meters", [])
        ]

        self.current_editing_meter = None
        self.is_options_flow = True

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Entry point for the options flow."""
        return await self.async_step_main_menu()

    async def async_step_change_api_key(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Form to modify the API Key during the options flow."""
        errors: dict[str, str] = {}

        if user_input is not None:
            api = PowerCollectAPI(
                base_url=POWERCOLLECT_BASE_URL,
                api_key=user_input["api_key"],
                session=async_get_clientsession(self.hass),
                client_id=None,
            )
            try:
                await validate_input(api, user_input)
                self.api_key = user_input["api_key"]
                self.client_id = api.client_id
                return await self.async_step_main_menu()
            except PowerCollectError as e:
                if isinstance(e, PowerCollectAuthError):
                    errors["base"] = "invalid_auth"
                elif isinstance(e, PowerCollectConnError):
                    errors["base"] = "cannot_connect"
                else:
                    errors["base"] = "unknown_error"

        schema = vol.Schema(
            {vol.Required("api_key", default=self.api_key): selector.TextSelector()}
        )
        return self.async_show_form(
            step_id="change_api_key", data_schema=schema, errors=errors
        )

    async def async_step_finish_flow(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Finalize the options flow, sync updates to the API, and update the config entry."""
        if not self.observed_devices and not self.manual_meters:
            return self.async_abort(reason="no_configured_items")

        api = PowerCollectAPI(
            base_url=POWERCOLLECT_BASE_URL,
            api_key=self.api_key,
            session=async_get_clientsession(self.hass),
            client_id=self.client_id,
        )

        dev_reg = dr.async_get(self.hass)
        ent_reg = er.async_get(self.hass)

        new_meters: list[MeterListEntry] = []
        new_observed_devices = {}

        old_observed_devices = self.config_entry.data.get("observed_devices", {})
        old_meters_data = self.config_entry.data.get("meters", [])

        # 1. HARDWARE DEVICES
        for device_id in self.observed_devices:
            device = dev_reg.async_get(device_id)
            name = (
                device.name_by_user or device.name or "Unknown Device"
                if device
                else "Unknown Device"
            )
            vendor = (device.manufacturer or "") if device else ""
            model = (device.model or "") if device else ""

            entries = er.async_entries_for_device(ent_reg, device_id)
            entity_ids = [e.entity_id for e in entries if e.domain == "sensor"]

            if device_id in old_observed_devices:
                meter_id = old_observed_devices[device_id]
                new_observed_devices[device_id] = meter_id

                # Compare against stored state to see if HA details changed (e.g., user renamed the plug)
                old_m = next(
                    (m for m in old_meters_data if m["meter_id"] == meter_id), None
                )
                if old_m and (
                    old_m["name"] != name
                    or old_m["vendor"] != vendor
                    or old_m["model"] != model
                ):
                    try:
                        await api.change_meter_details(meter_id, name, vendor, model)
                    except PowerCollectError as e:
                        _LOGGER.error("Failed to patch hardware meter %s: %s", name, e)

                new_meters.append(
                    MeterListEntry(meter_id, name, vendor, model, entity_ids)
                )
            else:
                # Completely new hardware device added
                try:
                    meter_id = await api.register_meter(name, vendor, model)
                    new_observed_devices[device_id] = meter_id
                    new_meters.append(
                        MeterListEntry(meter_id, name, vendor, model, entity_ids)
                    )
                except PowerCollectError as e:
                    _LOGGER.error("Failed to register hardware meter %s: %s", name, e)

        # 2. VIRTUAL METERS
        for meter_name, assigned_entities in self.manual_meters.items():
            if meter_name in self.custom_meter_map:
                meter_id = self.custom_meter_map[meter_name]

                # Check if it was renamed (name doesn't match original stored state)
                old_m = next(
                    (m for m in old_meters_data if m["meter_id"] == meter_id), None
                )
                if old_m and old_m["name"] != meter_name:
                    try:
                        await api.change_meter_details(meter_id, meter_name, "", "")
                    except PowerCollectError as e:
                        _LOGGER.error(
                            "Failed to patch virtual meter %s: %s", meter_name, e
                        )

                new_meters.append(
                    MeterListEntry(meter_id, meter_name, "", "", assigned_entities)
                )
            else:
                # Completely new virtual meter added
                try:
                    meter_id = await api.register_meter(meter_name, "", "")
                    self.custom_meter_map[meter_name] = meter_id
                    new_meters.append(
                        MeterListEntry(meter_id, meter_name, "", "", assigned_entities)
                    )
                except PowerCollectError as e:
                    _LOGGER.error(
                        "Failed to register virtual meter %s: %s", meter_name, e
                    )

        # 3. DELETIONS (Unregister meters no longer checked/present)
        new_meter_ids = {m.meter_id for m in new_meters}
        for old_m in old_meters_data:
            if old_m["meter_id"] not in new_meter_ids:
                try:
                    await api.unregister_meter(old_m["meter_id"])
                except PowerCollectError as e:
                    _LOGGER.error(
                        "Failed to unregister meter %s: %s", old_m["meter_id"], e
                    )

        # 4. SAVE & RELOAD
        saved_data = {
            "api_key": self.api_key,
            "clientId": self.client_id,
            "observed_devices": new_observed_devices,
            "manual_meters": self.manual_meters,
            "meters": [meter.as_dict() for meter in new_meters],
        }

        self.hass.config_entries.async_update_entry(self.config_entry, data=saved_data)

        # Hot-Reload the integration so __init__.py catches the new configuration instantly!
        await self.hass.config_entries.async_reload(self.config_entry.entry_id)

        return self.async_create_entry(title="", data={})


class CannotConnect(HomeAssistantError):
    """Error to indicate we cannot connect."""


class InvalidAuth(HomeAssistantError):
    """Error to indicate there is invalid auth."""
