"""Switch platform for wattbox."""

import logging
import re
from collections.abc import Awaitable, Callable
from typing import Any

from homeassistant.components.switch import SwitchDeviceClass, SwitchEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_NAME
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import PlatformNotReady
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType
from httpx import HTTPStatusError
from pywattbox.base import BaseWattBox, Commands, Outlet

from .const import CONF_NAME_REGEXP, CONF_SKIP_REGEXP, DOMAIN_DATA, PLUG_ICON
from .entity import WattBoxEntity

_LOGGER = logging.getLogger(__name__)


def validate_regex(config: ConfigType, key: str) -> re.Pattern[str] | None:
    regexp_str: str = config.get(key, "")
    if regexp_str:
        try:
            return re.compile(regexp_str)
        except re.error:
            _LOGGER.error("Invalid %s: %s", key, regexp_str)
    return None


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up WattBox switches from a config entry."""
    try:
        name: str = entry.data[CONF_NAME]

        entities: list[WattBoxEntity] = []
        wattbox: BaseWattBox = hass.data[DOMAIN_DATA][name]

        # For config entries, we'll include all outlets by default
        # TODO: Add options for name_regexp and skip_regexp in config flow
        name_regexp = None
        skip_regexp = None

        skipped_an_outlet = False
        for i, outlet in wattbox.outlets.items():
            outlet_name = outlet.name or ""

            # Check to skip outlets
            if skip_regexp and skip_regexp.search(outlet_name):
                _LOGGER.debug("Skipping Outlet: %s - %s", i, outlet_name)
                skipped_an_outlet = True
                continue

            # Check outlet name pattern
            if name_regexp and not name_regexp.search(outlet_name):
                _LOGGER.debug("Not including Outlet: %s - %s", i, outlet_name)
                continue

            try:
                entities.append(WattBoxBinarySwitch(hass, name, i, outlet_name))
            except Exception as err:
                _LOGGER.error("Failed to append WattBoxBinarySwitch: %s", err)
                raise PlatformNotReady from err

        # Add the master switch if no outlets were skipped
        if not skipped_an_outlet:
            entities.append(WattBoxMasterSwitch(hass, name))
        else:
            _LOGGER.debug(
                "Skipping master switch because an outlet was skipped for %s", name
            )

        if skipped_an_outlet:
            _LOGGER.warning(
                "Some outlets were skipped. "
                "Check your settings for %s if this was unintentional.",
                CONF_SKIP_REGEXP,
            )

        async_add_entities(entities)
    except Exception as err:
        _LOGGER.error("Error setting up switch platform: %s", err)
        raise PlatformNotReady from err


async def async_setup_platform(
    hass: HomeAssistant,
    config: ConfigType,
    async_add_entities: AddEntitiesCallback,
    discovery_info: DiscoveryInfoType,
) -> None:
    """Setup switch platform (legacy YAML support)."""
    try:
        name: str = discovery_info[CONF_NAME]

        entities: list[WattBoxEntity] = []
        wattbox: BaseWattBox = hass.data[DOMAIN_DATA][name]

        name_regexp = validate_regex(config, CONF_NAME_REGEXP)
        skip_regexp = validate_regex(config, CONF_SKIP_REGEXP)

        skipped_an_outlet = False
        for i, outlet in wattbox.outlets.items():
            outlet_name = outlet.name or ""

            # Skip outlets if they match regex
            if skip_regexp and skip_regexp.search(outlet_name):
                _LOGGER.debug("Skipping switch #%s - %s", i, outlet_name)
                skipped_an_outlet = True
                continue

            if name_regexp:
                if matched := name_regexp.search(outlet_name):
                    outlet_name = matched.group()
                    try:
                        outlet_name = matched.group(1)
                    except re.error:
                        pass

            _LOGGER.debug("Adding switch #%s - %s", i, outlet_name)
            try:
                entities.append(WattBoxBinarySwitch(hass, name, i, outlet_name))
            except Exception as err:
                _LOGGER.error("Failed to append WattBoxBinarySwitch: %s", err)
                raise PlatformNotReady from err

        # Skip the master switch iff any of the outlets are skipped
        if not skipped_an_outlet:
            entities.append(WattBoxMasterSwitch(hass, name))
        else:
            _LOGGER.debug(
                "Skipping master switch because an outlet was skipped for %s", name
            )

        async_add_entities(entities)
    except Exception as err:
        _LOGGER.error("Error setting up switch platform: %s", err)
        raise PlatformNotReady from err


class WattBoxBinarySwitch(WattBoxEntity, SwitchEntity):
    """WattBox switch class."""

    _attr_device_class = SwitchDeviceClass.OUTLET
    _attr_icon = PLUG_ICON
    _outlet: Outlet

    def __init__(
        self, hass: HomeAssistant, name: str, index: int, outlet_name: str = ""
    ) -> None:
        super().__init__(hass, name, index)
        # Master Outlet (index == 0) is not in the oulets dict
        if index:
            self._outlet = self._wattbox.outlets[index]
        # Determine outlet name
        if outlet_name := outlet_name.strip():
            self._attr_name = f"{name} {outlet_name}"
        else:
            self._attr_name = f"{name} Outlet {index}"
        self._attr_unique_id = f"{self._wattbox.serial_number}-switch-{index}"

    async def async_update(self) -> None:
        """Update the sensor."""
        # Check the data and update the value.
        self._attr_is_on = self._outlet.status

        # Set/update attributes
        self._attr_extra_state_attributes["name"] = self._outlet.name
        self._attr_extra_state_attributes["method"] = self._outlet.method
        self._attr_extra_state_attributes["index"] = self._outlet.index

    async def async_turn_on(self, **_kwargs: Any) -> None:
        """Turn on the switch."""
        _LOGGER.debug("Turning On: %s - %s", self._wattbox, self._outlet)
        _LOGGER.debug(
            "Current Outlet Before: %s - %s", self._outlet.status, repr(self._outlet)
        )
        await self._async_run_outlet_command(True, self._outlet.async_turn_on)

    async def async_turn_off(self, **_kwargs: Any) -> None:
        """Turn off the switch."""
        _LOGGER.debug("Turning Off: %s - %s", self._wattbox, self._outlet)
        _LOGGER.debug(
            "Current Outlet Before: %s - %s", self._outlet.status, repr(self._outlet)
        )
        await self._async_run_outlet_command(False, self._outlet.async_turn_off)

    async def _async_run_outlet_command(
        self, desired_state: bool, command: Callable[[], Awaitable[None]]
    ) -> None:
        """Run an outlet command and verify state from the WattBox afterward."""
        try:
            await command()
        except HTTPStatusError as err:
            if err.response.status_code != 400:
                raise

            await self._async_refresh_from_wattbox()
            if self._outlet.status is desired_state:
                _LOGGER.warning(
                    "WattBox returned HTTP 400 for %s, but outlet %s reached %s",
                    self._wattbox,
                    self._outlet.index,
                    desired_state,
                )
                return
            raise

        await self._async_refresh_from_wattbox()

    async def _async_refresh_from_wattbox(self) -> None:
        """Refresh WattBox data and publish the current outlet state."""
        await self._wattbox.async_update()
        await self.async_update()
        self.async_write_ha_state()


class WattBoxMasterSwitch(WattBoxBinarySwitch):
    """WattBox master switch class."""

    _outlet: Outlet | None  # type: ignore[assignment]

    def __init__(self, hass: HomeAssistant, name: str) -> None:
        super().__init__(hass, name, 0)
        self._outlet = self._wattbox.master_outlet
        self._attr_name = f"{name} Master Switch"
        self._attr_unique_id = f"{self._wattbox.serial_number}-switch-master"

    async def async_update(self) -> None:
        """Update the sensor."""
        if self._outlet is not None:
            # Check the data and update the value.
            self._attr_is_on = self._outlet.status

    async def async_turn_on(self, **kwargs: Any) -> None:
        """Turn on the switch."""
        if self._outlet is not None:
            await super().async_turn_on(**kwargs)

    async def async_turn_off(self, **kwargs: Any) -> None:
        """Turn off the switch."""
        if self._outlet is not None:
            async_send_master_command = getattr(
                self._wattbox, "async_send_master_command", None
            )
            if callable(async_send_master_command):
                await self._async_run_outlet_command(
                    False, lambda: async_send_master_command(Commands.OFF)
                )
            else:
                await super().async_turn_off(**kwargs)
