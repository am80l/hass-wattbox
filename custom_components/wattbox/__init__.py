"""
Component to integrate with wattbox.

For more details about this component, please refer to
https://github.com/eseglem/hass-wattbox/
"""

import asyncio
import logging
from datetime import datetime
from functools import partial
from typing import Any, Final, cast

import aiohttp
import homeassistant.helpers.config_validation as cv
import httpx
import voluptuous as vol
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import (
    CONF_HOST,
    CONF_NAME,
    CONF_PASSWORD,
    CONF_PORT,
    CONF_RESOURCES,
    CONF_SCAN_INTERVAL,
    CONF_USERNAME,
)
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers import discovery
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.dispatcher import async_dispatcher_send
from homeassistant.helpers.event import async_track_time_interval
from homeassistant.helpers.importlib import async_import_module
from homeassistant.helpers.typing import ConfigType
from pywattbox.base import BaseWattBox, Commands

from .const import (
    BINARY_SENSOR_TYPES,
    CONF_NAME_REGEXP,
    CONF_SKIP_REGEXP,
    DEFAULT_NAME,
    DEFAULT_PASSWORD,
    DEFAULT_PORT,
    DEFAULT_SCAN_INTERVAL,
    DEFAULT_USER,
    DOMAIN,
    DOMAIN_DATA,
    PLATFORMS,
    SENSOR_TYPES,
    STARTUP,
    TOPIC_UPDATE,
)

REQUIREMENTS: Final[list[str]] = ["pywattbox>=0.7.2"]

_LOGGER = logging.getLogger(__name__)

ALL_SENSOR_TYPES: Final[list[str]] = [*BINARY_SENSOR_TYPES.keys(), *SENSOR_TYPES.keys()]
HTTP_CLIENT_HEADERS: Final[dict[str, str]] = {
    "Connection": "close",
    "User-Agent": "HA",
}
SETUP_RETRY_DELAY: Final[int] = 30

WATTBOX_HOST_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_HOST): cv.string,
        vol.Optional(CONF_PORT, default=DEFAULT_PORT): int,
        vol.Optional(CONF_USERNAME, default=DEFAULT_USER): cv.string,
        vol.Optional(CONF_PASSWORD, default=DEFAULT_PASSWORD): cv.string,
        vol.Optional(CONF_NAME, default=DEFAULT_NAME): cv.string,
        vol.Optional(CONF_NAME_REGEXP): cv.string,
        vol.Optional(CONF_SKIP_REGEXP): cv.string,
        vol.Optional(CONF_RESOURCES, default=ALL_SENSOR_TYPES): vol.All(
            cv.ensure_list, [vol.In(ALL_SENSOR_TYPES)]
        ),
        vol.Optional(CONF_SCAN_INTERVAL, default=DEFAULT_SCAN_INTERVAL): cv.time_period,
    }
)

CONFIG_SCHEMA = vol.Schema(
    {
        DOMAIN: vol.All(cv.ensure_list, [WATTBOX_HOST_SCHEMA]),
    },
    extra=vol.ALLOW_EXTRA,
)


async def _async_create_wattbox(
    hass: HomeAssistant, host: str, port: int, username: str, password: str
) -> BaseWattBox:
    """Create a WattBox instance based on port (IP or HTTP)."""
    if port in (22, 23):
        _LOGGER.debug("Importing IP Wattbox")
        from pywattbox.ip_wattbox import async_create_ip_wattbox

        # Pre-import the transport plugin to avoid blocking call issues
        transport = "asyncssh" if port == 22 else "asynctelnet"
        await async_import_module(
            hass, f"scrapli.transport.plugins.{transport}.transport"
        )

        _LOGGER.debug("Creating IP WattBox")
        wattbox: BaseWattBox = await async_create_ip_wattbox(
            host=host, user=username, password=password, port=port
        )
    else:
        _LOGGER.debug("Importing HTTP Wattbox")
        from pywattbox.http_wattbox import HttpWattBox

        # Pre-import the encoding to avoid blocking call issues
        await async_import_module(hass, "encodings.ascii")

        _LOGGER.debug("Creating HTTP WattBox")
        # The library factory performs HTTP reads before we can wrap its client.
        wattbox = HttpWattBox(host=host, user=username, password=password, port=port)
        await _async_setup_http_client_recovery(hass, wattbox)
        await wattbox.async_get_initial()
        if not wattbox.outlets:
            raise ValueError("WattBox initial response contained no outlets")

    return wattbox


async def _async_setup_http_client_recovery(
    hass: HomeAssistant, wattbox: BaseWattBox
) -> None:
    """Use HA's HTTP session before any pywattbox network requests."""
    if not hasattr(wattbox, "async_client"):
        return

    wattbox_http = cast(Any, wattbox)
    wattbox_http._ha_aiohttp_session = async_get_clientsession(hass)
    old_client = getattr(wattbox, "async_client", None)
    await _async_close_http_client(old_client)

    if getattr(wattbox, "_ha_http_client_recovery_wrapped", False):
        return

    _wrap_http_update_methods(wattbox)
    _wrap_http_command_methods(wattbox)

    wattbox_http._ha_http_client_recovery_wrapped = True


def _wrap_http_update_methods(wattbox: BaseWattBox) -> None:
    """Use tolerant XML reads for WattBox status updates."""
    wattbox_http = cast(Any, wattbox)

    async def async_get_initial() -> None:
        response = await _async_get_wattbox_info_response(wattbox)
        wattbox_http.parse_initial(response)
        wattbox_http.parse_update(response)

    async def async_update() -> None:
        response = await _async_get_wattbox_info_response(wattbox)
        wattbox_http.parse_update(response)

    wattbox_http.async_get_initial = async_get_initial
    wattbox_http.async_update = async_update


async def _async_get_wattbox_info_response(wattbox: BaseWattBox) -> httpx.Response:
    """Fetch wattbox_info.xml while tolerating early connection closes."""
    wattbox_http = cast(Any, wattbox)
    session = wattbox_http._ha_aiohttp_session
    url = f"{wattbox_http.base_host}/wattbox_info.xml"
    chunks: list[bytes] = []
    status = 0

    try:
        async with session.get(
            url,
            auth=aiohttp.BasicAuth(wattbox.user, wattbox.password),
            headers=HTTP_CLIENT_HEADERS,
            timeout=aiohttp.ClientTimeout(total=8),
        ) as response:
            status = response.status
            try:
                async for chunk in response.content.iter_any():
                    chunks.append(chunk)
            except aiohttp.ClientPayloadError as error:
                if not chunks:
                    raise
                _LOGGER.debug(
                    "Using partial WattBox XML response from %s after early close: %s",
                    wattbox,
                    error,
                )
    except aiohttp.ClientError as error:
        raise httpx.TransportError(str(error)) from error
    except TimeoutError as error:
        raise httpx.TimeoutException(str(error)) from error

    http_response = httpx.Response(
        status_code=status,
        content=b"".join(chunks),
        request=httpx.Request("GET", url),
    )
    http_response.raise_for_status()
    return http_response


def _wrap_http_command_methods(wattbox: BaseWattBox) -> None:
    """Send commands once while tolerating broken WattBox response bodies."""
    wattbox_http = cast(Any, wattbox)

    async def async_send_command(outlet: int, command: Commands) -> None:
        await _async_send_wattbox_command(wattbox, outlet, command)

    async def async_send_master_command(command: Commands) -> None:
        if command not in (Commands.ON, Commands.OFF):
            raise ValueError(
                f"Command ({command}) can only be `Commands.ON` or `Commands.OFF`."
            )
        desired_state = command == Commands.ON
        for outlet in wattbox.outlets.values():
            if outlet.method and outlet.status is not desired_state:
                await async_send_command(outlet.index, command)

    wattbox_http.async_send_command = async_send_command
    wattbox_http.async_send_master_command = async_send_master_command


async def _async_send_wattbox_command(
    wattbox: BaseWattBox, outlet: int, command: Commands
) -> None:
    """Send one HTTP command without requiring a complete response body."""
    wattbox_http = cast(Any, wattbox)
    session = wattbox_http._ha_aiohttp_session
    url = f"{wattbox_http.base_host}/control.cgi"
    params = {"outlet": outlet, "command": command.value}
    status = 0

    try:
        async with session.get(
            url,
            params=params,
            auth=aiohttp.BasicAuth(wattbox.user, wattbox.password),
            headers=HTTP_CLIENT_HEADERS,
            timeout=aiohttp.ClientTimeout(total=8),
        ) as response:
            status = response.status
    except aiohttp.ClientError as error:
        raise httpx.TransportError(str(error)) from error
    except TimeoutError as error:
        raise httpx.TimeoutException(str(error)) from error

    if status == 400:
        _LOGGER.debug(
            "WattBox returned HTTP 400 after accepting outlet %s command %s",
            outlet,
            command.name,
        )
        return

    http_response = httpx.Response(
        status_code=status,
        request=httpx.Request("GET", url, params=params),
    )
    http_response.raise_for_status()


async def _async_close_http_client(async_client: Any) -> None:
    """Close an httpx client without breaking WattBox setup/update."""
    if async_client is None or getattr(async_client, "is_closed", False):
        return
    try:
        await async_client.aclose()
    except Exception as error:
        _LOGGER.debug("Could not close old WattBox HTTP client: %s", error)


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up this component."""
    _LOGGER.info(STARTUP)

    hass.data.setdefault(DOMAIN_DATA, {})

    # Only process YAML config if it exists
    domain_config = config.get(DOMAIN, [])
    if domain_config:
        _LOGGER.debug(
            "Found YAML configuration for %d WattBox device(s)", len(domain_config)
        )
    else:
        _LOGGER.debug("No YAML configuration found, will rely on config entries")

    for wattbox_host in domain_config:
        # HA cancels background tasks at shutdown; retries must not block startup.
        hass.async_create_background_task(
            _async_setup_yaml_wattbox(hass, wattbox_host, config),
            f"WattBox setup: {wattbox_host[CONF_NAME]}",
        )

    return True


async def _async_setup_yaml_wattbox(
    hass: HomeAssistant, wattbox_host: ConfigType, config: ConfigType
) -> None:
    """Connect one YAML device independently, retrying until it is ready."""
    name = wattbox_host[CONF_NAME]
    while True:
        wattbox: BaseWattBox
        try:
            wattbox = await _async_create_wattbox(
                hass,
                wattbox_host[CONF_HOST],
                wattbox_host[CONF_PORT],
                wattbox_host[CONF_USERNAME],
                wattbox_host[CONF_PASSWORD],
            )
        except Exception as error:
            _LOGGER.warning(
                "WattBox %s is not ready (%r); retrying in %s seconds",
                name,
                error,
                SETUP_RETRY_DELAY,
            )
            await asyncio.sleep(SETUP_RETRY_DELAY)
            continue
        break

    hass.data[DOMAIN_DATA][name] = wattbox

    for platform in PLATFORMS:
        hass.async_create_task(
            discovery.async_load_platform(hass, platform, DOMAIN, wattbox_host, config)
        )

    async_track_time_interval(
        hass,
        partial(update_data, hass=hass, name=name),
        wattbox_host[CONF_SCAN_INTERVAL],
    )
    _LOGGER.info("WattBox %s connected with %s outlets", name, len(wattbox.outlets))


async def update_data(_dt: datetime, hass: HomeAssistant, name: str) -> None:
    """Update data."""

    # This is where the main logic to update platform data goes.
    try:
        wattbox = hass.data[DOMAIN_DATA][name]
        await wattbox.async_update()
        _LOGGER.debug("Updated: %s - %s", wattbox, repr(wattbox))
        # Send update to topic for entities to see
        async_dispatcher_send(hass, TOPIC_UPDATE.format(DOMAIN, name))
    except Exception as error:
        _LOGGER.error(
            "Could not update data for %s (%s) - %s", repr(wattbox), wattbox, error
        )


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up WattBox from a config entry."""
    if DOMAIN_DATA not in hass.data:
        hass.data[DOMAIN_DATA] = {}

    # Extract configuration
    host = entry.data[CONF_HOST]
    port = entry.data[CONF_PORT]
    username = entry.data[CONF_USERNAME]
    password = entry.data[CONF_PASSWORD]
    name = entry.data[CONF_NAME]
    scan_interval = entry.data.get(CONF_SCAN_INTERVAL, DEFAULT_SCAN_INTERVAL)

    wattbox: BaseWattBox
    try:
        wattbox = await _async_create_wattbox(hass, host, port, username, password)
    except Exception as error:
        raise ConfigEntryNotReady(
            f"Could not connect to WattBox {name}: {error!r}"
        ) from error

    hass.data[DOMAIN_DATA][name] = wattbox

    # Forward entry setup to platforms
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    # Use the scan interval to trigger updates
    async_track_time_interval(
        hass, partial(update_data, hass=hass, name=name), scan_interval
    )

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    name = entry.data[CONF_NAME]

    # Unload platforms
    unload_ok = all(
        await asyncio.gather(
            *[
                hass.config_entries.async_forward_entry_unload(entry, platform)
                for platform in PLATFORMS
            ]
        )
    )

    if unload_ok:
        # Remove the wattbox from data
        if name in hass.data[DOMAIN_DATA]:
            await _async_close_http_client(
                getattr(hass.data[DOMAIN_DATA][name], "async_client", None)
            )
            del hass.data[DOMAIN_DATA][name]

    return unload_ok
