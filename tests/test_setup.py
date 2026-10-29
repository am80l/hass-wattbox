"""Exercise startup failures without controlling physical outlets."""

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import aiohttp
import httpx
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady
from pywattbox.http_wattbox import HttpWattBox

from custom_components import wattbox
from custom_components.wattbox.const import DOMAIN, DOMAIN_DATA, PLATFORMS


async def wait_until(predicate):
    async with asyncio.timeout(2):
        while not predicate():
            await asyncio.sleep(0.001)


class StartupRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.hass = HomeAssistant("/tmp/wattbox-test-config")
        self.config = wattbox.CONFIG_SCHEMA(
            {
                DOMAIN: [
                    {"host": "slow.invalid", "name": "slow"},
                    {"host": "healthy.invalid", "name": "healthy"},
                ]
            }
        )
        self.device = SimpleNamespace(outlets={1: object()})
        self.factory = self.patch("_async_create_wattbox", new_callable=AsyncMock)
        self.poll = self.patch("async_track_time_interval")
        self.load = self.patch("discovery.async_load_platform", new_callable=AsyncMock)
        self.patch("SETUP_RETRY_DELAY", new=0.01)

    def patch(self, name, **kwargs):
        patcher = patch(f"custom_components.wattbox.{name}", **kwargs)
        result = patcher.start()
        self.addCleanup(patcher.stop)
        return result

    async def asyncTearDown(self):
        await self.hass.async_stop(force=True)

    async def test_one_timeout_does_not_block_other_devices_and_recovers(self):
        recovered = asyncio.Event()
        attempts = []

        async def create(hass, host, *args):
            attempts.append(host)
            if host == "slow.invalid" and not recovered.is_set():
                raise httpx.ReadTimeout("Device is starting")
            return self.device

        self.factory.side_effect = create
        self.assertTrue(await wattbox.async_setup(self.hass, self.config))
        await wait_until(lambda: "healthy" in self.hass.data[DOMAIN_DATA])
        await self.hass.async_block_till_done()
        self.assertNotIn("slow", self.hass.data[DOMAIN_DATA])
        self.assertEqual(self.load.await_count, len(PLATFORMS))
        self.assertEqual(self.poll.call_count, 1)

        recovered.set()
        await wait_until(lambda: "slow" in self.hass.data[DOMAIN_DATA])
        await self.hass.async_block_till_done()
        await asyncio.sleep(0.03)
        self.assertEqual(self.load.await_count, 2 * len(PLATFORMS))
        self.assertEqual(self.poll.call_count, 2)
        self.assertEqual(attempts.count("healthy.invalid"), 1)
        self.assertGreaterEqual(attempts.count("slow.invalid"), 2)

    async def test_blocked_connection_does_not_block_component_or_other_devices(self):
        async def create(hass, host, *args):
            if host == "slow.invalid":
                await asyncio.Event().wait()
            return self.device

        self.factory.side_effect = create
        self.assertTrue(
            await asyncio.wait_for(wattbox.async_setup(self.hass, self.config), 1)
        )
        await wait_until(lambda: "healthy" in self.hass.data[DOMAIN_DATA])
        await self.hass.async_block_till_done()
        self.assertEqual(self.load.await_count, len(PLATFORMS))
        tasks = list(self.hass._background_tasks)
        self.assertEqual(len(tasks), 1)
        await self.hass.async_stop(force=True)
        self.assertTrue(tasks[0].cancelled())

    async def test_all_offline_retry_then_shutdown_cancels_waits(self):
        self.factory.side_effect = httpx.ConnectError("Offline")
        self.assertTrue(await wattbox.async_setup(self.hass, self.config))
        await wait_until(lambda: self.factory.await_count >= 4)
        self.assertEqual(self.hass.data[DOMAIN_DATA], {})
        self.load.assert_not_awaited()
        self.poll.assert_not_called()
        tasks = list(self.hass._background_tasks)
        self.assertEqual(len(tasks), 2)
        await self.hass.async_stop(force=True)
        self.assertTrue(all(task.cancelled() for task in tasks))
        attempts = self.factory.await_count
        await asyncio.sleep(0.03)
        self.assertEqual(self.factory.await_count, attempts)

    async def test_config_entry_failure_uses_ha_setup_retry(self):
        self.factory.side_effect = httpx.ReadTimeout("Device is starting")
        entry = SimpleNamespace(data=self.config[DOMAIN][0])
        with self.assertRaises(ConfigEntryNotReady):
            await wattbox.async_setup_entry(self.hass, entry)
        self.assertEqual(self.hass.data[DOMAIN_DATA], {})
        self.poll.assert_not_called()

    async def test_no_yaml_preserves_existing_device_data(self):
        self.hass.data[DOMAIN_DATA] = {"existing": self.device}
        self.assertTrue(await wattbox.async_setup(self.hass, {}))
        self.assertEqual(self.hass.data[DOMAIN_DATA], {"existing": self.device})
        self.factory.assert_not_awaited()


class HttpStartupTests(unittest.IsolatedAsyncioTestCase):
    async def fetch_initial(self, payload, status=200):
        requests = []

        async def handle(reader, writer):
            requests.append(await reader.readuntil(b"\r\n\r\n"))
            writer.write(
                f"HTTP/1.1 {status} Test\r\nTransfer-Encoding: chunked\r\n"
                "Connection: close\r\n\r\n".encode()
                + f"{len(payload):x}\r\n".encode()
                + payload
                + b"\r\n"
            )
            await writer.drain()
            await asyncio.sleep(0.01)
            # Reproduce firmware that sends XML but omits the final zero chunk.
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        async with server, aiohttp.ClientSession() as session:
            with (
                patch.object(wattbox, "async_get_clientsession", return_value=session),
                patch.object(wattbox, "async_import_module", new_callable=AsyncMock),
                patch.object(
                    HttpWattBox, "async_get_initial", new_callable=AsyncMock
                ) as original,
            ):
                original.side_effect = AssertionError("Unwrapped initial read")
                device = await wattbox._async_create_wattbox(
                    SimpleNamespace(),
                    "127.0.0.1",
                    server.sockets[0].getsockname()[1],
                    "test-user",
                    "test-password",
                )
                original.assert_not_awaited()
                self.assertTrue(device.async_client.is_closed)
                self.assertEqual(len(requests), 1)
                self.assertIn(b"GET /wattbox_info.xml ", requests[0])
                return device

    async def test_first_read_tolerates_missing_final_chunk_and_loads_status(self):
        device = await self.fetch_initial(
            b"<wattbox><hardware_version>WB-700-IPV-12</hardware_version>"
            b"<serial_number>test-serial</serial_number><outlet_status>1,0</outlet_status>"
            b"<outlet_method>1,1</outlet_method><current_value>21</current_value>"
            b"<power_value>245</power_value></wattbox>"
        )
        self.assertEqual(device.serial_number, "test-serial")
        self.assertEqual(len(device.outlets), 2)
        self.assertIs(device.outlets[1].status, True)
        self.assertIs(device.outlets[2].status, False)
        self.assertEqual(device.current_value, 2.1)
        self.assertEqual(device.power_value, 245)

    async def test_login_page_cannot_silently_create_a_device_without_outlets(self):
        with self.assertRaisesRegex(ValueError, "no outlets"):
            await self.fetch_initial(b"<html><body>Login</body></html>")

    async def test_initial_auth_failure_is_not_treated_as_success(self):
        with self.assertRaises(httpx.HTTPStatusError) as raised:
            await self.fetch_initial(b"Unauthorized", status=401)
        self.assertEqual(raised.exception.response.status_code, 401)
