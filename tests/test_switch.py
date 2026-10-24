"""Exercise command verification with delayed and failed device responses."""

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call, patch

from homeassistant.exceptions import HomeAssistantError
from httpx import HTTPStatusError, Request, Response, TransportError
from pywattbox.base import Commands, Outlet

from custom_components.wattbox import switch
from custom_components.wattbox.const import DOMAIN_DATA


def http_error(status):
    """Build a device HTTP error without making a network request."""
    request = Request("GET", "http://wattbox.invalid/control.cgi")
    return HTTPStatusError(
        "Device response", request=request, response=Response(status, request=request)
    )


class CommandVerificationTests(unittest.IsolatedAsyncioTestCase):
    """Drive real switch methods with a simulated WattBox."""

    def setUp(self):
        self.wattbox = SimpleNamespace(
            serial_number="test-device",
            host="",
            port=80,
            outlets={},
            async_send_command=AsyncMock(),
            async_update=AsyncMock(),
        )
        self.outlet = self.add_outlet(11, True)
        self.wattbox.master_outlet = Outlet(0, self.wattbox)
        self.hass = SimpleNamespace(data={DOMAIN_DATA: {"test": self.wattbox}})
        self.entity = switch.WattBoxBinarySwitch(self.hass, "test", 11)
        self.entity.async_write_ha_state = Mock()
        for name, value in (
            ("COMMAND_SETTLE_DELAY", 0.001),
            ("COMMAND_POLL_INTERVAL", 0.002),
            ("COMMAND_VERIFY_TIMEOUT", 0.1),
        ):
            patcher = patch.object(switch, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def add_outlet(self, index, state, managed=True):
        outlet = Outlet(index, self.wattbox)
        outlet.status = state
        outlet.method = managed
        self.wattbox.outlets[index] = outlet
        return outlet

    def status_responses(self, *responses):
        """Apply each device snapshot only when a status read succeeds."""
        pending = iter(responses)

        async def update():
            response = next(pending)
            if isinstance(response, Exception):
                raise response
            for index, state in response.items():
                self.wattbox.outlets[index].status = state

        self.wattbox.async_update.side_effect = update

    async def test_delayed_on_and_off_send_only_one_command(self):
        for desired in (False, True):
            with self.subTest(desired=desired):
                self.outlet.status = not desired
                self.wattbox.async_send_command.reset_mock()
                self.wattbox.async_update.reset_mock()
                self.status_responses(
                    {11: not desired}, {11: not desired}, {11: desired}
                )
                command = (
                    self.entity.async_turn_on if desired else self.entity.async_turn_off
                )
                await command()
                self.assertIs(self.entity.is_on, desired)
                self.wattbox.async_send_command.assert_awaited_once_with(
                    11, Commands.ON if desired else Commands.OFF
                )
                self.assertEqual(self.wattbox.async_update.await_count, 3)

    async def test_response_error_then_delayed_success_does_not_resend(self):
        for error in (http_error(400), TransportError("Incomplete response")):
            with self.subTest(error=type(error).__name__):
                self.outlet.status = True
                self.wattbox.async_send_command.reset_mock()
                self.wattbox.async_send_command.side_effect = error
                self.status_responses({11: True}, {11: False})
                await self.entity.async_turn_off()
                self.wattbox.async_send_command.assert_awaited_once_with(
                    11, Commands.OFF
                )
                self.assertIs(self.entity.is_on, False)

    async def test_transient_status_errors_allow_later_confirmation(self):
        self.status_responses(
            TransportError("Temporary connection failure"),
            http_error(400),
            {11: True},
            {11: False},
        )
        await self.entity.async_turn_off()
        self.wattbox.async_send_command.assert_awaited_once_with(11, Commands.OFF)
        self.assertIs(self.entity.is_on, False)

    async def test_unchanged_state_still_fails_after_deadline(self):
        with self.assertRaisesRegex(HomeAssistantError, r"confirm outlets \[11\] off"):
            await self.entity.async_turn_off()
        self.wattbox.async_send_command.assert_awaited_once_with(11, Commands.OFF)
        self.assertGreater(self.wattbox.async_update.await_count, 1)
        self.assertIs(self.entity.is_on, True)

    async def test_cached_matching_state_requires_a_successful_read(self):
        self.outlet.status = False
        self.wattbox.async_update.side_effect = TransportError("Offline")
        with self.assertRaises(HomeAssistantError):
            await self.entity.async_turn_off()
        self.wattbox.async_send_command.assert_awaited_once_with(11, Commands.OFF)
        self.entity.async_write_ha_state.assert_not_called()

    async def test_unresponsive_read_is_bounded_by_deadline(self):
        async def blocked_update():
            await asyncio.Event().wait()

        self.wattbox.async_update.side_effect = blocked_update
        with self.assertRaises(HomeAssistantError):
            await asyncio.wait_for(self.entity.async_turn_off(), timeout=1)
        self.wattbox.async_send_command.assert_awaited_once_with(11, Commands.OFF)

    async def test_authentication_errors_are_not_retried(self):
        self.wattbox.async_send_command.side_effect = http_error(401)
        with self.assertRaises(HTTPStatusError):
            await self.entity.async_turn_off()
        self.wattbox.async_send_command.assert_awaited_once()
        self.wattbox.async_update.assert_not_awaited()

    async def test_status_authentication_errors_are_not_retried(self):
        self.wattbox.async_update.side_effect = http_error(401)
        with self.assertRaises(HTTPStatusError):
            await self.entity.async_turn_off()
        self.wattbox.async_update.assert_awaited_once()

    async def test_master_waits_for_every_managed_outlet(self):
        self.add_outlet(12, True)
        self.add_outlet(13, True, managed=False)
        master = switch.WattBoxMasterSwitch(self.hass, "test")
        states = []
        master.async_write_ha_state = Mock(
            side_effect=lambda: states.append(master.is_on)
        )
        self.status_responses({11: False, 12: True}, {11: False, 12: False})
        await master.async_turn_off()
        self.assertEqual(
            self.wattbox.async_send_command.await_args_list,
            [call(11, Commands.OFF), call(12, Commands.OFF)],
        )
        self.assertEqual(states, [True, False])
        self.assertIs(self.wattbox.outlets[13].status, True)

    async def test_master_reports_a_real_failure(self):
        master = switch.WattBoxMasterSwitch(self.hass, "test")
        master.async_write_ha_state = Mock()
        with self.assertRaises(HomeAssistantError):
            await master.async_turn_off()
        self.wattbox.async_send_command.assert_awaited_once_with(11, Commands.OFF)
        self.assertIs(master.is_on, True)

    async def test_cancellation_propagates_without_resending(self):
        reading = asyncio.Event()

        async def blocked_update():
            reading.set()
            await asyncio.Event().wait()

        self.wattbox.async_update.side_effect = blocked_update
        task = asyncio.create_task(self.entity.async_turn_off())
        await reading.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.wattbox.async_send_command.assert_awaited_once_with(11, Commands.OFF)
