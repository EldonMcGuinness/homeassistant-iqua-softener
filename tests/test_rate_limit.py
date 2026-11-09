import asyncio
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

try:
    import requests
except ModuleNotFoundError:
    # The integration declares requests as a Home Assistant dependency. Keep
    # these offline unit tests runnable when that runtime dependency is absent.
    class RequestException(Exception):
        pass

    class HTTPError(Exception):
        pass

    requests = SimpleNamespace(
        Response=object,
        HTTPError=HTTPError,
        exceptions=SimpleNamespace(RequestException=RequestException),
        Session=lambda: (_ for _ in ()).throw(
            AssertionError("Tests must use the mocked session")
        ),
    )
    sys.modules["requests"] = requests

VENDOR_PATH = (
    Path(__file__).resolve().parents[1]
    / "custom_components"
    / "iqua_softener"
    / "vendor"
)
sys.path.insert(0, str(VENDOR_PATH))

from iqua_softener import IquaRateLimitException, IquaSoftener


class FakeResponse:
    def __init__(self, status_code, payload=None, headers=None):
        self.status_code = status_code
        self.headers = headers or {}
        self.text = "mock response"
        self._payload = payload or {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"mock HTTP {self.status_code}")


def make_client():
    client = IquaSoftener(
        "user@example.test",
        "not-a-real-password",
        device_serial_number="serial-1",
        enable_websocket=False,
    )
    client._session = Mock()
    client._session.headers = {}
    client._access_token = "mock-access-token"
    client._refresh_token = "mock-refresh-token"
    client._access_expires_at = None
    return client


class RateLimitClientTests(unittest.TestCase):
    def test_retry_after_seconds_and_http_date_are_parsed(self):
        self.assertEqual(IquaSoftener._parse_retry_after("125"), 125.0)

        retry_at = datetime.now(timezone.utc) + timedelta(seconds=120)
        retry_after = IquaSoftener._parse_retry_after(
            format_datetime(retry_at, usegmt=True)
        )
        self.assertIsNotNone(retry_after)
        self.assertGreaterEqual(retry_after, 110)
        self.assertLessEqual(retry_after, 120)

    def test_exponential_backoff_has_a_60_second_floor_and_60_minute_cap(self):
        client = make_client()
        response = FakeResponse(429)

        with patch(
            "iqua_softener.iqua.random.uniform",
            side_effect=[0.8, 1.2, 1.2],
        ):
            first = client._record_rate_limit(response, "GET /devices")
            self.assertEqual(first.retry_after, 60)

            # Simulate the first cooldown expiring before the next 429.
            client._rate_limit_until = 0
            second = client._record_rate_limit(response, "GET /devices")
            self.assertEqual(second.retry_after, 144)

            client._rate_limit_until = 0
            client._rate_limit_count = 6
            capped = client._record_rate_limit(response, "GET /devices")
            self.assertEqual(capped.retry_after, 3600)

    def test_polling_request_is_skipped_during_shared_cooldown(self):
        client = make_client()
        client._session.request.return_value = FakeResponse(
            429, headers={"Retry-After": "180"}
        )

        with patch(
            "iqua_softener.iqua.random.uniform",
            return_value=1.0,
        ):
            with self.assertRaises(IquaRateLimitException) as first_error:
                client._request("GET", "/devices")
            self.assertEqual(first_error.exception.retry_after, 180)

            with self.assertRaises(IquaRateLimitException):
                client._request("GET", "/devices/device-1/detail-or-summary")

        client._session.request.assert_called_once()

    def test_concurrent_callers_share_one_rate_limit_response(self):
        client = make_client()
        client._session.request.return_value = FakeResponse(429)
        start_together = threading.Barrier(3)

        def make_request():
            start_together.wait()
            try:
                client._request("GET", "/devices")
            except IquaRateLimitException:
                return "rate-limited"
            return "unexpected-success"

        with patch(
            "iqua_softener.iqua.random.uniform",
            return_value=1.0,
        ):
            with ThreadPoolExecutor(max_workers=2) as executor:
                results = [executor.submit(make_request) for _ in range(2)]
                start_together.wait()
                self.assertEqual(
                    [future.result(timeout=2) for future in results],
                    ["rate-limited", "rate-limited"],
                )

        client._session.request.assert_called_once()

    def test_device_command_429_is_not_retried(self):
        client = make_client()
        client._session.request.return_value = FakeResponse(429)

        with patch(
            "iqua_softener.iqua.random.uniform",
            return_value=1.0,
        ):
            with self.assertRaises(IquaRateLimitException):
                client._request(
                    "PUT",
                    "/devices/device-1/command",
                    json={"function": "regenerate", "action": "schedule"},
                )

        client._session.request.assert_called_once()
        client._session.post.assert_not_called()

    def test_refresh_429_does_not_fall_back_to_login_or_send_api_request(self):
        client = make_client()
        client._access_expires_at = 0
        client._session.post.return_value = FakeResponse(429)

        with patch(
            "iqua_softener.iqua.random.uniform",
            return_value=1.0,
        ):
            with self.assertRaises(IquaRateLimitException):
                client._request("GET", "/devices")

        client._session.post.assert_called_once()
        self.assertIn("/auth/refresh", client._session.post.call_args.args[0])
        client._session.request.assert_not_called()

    def test_login_429_is_not_retried(self):
        client = make_client()
        client._access_token = None
        client._refresh_token = None
        client._session.post.return_value = FakeResponse(429)

        with patch(
            "iqua_softener.iqua.random.uniform",
            return_value=1.0,
        ):
            with self.assertRaises(IquaRateLimitException):
                client._request("GET", "/devices")

        client._session.post.assert_called_once()
        self.assertIn("/auth/login", client._session.post.call_args.args[0])
        client._session.request.assert_not_called()

    def test_401_followed_by_refresh_429_does_not_replay_command(self):
        client = make_client()
        client._session.request.return_value = FakeResponse(401)
        client._session.post.return_value = FakeResponse(429)

        with patch(
            "iqua_softener.iqua.random.uniform",
            return_value=1.0,
        ):
            with self.assertRaises(IquaRateLimitException):
                client._request(
                    "PUT",
                    "/devices/device-1/command",
                    json={"function": "regenerate", "action": "regenerate"},
                )

        client._session.request.assert_called_once()
        client._session.post.assert_called_once()

    def test_websocket_uri_rate_limit_is_not_swallowed(self):
        client = make_client()
        client._device_id = "device-1"
        client._request = Mock(
            side_effect=IquaRateLimitException("cooldown", retry_after=60)
        )

        with self.assertRaises(IquaRateLimitException):
            client.get_websocket_uri()


class WebSocketAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_uri_request_runs_off_the_websocket_event_loop(self):
        client = make_client()
        client._device_id = "device-1"
        request_started = threading.Event()
        release_request = threading.Event()

        def mocked_request(*args, **kwargs):
            request_started.set()
            release_request.wait(timeout=1)
            return FakeResponse(200, {"websocket_uri": "/ws/mock"})

        client._request = Mock(side_effect=mocked_request)
        uri_task = asyncio.create_task(client._get_websocket_uri())

        await asyncio.sleep(0.02)
        self.assertTrue(request_started.is_set())
        self.assertFalse(uri_task.done())

        release_request.set()
        self.assertEqual(await uri_task, "/ws/mock")
        client._request.assert_called_once_with("GET", "/devices/device-1/live")


if __name__ == "__main__":
    unittest.main()
