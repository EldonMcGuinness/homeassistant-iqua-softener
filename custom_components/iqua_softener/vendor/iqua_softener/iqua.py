import logging
import time
import json
import os
import random
import threading
import asyncio
from enum import Enum, IntEnum
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Optional, Dict, Any, Callable
from urllib.parse import urlsplit

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

import requests

try:
    import jwt  # optional (PyJWT)
except ImportError:
    jwt = None

try:
    import websockets
except ImportError:
    websockets = None

logger = logging.getLogger(__name__)


DEFAULT_API_BASE_URL = "https://api.myiquaapp.com/v1"
RATE_LIMIT_MIN_BACKOFF_SECONDS = 60
RATE_LIMIT_MAX_BACKOFF_SECONDS = 60 * 60


class IquaSoftenerState(str, Enum):
    ONLINE = "Online"
    OFFLINE = "Offline"


class IquaSoftenerVolumeUnit(IntEnum):
    GALLONS = 0
    LITERS = 1


class IquaSoftenerException(Exception):
    pass


class IquaRateLimitException(IquaSoftenerException):
    """Raised when the API is rate limited or a shared cooldown is active."""

    def __init__(self, message: str, retry_after: float):
        super().__init__(message)
        self.retry_after = max(0.0, retry_after)


@dataclass(frozen=True)
class IquaSoftenerData:
    timestamp: datetime
    model: str
    state: IquaSoftenerState
    device_date_time: datetime
    volume_unit: IquaSoftenerVolumeUnit
    current_water_flow: float
    today_use: int
    average_daily_use: int
    total_water_available: int
    days_since_last_regeneration: int
    salt_level: int
    salt_level_percent: int
    out_of_salt_estimated_days: int
    hardness_grains: int
    water_shutoff_valve_state: int


class IquaSoftener:
    def __init__(
        self,
        username: str,
        password: str,
        device_serial_number: Optional[str] = None,
        product_serial_number: Optional[str] = None,
        api_base_url: str = DEFAULT_API_BASE_URL,
        enable_websocket: bool = True,
        external_realtime_data: Optional[Dict[str, Any]] = None,
    ):
        self._username: str = username
        self._password: str = password
        self._device_serial_number = device_serial_number
        self._product_serial_number = product_serial_number
        self._api_base_url: str = api_base_url

        # requests.Session and the rate-limit state are shared with the
        # Home Assistant polling worker and the WebSocket worker thread.
        self._api_lock = threading.RLock()
        self._rate_limit_lock = threading.Lock()
        self._rate_limit_until = 0.0
        self._rate_limit_count = 0
        
        # Validate that at least one serial number is provided
        if not device_serial_number and not product_serial_number:
            raise ValueError("Either device_serial_number or product_serial_number must be provided")
        self._session: Optional[requests.Session] = None
        self._access_token: Optional[str] = None
        self._refresh_token: Optional[str] = None
        self._user_id: Optional[str] = None
        self._access_expires_at: Optional[int] = None
        self._device_id: Optional[str] = None  # Cache the device ID

        # WebSocket support
        self._enable_websocket = enable_websocket and websockets is not None
        self._websocket_uri: Optional[str] = None
        self._websocket_task: Optional[asyncio.Task] = None
        self._websocket_thread: Optional[threading.Thread] = None
        self._websocket_loop: Optional[asyncio.AbstractEventLoop] = None
        self._realtime_data: Dict[str, Any] = {}
        self._websocket_running = False
        self._websocket_lock = threading.Lock()
        self._websocket_connected_at: Optional[float] = None
        self._websocket_max_duration = 170  # Reconnect after 170 seconds (before 3 min timeout)

        # External real-time data (for Home Assistant integration)
        self._external_realtime_data = external_realtime_data

    @property
    def device_serial_number(self) -> Optional[str]:
        return self._device_serial_number
    
    @property
    def product_serial_number(self) -> Optional[str]:
        return self._product_serial_number

    @property
    def rate_limit_cooldown_remaining(self) -> float:
        """Return the remaining shared API cooldown in seconds."""
        with self._rate_limit_lock:
            return max(0.0, self._rate_limit_until - time.monotonic())

    def get_data(self) -> IquaSoftenerData:
        device_id = self._get_device_id()
        device = self._get_device_detail(device_id)
        props = device.get("properties", {})
        enriched = device.get("enriched_data", {}).get("water_treatment", {})

        def val(name: str, default=None):
            return props.get(name, {}).get("value", default)

        def enriched_val(name: str, default=None):
            """Get value from enriched_data."""
            return enriched.get(name, default)

        def realtime_val(name: str, fallback_name: str = None, default=None):
            """Get value from real-time data if available, otherwise fallback to API data."""
            realtime_value = self.get_realtime_property(name)
            if realtime_value is not None:
                return realtime_value
            if fallback_name:
                return val(fallback_name, default)
            return val(name, default)

        model_desc = val("model_description", "Unknown Model")
        model_id = val("model_id", "N/A")

        # Get device date from properties or use current time
        device_date_str = val("device_date")
        if device_date_str:
            try:
                # Parse the device date, assuming it's in ISO format
                device_date_time = datetime.fromisoformat(
                    device_date_str.rstrip("Z")
                ).replace(tzinfo=ZoneInfo("UTC"))
            except (ValueError, AttributeError):
                device_date_time = datetime.now(tz=ZoneInfo("UTC"))
        else:
            device_date_time = datetime.now(tz=ZoneInfo("UTC"))

        # Use real-time service_active if available
        service_active = realtime_val("service_active", "service_active", True)

        return IquaSoftenerData(
            timestamp=datetime.now(tz=ZoneInfo("UTC")),
            model=f"{model_desc} ({model_id})",
            state=(
                IquaSoftenerState.ONLINE
                if service_active
                else IquaSoftenerState.OFFLINE
            ),
            device_date_time=device_date_time,
            volume_unit=IquaSoftenerVolumeUnit(int(val("volume_unit_enum", 0))),
            # Use real-time current_water_flow if available
            current_water_flow=float(
                realtime_val("current_water_flow_gpm")
                or props.get("current_water_flow_gpm", {}).get("converted_value", 0.0)
            ),
            # Use enriched_data for today's usage if available
            today_use=int(
                enriched_val("gallons_used_today") or val("gallons_used_today", 0)
            ),
            average_daily_use=int(val("avg_daily_use_gals", 0)),
            # Use enriched_data for treated water available
            total_water_available=int(
                enriched_val("treated_water_available", {}).get("value")
                or val("treated_water_avail_gals", 0)
            ),
            # Use enriched_data for days since last regeneration
            days_since_last_regeneration=int(
                enriched_val("days_since_last_recharge")
                or val("days_since_last_regen", 0)
            ),
            salt_level=int(val("salt_level_tenths", 0) / 10),
            # Use enriched_data for salt level percent
            salt_level_percent=int(enriched_val("salt_level_percent") or 0),
            out_of_salt_estimated_days=int(val("out_of_salt_estimate_days", 0)),
            hardness_grains=int(val("hardness_grains", 0)),
            water_shutoff_valve_state=self._get_water_shutoff_valve_state(device),
        )

    def get_flow_and_salt(self) -> dict:
        """Return just flow (gpm) and salt level percent for quick dashboards."""
        # Try to get real-time flow first
        realtime_flow = self.get_realtime_property("current_water_flow_gpm")

        if realtime_flow is not None:
            flow = realtime_flow
        else:
            # Fallback to API data
            device_id = self._get_device_id()
            device = self._get_device_detail(device_id)
            props = device.get("properties", {})
            flow = props.get("current_water_flow_gpm", {}).get("converted_value", 0.0)

        # Salt level is typically not real-time, so get from API
        device_id = self._get_device_id()
        device = self._get_device_detail(device_id)
        salt = (
            device.get("enriched_data", {})
            .get("water_treatment", {})
            .get("salt_level_percent")
        )

        return {"flow_gpm": flow, "salt_percent": salt}

    def set_water_shutoff_valve(self, state: int):
        if state not in (0, 1):
            raise ValueError(
                "Invalid state for water shutoff valve (should be 0 or 1)."
            )

        device_id = self._get_device_id()
        url = f"/devices/{device_id}/command"

        # Convert state to action string: 1 = open, 0 = closed
        action = "open" if state == 1 else "close"
        payload = {"function": "water_shutoff_valve", "action": action}

        response = self._request("PUT", url, json=payload)
        if response.status_code != 200:
            raise IquaSoftenerException(
                f"Invalid status ({response.status_code}) for set water shutoff valve request"
            )
        response_data = response.json()
        return response_data

    def open_water_shutoff_valve(self):
        """Open the water shutoff valve (allow water flow) - state 1."""
        return self.set_water_shutoff_valve(1)

    def close_water_shutoff_valve(self):
        """Close the water shutoff valve (stop water flow) - state 0."""
        return self.set_water_shutoff_valve(0)

    def schedule_regeneration(self):
        """Schedule a regeneration cycle for the water softener."""
        device_id = self._get_device_id()
        url = f"/devices/{device_id}/command"
        payload = {"function": "regenerate", "action": "schedule"}

        response = self._request("PUT", url, json=payload)
        if response.status_code != 200:
            raise IquaSoftenerException(
                f"Invalid status ({response.status_code}) for schedule regeneration request"
            )
        response_data = response.json()
        return response_data

    def cancel_scheduled_regeneration(self):
        """Cancel a scheduled regeneration cycle."""
        device_id = self._get_device_id()
        url = f"/devices/{device_id}/command"
        payload = {"function": "regenerate", "action": "cancel"}

        response = self._request("PUT", url, json=payload)
        if response.status_code != 200:
            raise IquaSoftenerException(
                f"Invalid status ({response.status_code}) for cancel regeneration request"
            )
        response_data = response.json()
        return response_data

    def regenerate_now(self):
        """Start a regeneration cycle immediately."""
        device_id = self._get_device_id()
        url = f"/devices/{device_id}/command"
        payload = {"function": "regenerate", "action": "regenerate"}

        response = self._request("PUT", url, json=payload)
        if response.status_code != 200:
            raise IquaSoftenerException(
                f"Invalid status ({response.status_code}) for regenerate now request"
            )
        response_data = response.json()
        return response_data

    def get_devices(self) -> list:
        """Get list of all devices for the authenticated user."""
        return self._get_devices()

    def get_device_id(self) -> str:
        """Get the device ID for the configured serial number."""
        return self._get_device_id()

    def get_device_details(self) -> dict:
        """Get detailed device information for the configured device."""
        device_id = self._get_device_id()
        return self._get_device_detail(device_id)

    def has_water_shutoff_valve(self) -> bool:
        """Check if the device has a water shutoff valve installed."""
        try:
            device_id = self._get_device_id()
            device = self._get_device_detail(device_id)
            
            # Check for water_shutoff_valve in multiple locations
            valve_data = None
            
            # Check enriched_data first
            enriched = device.get("enriched_data", {}).get("water_treatment", {})
            valve_data = enriched.get("water_shutoff_valve", {})
            
            # If not in enriched_data, check properties
            if not valve_data:
                props = device.get("properties", {})
                valve_data = props.get("water_shutoff_valve", {})
            
            # If still not found, check device root level  
            if not valve_data:
                valve_data = device.get("water_shutoff_valve", {})
            
            # Check if valve is installed
            if isinstance(valve_data, dict):
                return valve_data.get("is_installed", False)
            
            return False
        except IquaRateLimitException:
            raise
        except Exception as e:
            logger.error(f"Error checking water shutoff valve availability: {e}")
            return False

    def start_websocket(self):
        """Start WebSocket connection for real-time updates."""
        if not self._enable_websocket:
            logger.warning(
                "WebSocket support is disabled or websockets library not available"
            )
            return

        if self._websocket_running:
            logger.info("WebSocket already running")
            return

        self._websocket_running = True
        self._websocket_thread = threading.Thread(
            target=self._run_websocket_thread, daemon=True
        )
        self._websocket_thread.start()

    def stop_websocket(self):
        """Stop WebSocket connection."""
        if not self._websocket_running:
            return

        self._websocket_running = False

        if self._websocket_loop and self._websocket_task:
            self._websocket_loop.call_soon_threadsafe(self._websocket_task.cancel)

        if self._websocket_thread:
            self._websocket_thread.join(timeout=5)

    def get_realtime_property(self, property_name: str) -> Optional[Any]:
        """Get a real-time property value from WebSocket data."""
        # Check external real-time data first (for Home Assistant integration)
        if self._external_realtime_data:
            prop_data = self._external_realtime_data.get(property_name)
            if prop_data:
                if prop_data.get("converted_property"):
                    return prop_data["converted_property"]["value"]
                return prop_data.get("value")

        # Fall back to internal WebSocket data
        with self._websocket_lock:
            prop_data = self._realtime_data.get(property_name)
            if prop_data:
                # Return converted_value if available, otherwise raw value
                if prop_data.get("converted_property"):
                    return prop_data["converted_property"]["value"]
                return prop_data.get("value")
            return None

    def update_external_realtime_data(self, realtime_data: Dict[str, Any]):
        """Update external real-time data (for Home Assistant integration)."""
        self._external_realtime_data = realtime_data

    def get_websocket_uri(self) -> Optional[str]:
        """Get WebSocket URI for external use (like Home Assistant integration)."""
        ws_uri = self._fetch_websocket_uri()
        if ws_uri:
            return (
                ws_uri
                if ws_uri.startswith(("ws://", "wss://"))
                else f"wss://api.myiquaapp.com{ws_uri}"
            )
        return None

    def _fetch_websocket_uri(self) -> Optional[str]:
        """Fetch the WebSocket URI without hiding API errors."""
        device_id = self._get_device_id()
        response = self._request("GET", f"/devices/{device_id}/live")
        return response.json().get("websocket_uri")

    def _run_websocket_thread(self):
        """Run WebSocket client in a separate thread."""
        try:
            self._websocket_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._websocket_loop)
            self._websocket_loop.run_until_complete(self._websocket_client())
        except Exception as e:
            logger.error(f"WebSocket thread error: {e}")
        finally:
            if self._websocket_loop:
                self._websocket_loop.close()

    async def _websocket_client(self):
        """WebSocket client coroutine."""
        self._websocket_task = asyncio.current_task()
        while self._websocket_running:
            try:
                # Get WebSocket URI
                ws_uri = await self._get_websocket_uri()
            except IquaRateLimitException as err:
                delay = max(err.retry_after, self.rate_limit_cooldown_remaining)
                logger.warning(
                    "iQua API rate limited the WebSocket URI request; retrying in %.1f seconds",
                    delay,
                )
                await asyncio.sleep(delay)
                continue
            except Exception as err:
                logger.warning(
                    "Failed to retrieve WebSocket URI; retrying in 30 seconds: %s",
                    err,
                )
                await asyncio.sleep(30)
                continue

            if not ws_uri:
                logger.debug("iQua API returned no WebSocket URI; retrying in 30 seconds")
                await asyncio.sleep(30)
                continue

            full_uri = (
                ws_uri
                if ws_uri.startswith(("ws://", "wss://"))
                else f"wss://api.myiquaapp.com{ws_uri}"
            )
            # WebSocket URIs can contain access credentials in the query string.
            safe_uri = urlsplit(full_uri)._replace(query="", fragment="").geturl()
            logger.info("Connecting to WebSocket endpoint: %s", safe_uri)

            try:
                async with websockets.connect(full_uri) as websocket:
                    logger.info("WebSocket connected successfully")
                    self._websocket_connected_at = time.time()

                    async for message in websocket:
                        if not self._websocket_running:
                            break

                        # Check if we've been connected too long (proactive reconnect)
                        if self._websocket_connected_at:
                            connection_duration = time.time() - self._websocket_connected_at
                            if connection_duration >= self._websocket_max_duration:
                                logger.info(
                                    "WebSocket connection duration (%.1fs) exceeded max (%ss); reconnecting",
                                    connection_duration,
                                    self._websocket_max_duration,
                                )
                                break

                        try:
                            data = json.loads(message)
                            await self._handle_websocket_message(data)
                        except json.JSONDecodeError as err:
                            logger.warning("Failed to parse WebSocket message: %s", err)
                        except Exception as err:
                            logger.error("Error handling WebSocket message: %s", err)
            except asyncio.CancelledError:
                raise
            except Exception as err:
                logger.warning("WebSocket connection failed: %s", err)
                self._websocket_connected_at = None
                if self._websocket_running:
                    logger.debug("Retrying WebSocket connection in 10 seconds")
                    await asyncio.sleep(10)

    async def _get_websocket_uri(self) -> Optional[str]:
        """Get WebSocket URI without blocking the WebSocket event loop."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._fetch_websocket_uri)

    async def _handle_websocket_message(self, data: Dict[str, Any]):
        """Handle incoming WebSocket message."""
        if data.get("type") == "property" and "name" in data:
            property_name = data["name"]
            with self._websocket_lock:
                self._realtime_data[property_name] = data
            logger.debug(
                f"Updated real-time property: {property_name} = {data.get('value')}"
            )

    def save_tokens(self, path: str):
        """Save authentication tokens to a file."""
        with open(path, "w") as f:
            json.dump(
                {
                    "access_token": self._access_token,
                    "refresh_token": self._refresh_token,
                    "user_id": self._user_id,
                    "_access_expires_at": self._access_expires_at,
                },
                f,
            )

    def load_tokens(self, path: str):
        """Load authentication tokens from a file."""
        if not os.path.exists(path):
            return
        with open(path, "r") as f:
            data = json.load(f)
        self._access_token = data.get("access_token")
        self._refresh_token = data.get("refresh_token")
        self._user_id = data.get("user_id")
        self._access_expires_at = data.get("_access_expires_at")

    def _get_water_shutoff_valve_state(self, device: dict) -> int:
        """Parse water shutoff valve state from API device data."""
        # Check enriched_data first (this is where it should be)
        enriched = device.get("enriched_data", {}).get("water_treatment", {})
        valve_data = enriched.get("water_shutoff_valve", {})
        # If not in enriched_data, check properties as fallback
        if not valve_data:
            props = device.get("properties", {})
            valve_data = props.get("water_shutoff_valve", {})

        # If still not found, check device root level
        if not valve_data:
            valve_data = device.get("water_shutoff_valve", {})

        if isinstance(valve_data, dict):
            # Check if valve is installed first
            is_installed = valve_data.get("is_installed", False)
            if not is_installed:
                return 0  # Default to closed if not installed
                
            status = valve_data.get("status", "closed")
            # Convert status string to int: "open" = 1, "closed" = 0
            return 1 if status == "open" else 0
        # Fallback for legacy numeric format
        return int(valve_data) if valve_data is not None else 0

    def _get_device_id(self) -> str:
        """Get the device ID for the configured serial number."""
        with self._api_lock:
            if self._device_id is not None:
                return self._device_id

            # Get all devices and find the one with matching serial number.
            devices = self._get_devices()
            for device in devices:
                props = device.get("properties", {})

                if self._device_serial_number:
                    device_serial = props.get("serial_number", {}).get("value")
                    if device_serial == self._device_serial_number:
                        self._device_id = device["id"]
                        return self._device_id

                if self._product_serial_number:
                    product_serial = props.get("product_serial_number", {}).get("value")
                    if product_serial == self._product_serial_number:
                        self._device_id = device["id"]
                        return self._device_id

            if self._device_serial_number and self._product_serial_number:
                identifier = (
                    f"device serial number '{self._device_serial_number}' or "
                    f"product serial number '{self._product_serial_number}'"
                )
            elif self._device_serial_number:
                identifier = f"device serial number '{self._device_serial_number}'"
            else:
                identifier = f"product serial number '{self._product_serial_number}'"

            raise IquaSoftenerException(f"Device with {identifier} not found")

    def _get_devices(self) -> list:
        """Get list of all devices for the authenticated user."""
        r = self._request("GET", "/devices")
        data = r.json()
        return data.get("data", [])

    def _ensure_session(self):
        """Ensure we have a session object."""
        if self._session is None:
            self._session = requests.Session()

    @staticmethod
    def _parse_retry_after(value: Optional[str]) -> Optional[float]:
        """Parse Retry-After as seconds or an HTTP date."""
        if value is None:
            return None

        try:
            return max(0.0, float(value.strip()))
        except (AttributeError, TypeError, ValueError):
            pass

        try:
            retry_at = parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return None

        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        return max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds())

    def _raise_if_rate_limited(self) -> None:
        """Fail fast while the shared API cooldown is active."""
        remaining = self.rate_limit_cooldown_remaining
        if remaining > 0:
            raise IquaRateLimitException(
                f"iQua API cooldown is active; request was skipped "
                f"(retry in {remaining:.1f} seconds)",
                retry_after=remaining,
            )

    def _record_rate_limit(
        self, response: requests.Response, operation: str
    ) -> IquaRateLimitException:
        """Start a shared cooldown after an HTTP 429 response."""
        with self._rate_limit_lock:
            now = time.monotonic()
            headers = getattr(response, "headers", {}) or {}
            retry_after = self._parse_retry_after(headers.get("Retry-After"))

            exponent = min(self._rate_limit_count, 6)
            base_delay = min(
                RATE_LIMIT_MIN_BACKOFF_SECONDS * (2**exponent),
                RATE_LIMIT_MAX_BACKOFF_SECONDS,
            )
            backoff_delay = min(
                RATE_LIMIT_MAX_BACKOFF_SECONDS,
                max(
                    RATE_LIMIT_MIN_BACKOFF_SECONDS,
                    base_delay * random.uniform(0.8, 1.2),
                ),
            )
            delay = max(backoff_delay, retry_after or 0.0)
            self._rate_limit_until = max(self._rate_limit_until, now + delay)
            self._rate_limit_count += 1
            remaining = max(0.0, self._rate_limit_until - now)

            logger.warning(
                "iQua API rate limited %s; pausing requests for %.1f seconds",
                operation,
                remaining,
            )
            return IquaRateLimitException(
                f"iQua API rate limited {operation}; retry in {remaining:.1f} seconds",
                retry_after=remaining,
            )

    def _record_success(self) -> None:
        """Reset exponential backoff after an API request succeeds."""
        with self._rate_limit_lock:
            self._rate_limit_count = 0
            self._rate_limit_until = 0.0

    def _set_tokens(self, access_token: str, refresh_token: Optional[str]):
        """Set authentication tokens and update session headers."""
        self._access_token = access_token
        self._refresh_token = refresh_token
        if jwt:
            try:
                decoded = jwt.decode(access_token, options={"verify_signature": False})
                exp = decoded.get("exp")
                if exp:
                    self._access_expires_at = int(exp) - 60
            except Exception:
                self._access_expires_at = None

        self._ensure_session()
        if self._access_token:
            self._session.headers.update(
                {"Authorization": f"Bearer {self._access_token}"}
            )

    def _is_token_expired(self) -> bool:
        """Check if the current access token is expired."""
        if not self._access_token:
            return True
        if self._access_expires_at is None:
            return False
        return time.time() >= self._access_expires_at

    def _login(self) -> Dict[str, Any]:
        """Authenticate with the API and get tokens."""
        with self._api_lock:
            self._raise_if_rate_limited()
            self._ensure_session()
            url = f"{self._api_base_url}/auth/login"
            payload = {"email": self._username, "password": self._password}
            try:
                r = self._session.post(url, json=payload, timeout=15)
            except requests.exceptions.RequestException as ex:
                raise IquaSoftenerException(f"Exception on login request ({ex})")

            if r.status_code == 429:
                raise self._record_rate_limit(r, "POST /auth/login")
            if r.status_code == 401:
                raise IquaSoftenerException(f"Authentication error ({r.text})")
            if r.status_code != 200:
                raise IquaSoftenerException(f"Login failed: {r.status_code} {r.text}")

            self._record_success()
            data = r.json()
            self._set_tokens(data.get("access_token"), data.get("refresh_token"))
            self._user_id = data.get("user_id")
            return data

    def _refresh_access_token(self) -> Dict[str, Any]:
        """Refresh the access token using the refresh token."""
        with self._api_lock:
            if not self._refresh_token:
                raise IquaSoftenerException("No refresh token available")

            self._raise_if_rate_limited()
            self._ensure_session()
            url = f"{self._api_base_url}/auth/refresh"
            payload = {"refresh_token": self._refresh_token}
            try:
                r = self._session.post(url, json=payload, timeout=15)
            except requests.exceptions.RequestException as ex:
                raise IquaSoftenerException(f"Exception on token refresh ({ex})")

            if r.status_code == 429:
                raise self._record_rate_limit(r, "POST /auth/refresh")
            if r.status_code != 200:
                raise IquaSoftenerException(f"Refresh failed: {r.status_code} {r.text}")

            self._record_success()
            data = r.json()
            self._set_tokens(data.get("access_token"), data.get("refresh_token"))
            return data

    def _ensure_authenticated(self):
        """Ensure we have a valid authentication token."""
        with self._api_lock:
            self._raise_if_rate_limited()
            if not self._is_token_expired():
                return

            if not self._refresh_token:
                self._login()
                return

            try:
                self._refresh_access_token()
            except IquaRateLimitException:
                raise
            except IquaSoftenerException as err:
                logger.warning("Token refresh failed; falling back to login: %s", err)
                self._login()

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        """Make an authenticated request to the API."""
        with self._api_lock:
            self._raise_if_rate_limited()
            self._ensure_authenticated()
            self._ensure_session()

            url = (
                path
                if path.startswith("http")
                else f"{self._api_base_url.rstrip('/')}/{path.lstrip('/')}"
            )
            operation = f"{method.upper()} {urlsplit(url).path}"

            def send_request() -> requests.Response:
                response = self._session.request(method, url, timeout=20, **kwargs)
                if response.status_code == 429:
                    raise self._record_rate_limit(response, operation)
                return response

            response = send_request()
            if response.status_code == 401 and self._refresh_token:
                try:
                    self._refresh_access_token()
                except IquaRateLimitException:
                    # A throttled refresh must not trigger a login or replay the
                    # original request (especially for device commands).
                    raise
                except IquaSoftenerException as err:
                    logger.warning(
                        "Token refresh after an unauthorized API response failed; "
                        "falling back to login: %s",
                        err,
                    )
                    self._login()
                response = send_request()

            if response.status_code == 200:
                self._record_success()
            else:
                response.raise_for_status()
            return response

    def _get_device_detail(self, device_id: str) -> dict:
        """Get detailed device information."""
        r = self._request("GET", f"/devices/{device_id}/detail-or-summary")
        data = r.json()
        return data.get("device", {})
