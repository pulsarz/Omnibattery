"""Zendure MQTT driver (Hyper 2000).

The Hyper 2000 predates Zendure's local HTTP API (zenSDK): it only speaks MQTT.
Out of the box it talks to Zendure's cloud broker; once redirected to a local
broker (over Bluetooth, e.g. with solarflow-bt-manager or Zendure-HA's local MQTT
option) it publishes and listens on the same topics there:

  device → broker  ``/{productKey}/{deviceId}/properties/report``  (JSON deltas)
  device → broker  ``/{productKey}/{deviceId}/time-sync``          (clock request)
  broker → device  ``iot/{productKey}/{deviceId}/properties/read``  ({"properties": ["getAll"]})
  broker → device  ``iot/{productKey}/{deviceId}/properties/write`` ({"properties": {...}})
  broker → device  ``iot/{productKey}/{deviceId}/function/invoke``  (deviceAutomation)

Two ways to reach that broker, chosen per battery:

* **Home Assistant's MQTT integration** (no credentials stored here), when HA is
  already connected to the broker the Hyper uses.
* **A broker of its own** (host / port / user / password), for a Mosquitto that
  HA's MQTT integration is not pointed at. Uses paho-mqtt, which ships with HA.

Telemetry is pushed. Reports are deltas: the device only sends what changed and
leaves zero-valued powers out of its full dump, so the driver merges every
report into a cache, asks for a full dump (``getAll``) on a fixed cadence, and
treats a power that has not been reported for a few dumps as 0.

Control. Two wire formats, selected per battery:

* ``automation`` (default): the ``deviceAutomation`` function with
  ``autoModel=8`` — the command the Zendure app's smart-matching mode sends. It
  is held in RAM, so per-cycle setpoints do not wear the flash. This is what
  Zendure's own Home Assistant integration uses for the Hyper 2000.
* ``properties``: ``acMode`` + ``inputLimit``/``outputLimit`` with
  ``smartMode=1``, the same mapping as the local HTTP driver, for firmware that
  ignores automation commands.

Either way Zendure's own HEMS / smart CT mode must be off, or it fights the
setpoints. Sign convention as everywhere: net power +charge / -discharge.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import re
import time
import uuid
from abc import ABC, abstractmethod
from typing import Any, Callable, Optional

from homeassistant.components import mqtt
from homeassistant.core import HomeAssistant, callback

from .base import (
    DELIVERED_AC_POWER_KEY,
    BatteryDriver,
    DriverCapabilities,
    ReadGroup,
    SetpointResult,
    TelemetrySnapshot,
)
from .zendure import (
    NUMBER_DEFINITIONS as _HTTP_NUMBER_DEFINITIONS,
    PACK_FIELD_SPECS,
    SENSOR_DEFINITIONS as _HTTP_SENSOR_DEFINITIONS,
    ZENDURE_MODEL_HYPER_2000,
    _AC_MODE_CHARGE,
    _AC_MODE_DISCHARGE,
    _DECIPERCENT_KEYS,
    _KEY_TO_PROP,
    _PACK_KEY_RE,
    _PROP_TO_KEY,
    _REMAIN_TIME_SENTINEL,
    zendure_power_limits,
)

_LOGGER = logging.getLogger(__name__)

# Product keys the Hyper 2000 registers under (two hardware generations). Any
# other key is still accepted when the device answers, since the topic layout is
# shared by Zendure's legacy MQTT products.
HYPER_2000_PRODUCT_KEYS: tuple[str, ...] = ("ja72U0ha", "gDa3tb")

# Shape of Zendure product keys and device ids, used to tell a Zendure device
# apart from anything else publishing under a leading-slash topic.
_ZENDURE_KEY_RE = re.compile(r"[A-Za-z0-9]{6,8}")
_ZENDURE_ID_RE = re.compile(r"[A-Za-z0-9]{6,12}")

CONTROL_AUTOMATION = "automation"
CONTROL_PROPERTIES = "properties"
CONTROL_METHODS: tuple[str, ...] = (CONTROL_AUTOMATION, CONTROL_PROPERTIES)

BROKER_HOME_ASSISTANT = "homeassistant"
BROKER_CUSTOM = "custom"
BROKER_CHOICES: tuple[str, ...] = (BROKER_HOME_ASSISTANT, BROKER_CUSTOM)
DEFAULT_MQTT_PORT = 1883

# A full dump is requested this often. It doubles as the liveness signal: the
# device answers getAll even when nothing has changed.
_REFRESH_INTERVAL_S = 30.0
# Re-send the active setpoint this often even if the controller has not changed
# it, so a device that drops back to its own program is pulled back in.
_COMMAND_KEEPALIVE_S = 60.0
# Smallest power the Hyper reliably acts on, the start power Zendure's own
# integration uses. Requests between 1 W and this are rounded up rather than
# sent: a smaller one can leave the device idle, which the controller then
# reads as a battery refusing to deliver and excludes for minutes.
_MIN_POWER_W = 50
# Kickstart, also after Zendure's integration: a small request that has not
# produced any power this long after the start is re-sent at _KICK_POWER_W
# until power flows (at most _KICK_HOLD_S), then the request is restored.
_KICK_POWER_W = 100
_KICK_AFTER_S = 10.0
_KICK_HOLD_S = 15.0
# Power in the commanded direction that counts as "running".
_RUNNING_W = 20
# A power value missing from more than two consecutive dumps is zero: the device
# omits zero-valued powers from getAll rather than reporting 0.
_POWER_ZERO_AFTER_S = 2.5 * _REFRESH_INTERVAL_S
# No message at all for this long means the device (or the broker path) is gone.
_STALE_AFTER_S = 4 * _REFRESH_INTERVAL_S
# How long connect()/probe() wait for the first answer to getAll.
_FIRST_REPORT_TIMEOUT_S = 10.0
_BROKER_CONNECT_TIMEOUT_S = 10.0
# How long probe() waits for pack data after the first property report.
_PACK_WAIT_S = 3.0

# Powers the device leaves out when they are zero; they default to 0.
_POWER_PROPS: tuple[str, ...] = (
    "packInputPower",
    "outputPackPower",
    "outputHomePower",
    "gridInputPower",
    "solarInputPower",
    "solarPower1",
    "solarPower2",
)
# Mutually exclusive directions: a positive reading for one means the other is 0
# now, even if the delta that said so did not mention it.
_EXCLUSIVE_POWER_PAIRS: dict[str, str] = {
    "packInputPower": "outputPackPower",
    "outputPackPower": "packInputPower",
    "outputHomePower": "gridInputPower",
    "gridInputPower": "outputHomePower",
}

# Configuration the user may change from Home Assistant.
_WRITABLE_KEYS = frozenset({"soc_set", "min_soc", "inverse_max_power"})

# Telemetry keys that are not entities but that the control layer reads.
_COMMANDED_KEY = "commanded_net_power"


# ---------------------------------------------------------------------------
# Entity definitions
# ---------------------------------------------------------------------------
# Same logical keys as the HTTP driver (shared translations and dashboard
# cards), restricted to what the Hyper 2000 reports. Values whose presence on
# this firmware generation is not certain start disabled.

_HYPER_SENSOR_KEYS: tuple[str, ...] = (
    "battery_soc",
    "solar_power",
    "battery_power",
    "internal_temperature",
    "remain_discharge_time",
    "fault_level",
    "pack_count",
    "output_limit",
    "input_limit",
    "ac_mode",
    "mppt1_power",
    "mppt2_power",
    "battery_voltage",
    "max_cell_voltage",
    "min_cell_voltage",
    "wifi_signal_strength",
)
_DISABLED_BY_DEFAULT = frozenset({"battery_voltage", "wifi_signal_strength"})


def _hyper_sensor_definitions() -> list[dict]:
    by_key = {d["key"]: d for d in _HTTP_SENSOR_DEFINITIONS}
    definitions = []
    for key in _HYPER_SENSOR_KEYS:
        definition = dict(by_key[key])
        if key == "internal_temperature":
            # The MQTT report carries deci-Kelvin; the driver converts to °C
            # itself, so the coordinator must not scale it again.
            definition.update({"scale": 1, "precision": 1})
        if key in _DISABLED_BY_DEFAULT:
            definition["enabled_by_default"] = False
        definitions.append(definition)
    return definitions


SENSOR_DEFINITIONS: list[dict] = _hyper_sensor_definitions() + [
    # Derived from the packs' serials (see zendure_pack_model): the Hyper itself
    # reports no capacity, and the stack can grow or shrink.
    {"key": "battery_total_energy", "name": "Battery Total Energy", "unit": "kWh",
     "device_class": "energy", "state_class": "total", "scale": 1, "precision": 2,
     "scan_interval": "low", "enabled_by_default": True},
]


def _hyper_number_definitions(max_discharge_w: int) -> list[dict]:
    return [
        {**d, "max": max_discharge_w} if d["key"] == "inverse_max_power" else dict(d)
        for d in _HTTP_NUMBER_DEFINITIONS
    ]


# ---------------------------------------------------------------------------
# Transports
# ---------------------------------------------------------------------------

MessageCallback = Callable[[str, Any], None]


class ZendureMqttTransport(ABC):
    """Minimal pub/sub surface the driver needs from a broker connection."""

    @abstractmethod
    async def start(self) -> bool:
        """Make the broker usable. Return False when it cannot be reached."""

    @abstractmethod
    async def stop(self) -> None:
        """Release the connection. Safe to call more than once."""

    @abstractmethod
    async def subscribe(self, topic: str, handler: MessageCallback) -> Callable[[], Any]:
        """Subscribe; ``handler(topic, payload)`` runs on the event loop."""

    @abstractmethod
    async def publish(self, topic: str, payload: str) -> None:
        """Publish at QoS 0, not retained. Raises when the broker is unusable."""


class HomeAssistantMqttTransport(ZendureMqttTransport):
    """Use the broker Home Assistant's MQTT integration is connected to."""

    def __init__(self, hass: HomeAssistant, *, wait_timeout: float = _BROKER_CONNECT_TIMEOUT_S) -> None:
        self.hass = hass
        self._wait_timeout = wait_timeout

    async def start(self) -> bool:
        wait = getattr(mqtt, "async_wait_for_mqtt_client", None)
        if wait is None:
            return True
        try:
            return bool(await asyncio.wait_for(wait(self.hass), self._wait_timeout))
        except (asyncio.TimeoutError, TimeoutError):
            return False
        except Exception as err:  # MQTT not set up, or not loaded yet
            _LOGGER.debug("Home Assistant MQTT is not available: %s", err)
            return False

    async def stop(self) -> None:
        return None

    async def subscribe(self, topic: str, handler: MessageCallback) -> Callable[[], Any]:
        @callback
        def _on_message(message) -> None:
            handler(getattr(message, "topic", topic), getattr(message, "payload", message))

        return await mqtt.async_subscribe(self.hass, topic, _on_message, qos=0)

    async def publish(self, topic: str, payload: str) -> None:
        await mqtt.async_publish(self.hass, topic, payload, qos=0, retain=False)


def _paho_reason_ok(reason: Any) -> bool:
    """True for a successful paho connect result (v1 int or v2 ReasonCode)."""
    is_failure = getattr(reason, "is_failure", None)
    if is_failure is not None:
        return not bool(is_failure)
    try:
        return int(reason) == 0
    except (TypeError, ValueError):
        return False


class PahoMqttTransport(ZendureMqttTransport):
    """A direct connection to a broker that HA's MQTT integration does not use.

    paho runs its network loop in its own thread; every callback is handed back
    to the event loop before it touches driver state. Subscriptions are replayed
    on each (re)connect, so a broker restart is survived without a reload.
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        host: str,
        port: int = DEFAULT_MQTT_PORT,
        *,
        username: str | None = None,
        password: str | None = None,
        client_id: str | None = None,
        connect_timeout: float = _BROKER_CONNECT_TIMEOUT_S,
        client_factory: Callable[[str], Any] | None = None,
    ) -> None:
        self._loop = loop
        self._host = host
        self._port = int(port or DEFAULT_MQTT_PORT)
        self._username = username or None
        self._password = password or None
        self._client_id = client_id or f"omnibattery-zendure-{uuid.uuid4().hex[:8]}"
        self._connect_timeout = connect_timeout
        self._client_factory = client_factory
        self._client: Any = None
        self._connected = False
        self._connected_event: asyncio.Event | None = None
        self._handlers: dict[str, list[MessageCallback]] = {}
        self.last_error: str | None = None

    @staticmethod
    def _default_client(client_id: str) -> Any:
        import paho.mqtt.client as paho  # bundled with Home Assistant

        try:
            from paho.mqtt.enums import CallbackAPIVersion
        except ImportError:  # paho-mqtt 1.x
            return paho.Client(client_id=client_id)
        return paho.Client(CallbackAPIVersion.VERSION2, client_id=client_id)

    @property
    def connected(self) -> bool:
        return self._connected

    async def start(self) -> bool:
        if self._client is not None and self._connected:
            return True
        try:
            if self._client_factory is not None:
                client = self._client_factory(self._client_id)
            else:
                # Importing paho and building its client touches the filesystem
                # (module import, socket setup); keep that off the event loop.
                client = await self._loop.run_in_executor(
                    None, self._default_client, self._client_id
                )
        except Exception as err:
            self.last_error = f"paho-mqtt unavailable: {err}"
            _LOGGER.warning("Zendure MQTT: %s", self.last_error)
            return False
        if self._username:
            client.username_pw_set(self._username, self._password)
        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        try:
            client.reconnect_delay_set(min_delay=1, max_delay=30)
        except Exception:
            pass
        self._client = client
        self._connected_event = asyncio.Event()
        try:
            client.connect_async(self._host, self._port, keepalive=30)
            client.loop_start()
        except Exception as err:
            self.last_error = str(err)
            _LOGGER.warning(
                "Zendure MQTT: cannot start a connection to %s:%s: %s",
                self._host, self._port, err,
            )
            await self.stop()
            return False
        try:
            await asyncio.wait_for(self._connected_event.wait(), self._connect_timeout)
        except (asyncio.TimeoutError, TimeoutError):
            if not self.last_error:
                self.last_error = "timeout"
            _LOGGER.warning(
                "Zendure MQTT: broker %s:%s did not accept a connection (%s)",
                self._host, self._port, self.last_error,
            )
            await self.stop()
            return False
        return True

    async def stop(self) -> None:
        client, self._client = self._client, None
        self._connected = False
        if client is None:
            return

        def _shutdown() -> None:
            try:
                client.disconnect()
            except Exception:
                pass
            try:
                client.loop_stop()
            except Exception:
                pass

        # loop_stop joins paho's network thread; keep that off the event loop.
        await self._loop.run_in_executor(None, _shutdown)

    # paho thread ------------------------------------------------------------

    def _on_connect(self, client, userdata, flags, reason, properties=None) -> None:
        if not _paho_reason_ok(reason):
            self.last_error = f"connection refused: {reason}"
            return
        self.last_error = None
        self._connected = True
        for topic in list(self._handlers):
            client.subscribe(topic, 0)
        event = self._connected_event
        if event is not None:
            self._loop.call_soon_threadsafe(event.set)

    def _on_disconnect(self, client, *args) -> None:
        self._connected = False

    def _dispatcher(self, topic_filter: str) -> Callable[[Any, Any, Any], None]:
        def _on_message(client, userdata, message) -> None:
            for handler in list(self._handlers.get(topic_filter, ())):
                self._loop.call_soon_threadsafe(handler, message.topic, message.payload)

        return _on_message

    # event loop -------------------------------------------------------------

    async def subscribe(self, topic: str, handler: MessageCallback) -> Callable[[], Any]:
        if self._client is None:
            raise ConnectionError("MQTT broker connection not started")
        handlers = self._handlers.setdefault(topic, [])
        first = not handlers
        handlers.append(handler)
        if first:
            self._client.message_callback_add(topic, self._dispatcher(topic))
            if self._connected:
                self._client.subscribe(topic, 0)

        def _unsubscribe() -> None:
            current = self._handlers.get(topic)
            if not current or handler not in current:
                return
            current.remove(handler)
            if current:
                return
            del self._handlers[topic]
            client = self._client
            if client is None:
                return
            try:
                client.message_callback_remove(topic)
                client.unsubscribe(topic)
            except Exception:
                pass

        return _unsubscribe

    async def publish(self, topic: str, payload: str) -> None:
        client = self._client
        if client is None or not self._connected:
            raise ConnectionError("MQTT broker not connected")
        info = client.publish(topic, payload, qos=0, retain=False)
        rc = getattr(info, "rc", 0)
        if rc not in (0, None):
            raise ConnectionError(f"MQTT publish failed (rc={rc})")


def build_transport(
    hass: HomeAssistant,
    broker: str = BROKER_HOME_ASSISTANT,
    *,
    host: str | None = None,
    port: int | None = None,
    username: str | None = None,
    password: str | None = None,
) -> ZendureMqttTransport:
    """Return the transport a battery's configuration asks for."""
    if broker == BROKER_CUSTOM:
        return PahoMqttTransport(
            hass.loop,
            (host or "").strip(),
            int(port or DEFAULT_MQTT_PORT),
            username=username,
            password=password,
        )
    return HomeAssistantMqttTransport(hass)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _decode(payload: Any) -> dict | None:
    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode("utf-8", "replace")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except ValueError:
            return None
    return payload if isinstance(payload, dict) else None


def _number(value: Any) -> float | int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value if math.isfinite(value) else None
    if isinstance(value, str):
        try:
            parsed = float(value.strip())
        except ValueError:
            return None
        return parsed if math.isfinite(parsed) else None
    return None


def _decikelvin_to_celsius(value: Any) -> float | None:
    raw = _number(value)
    if raw is None:
        return None
    return round((raw - 2731) / 10, 1)


def _split_topic(topic: str) -> tuple[str, str, str] | None:
    """``/pk/id/a/b`` → (pk, id, "a/b"); None for anything else."""
    parts = topic.split("/")
    if len(parts) < 4 or parts[0] != "":
        return None
    return parts[1], parts[2], "/".join(parts[3:])


def _report_topic(product_key: str | None, device_id: str) -> str:
    return f"/{product_key or '+'}/{device_id}/#"


def zendure_pack_model(sn: Any, pack_type: Any = None) -> tuple[str, float]:
    """Return ``(model, nominal kWh)`` for a Zendure battery pack.

    Zendure reports no capacity, but the pack serial identifies the model: its
    first letter is the family and, for the AB2000 line, the fourth letter the
    variant. Same table as Zendure's own Home Assistant integration. Unknown
    serials return ``("Unknown", 0.0)`` so callers can tell they must not trust
    a total built from them.
    """
    sn = str(sn or "")
    if not sn:
        return "Unknown", 0.0
    family = sn[0].upper()
    variant = sn[3].upper() if len(sn) > 3 else ""
    if family == "A":
        return ("AIO2400", 2.4) if variant == "3" else ("AB1000", 0.96)
    if family == "B":
        # packType 70 is the SF4000 Mix AC+'s internal 8 kWh pack, which shares
        # its serial prefix with the 0.96 kWh AB1000S.
        return ("I8000", 8.0) if _number(pack_type) == 70 else ("AB1000S", 0.96)
    if family == "C":
        return "AB2000" + ("S" if variant == "F" else "X" if variant == "E" else ""), 1.92
    if family == "F":
        return "AB3000", 2.88
    if family == "G":
        return "AB3000L", 2.88
    if family == "J":
        return "I2400", 2.4
    return "Unknown", 0.0


def packs_capacity_kwh(packs: list[dict]) -> float | None:
    """Total nominal capacity of ``packs``, or None if any pack is unknown."""
    if not packs:
        return None
    total = 0.0
    for pack in packs:
        _, kwh = zendure_pack_model(pack.get("sn"), pack.get("packType"))
        if kwh <= 0:
            return None
        total += kwh
    return round(total, 2)


def describe_packs(packs: list[dict]) -> str:
    """Short human summary such as ``3 × AB2000X``."""
    counts: dict[str, int] = {}
    for pack in packs:
        model, _ = zendure_pack_model(pack.get("sn"), pack.get("packType"))
        counts[model] = counts.get(model, 0) + 1
    return ", ".join(f"{n} × {model}" for model, n in counts.items())


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

class ZendureMqttDriver(BatteryDriver):
    """Push telemetry and set-point control for one Zendure Hyper 2000."""

    def __init__(
        self,
        hass: HomeAssistant,
        device_id: str,
        *,
        product_key: str | None = None,
        transport: ZendureMqttTransport | None = None,
        model: str = ZENDURE_MODEL_HYPER_2000,
        control_method: str = CONTROL_AUTOMATION,
        max_charge_power_w: int | None = None,
        max_discharge_power_w: int | None = None,
        clock: Callable[[], float] = time.monotonic,
        first_report_timeout: float = _FIRST_REPORT_TIMEOUT_S,
    ) -> None:
        self.hass = hass
        self.device_id = device_id.strip()
        self._product_key = (product_key or "").strip() or None
        self._transport = transport or HomeAssistantMqttTransport(hass)
        self._model = model or ZENDURE_MODEL_HYPER_2000
        self._control_method = (
            control_method if control_method in CONTROL_METHODS else CONTROL_AUTOMATION
        )
        self._clock = clock
        self._first_report_timeout = first_report_timeout

        default_charge, default_discharge = zendure_power_limits(self._model)
        self._capabilities = DriverCapabilities(
            hardware_soc_cutoff=True,     # socSet + minSoc exist on the device
            has_force_mode=False,
            push_telemetry=True,
            max_charge_power_w=int(max_charge_power_w or default_charge),
            max_discharge_power_w=int(max_discharge_power_w or default_discharge),
            # Two MPPT inputs feed the cells over DC. Published as the aggregate
            # solar_power (independent PV); the per-input keys are informational.
            # has_mppt_pv stays False: battery_power is already cell-side.
            has_mppt_pv=False,
            has_solar_telemetry=True,
            has_alarm_registers=False,
            has_rs485_control=False,
            has_energy_counters=False,    # no kWh in the report; synthesised
            # Nominal capacity comes from the pack serials. When a pack is not
            # recognised the coordinator falls back to the configured value.
            has_nominal_capacity=True,
            has_daily_energy_counters=False,
            setpoint_confirm_reliable=False,
            actuator_latency_s=3.0,
            readback_latency_s=6.0,
            min_charge_power_w=_MIN_POWER_W,
            min_discharge_power_w=_MIN_POWER_W,
            telemetry_liveness_checked=True,
            charge_cutoff_range=(70.0, 100.0),
            discharge_cutoff_range=(5.0, 50.0),
        )
        number_defs = _hyper_number_definitions(default_discharge)
        self._definitions: dict[str, list[dict]] = {
            "sensor": SENSOR_DEFINITIONS,
            "number": number_defs,
            "select": [],
            "switch": [],
            "binary_sensor": [],
            "button": [],
            "all": SENSOR_DEFINITIONS + number_defs,
        }
        self._read_groups = [
            ReadGroup(
                scan_interval="high",
                keys=tuple(d["key"] for d in self._definitions["all"])
                + (DELIVERED_AC_POWER_KEY, _COMMANDED_KEY, "max_charge_power"),
            )
        ]

        self._props: dict[str, Any] = {}
        self._power_seen: dict[str, float] = {}
        self._packs: dict[str, dict] = {}
        self._last_message: float | None = None
        self._first_report: asyncio.Event | None = None
        self._unsubscribers: list[Callable[[], Any]] = []
        self._transport_started = False
        self._connected = False
        self._shutting_down = False
        self._message_id = 0
        self._write_lock = asyncio.Lock()
        self._refresh_task: asyncio.Task | None = None
        self._last_command_w: int | None = None
        self._last_command_at: float | None = None
        # Kickstart state: the task waiting to nudge a small start, and whether
        # a nudge is being held right now. Instance attributes so tests can
        # shorten the timings.
        self._kick_task: asyncio.Task | None = None
        self._kicking = False
        self._kick_after_s = _KICK_AFTER_S
        self._kick_hold_s = _KICK_HOLD_S

    # --- definitions --------------------------------------------------------

    @property
    def sensor_definitions(self) -> list[dict]:
        return self._definitions["sensor"]

    @property
    def number_definitions(self) -> list[dict]:
        return self._definitions["number"]

    @property
    def select_definitions(self) -> list[dict]:
        return self._definitions["select"]

    @property
    def switch_definitions(self) -> list[dict]:
        return self._definitions["switch"]

    @property
    def binary_sensor_definitions(self) -> list[dict]:
        return self._definitions["binary_sensor"]

    @property
    def button_definitions(self) -> list[dict]:
        return self._definitions["button"]

    @property
    def all_definitions(self) -> list[dict]:
        return self._definitions["all"]

    @property
    def pack_field_specs(self) -> list[dict]:
        return PACK_FIELD_SPECS

    # --- identity -----------------------------------------------------------

    @property
    def capabilities(self) -> DriverCapabilities:
        return self._capabilities

    @property
    def model_label(self) -> Optional[str]:
        return "Hyper 2000"

    @property
    def model_key(self) -> str:
        return self._model

    @property
    def serial(self) -> Optional[str]:
        sn = self._props.get("sn")
        return str(sn) if sn else self.device_id

    @property
    def product_key(self) -> str | None:
        return self._product_key

    @property
    def control_method(self) -> str:
        return self._control_method

    # --- topics -------------------------------------------------------------

    def _iot_topic(self, suffix: str) -> str | None:
        if not self._product_key:
            return None
        return f"iot/{self._product_key}/{self.device_id}/{suffix}"

    # --- lifecycle ----------------------------------------------------------

    @property
    def connected(self) -> bool:
        return self._connected

    def set_shutting_down(self, value: bool) -> None:
        self._shutting_down = value

    async def connect(self) -> bool:
        if self._connected:
            # A reconnect attempt after the feed went quiet: nudge the device;
            # the coordinator's liveness probe decides whether it came back.
            await self._request_full_report()
            return True
        if not self._transport_started:
            if not await self._transport.start():
                return False
            self._transport_started = True
        self._first_report = asyncio.Event()
        try:
            unsubscribe = await self._transport.subscribe(
                _report_topic(self._product_key, self.device_id), self._handle_message
            )
        except Exception as err:
            if not self._shutting_down:
                _LOGGER.warning("Zendure MQTT: cannot subscribe for %s: %s", self.device_id, err)
            await self._release_transport()
            return False
        self._unsubscribers.append(unsubscribe)

        if self._last_message is None or not self._is_live():
            await self._request_full_report()
            try:
                await asyncio.wait_for(self._first_report.wait(), self._first_report_timeout)
            except (asyncio.TimeoutError, TimeoutError):
                if not self._shutting_down:
                    _LOGGER.warning(
                        "Zendure MQTT: no report from %s within %.0f s — is the "
                        "Hyper connected to this broker?",
                        self.device_id, self._first_report_timeout,
                    )
                await self._unsubscribe_all()
                await self._release_transport()
                return False

        self._connected = True
        self._ensure_refresh_task()
        _LOGGER.info(
            "Connected to Zendure Hyper %s over MQTT (product key %s)",
            self.device_id, self._product_key,
        )
        return True

    async def close(self) -> None:
        self._connected = False
        await self._cancel_kick()
        task, self._refresh_task = self._refresh_task, None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        await self._unsubscribe_all()
        await self._release_transport()

    async def _release_transport(self) -> None:
        if not self._transport_started:
            return
        self._transport_started = False
        try:
            await self._transport.stop()
        except Exception:
            pass

    async def _unsubscribe_all(self) -> None:
        for unsubscribe in self._unsubscribers:
            try:
                result = unsubscribe()
                if hasattr(result, "__await__"):
                    await result
            except Exception:
                pass
        self._unsubscribers.clear()

    def _ensure_refresh_task(self) -> None:
        if self._refresh_task is not None and not self._refresh_task.done():
            return
        create = getattr(self.hass, "async_create_background_task", None)
        coro = self._refresh_loop()
        if create is not None:
            self._refresh_task = create(coro, f"omnibattery_zendure_mqtt_{self.device_id}")
        else:
            self._refresh_task = asyncio.ensure_future(coro)

    async def _refresh_loop(self) -> None:
        while True:
            await asyncio.sleep(_REFRESH_INTERVAL_S)
            await self._refresh_once()

    async def _refresh_once(self) -> None:
        """One tick of the background cadence: full dump + setpoint keepalive."""
        try:
            await self._request_full_report()
            if (
                self._last_command_w is not None
                and self._last_command_at is not None
                and self._clock() - self._last_command_at >= _COMMAND_KEEPALIVE_S
            ):
                async with self._write_lock:
                    await self._send_setpoint(self._last_command_w)
        except Exception as err:
            if not self._shutting_down:
                _LOGGER.debug("Zendure MQTT refresh for %s failed: %s", self.device_id, err)

    # --- inbound ------------------------------------------------------------

    @callback
    def _handle_message(self, topic: str, payload: Any) -> None:
        parts = _split_topic(topic)
        if parts is None:
            return
        product_key, device_id, suffix = parts
        if device_id != self.device_id:
            return
        if self._product_key is None and product_key not in ("", "+"):
            self._product_key = product_key
        if suffix in ("properties/report", "properties/read/reply"):
            data = _decode(payload)
            if data is not None:
                self._merge_report(data)
        elif suffix == "time-sync":
            self._spawn(self._reply_time_sync())

    def _spawn(self, coro) -> None:
        create = getattr(self.hass, "async_create_task", None)
        if create is not None:
            create(coro)
        else:
            asyncio.ensure_future(coro)

    def _merge_report(self, data: dict) -> None:
        now = self._clock()
        merged = False
        props = data.get("properties")
        if isinstance(props, dict) and props:
            for prop, value in props.items():
                self._props[prop] = value
                if prop in _POWER_PROPS:
                    self._power_seen[prop] = now
            for prop, partner in _EXCLUSIVE_POWER_PAIRS.items():
                value = _number(props.get(prop))
                if value is not None and value > 0 and partner not in props:
                    self._props[partner] = 0
                    self._power_seen[partner] = now
            merged = True
        packs = data.get("packData")
        if isinstance(packs, list):
            for pack in packs:
                if not isinstance(pack, dict):
                    continue
                sn = pack.get("sn")
                if not sn:
                    continue
                self._packs.setdefault(str(sn), {}).update(pack)
                merged = True
        if merged:
            self._last_message = now
            if self._first_report is not None:
                self._first_report.set()

    # --- telemetry ----------------------------------------------------------

    @property
    def read_groups(self) -> list[ReadGroup]:
        return self._read_groups

    def _is_live(self) -> bool:
        return (
            self._last_message is not None
            and self._clock() - self._last_message <= _STALE_AFTER_S
        )

    def _power(self, prop: str) -> float | int:
        seen = self._power_seen.get(prop)
        if seen is None or self._clock() - seen > _POWER_ZERO_AFTER_S:
            return 0
        value = _number(self._props.get(prop))
        return value if value is not None else 0

    def _snapshot(self) -> TelemetrySnapshot:
        props = self._props
        snapshot: TelemetrySnapshot = {}
        for prop, key in _PROP_TO_KEY.items():
            if prop in props:
                snapshot[key] = props[prop]

        for key in _DECIPERCENT_KEYS:
            value = _number(snapshot.get(key))
            if value is not None:
                snapshot[key] = value / 10

        if "hyperTmp" in props:
            snapshot["internal_temperature"] = _decikelvin_to_celsius(props["hyperTmp"])

        remain = _number(snapshot.get("remain_discharge_time"))
        if remain is not None and remain >= _REMAIN_TIME_SENTINEL:
            snapshot["remain_discharge_time"] = None

        snapshot["solar_power"] = self._power("solarInputPower")
        snapshot["mppt1_power"] = self._power("solarPower1")
        snapshot["mppt2_power"] = self._power("solarPower2")

        packs = list(self._packs.values())
        if packs:
            snapshot["pack_count"] = snapshot.get("pack_count") or len(packs)
        max_vols = [v for p in packs if (v := _number(p.get("maxVol")))]
        min_vols = [v for p in packs if (v := _number(p.get("minVol")))]
        if max_vols:
            snapshot["max_cell_voltage"] = max(max_vols)
        if min_vols:
            snapshot["min_cell_voltage"] = min(min_vols)
        for idx, pack in enumerate(packs, start=1):
            prefix = f"pack{idx}_"
            if (soc := _number(pack.get("socLevel"))) is not None:
                snapshot[prefix + "soc"] = soc
            if (volt := _number(pack.get("totalVol"))) is not None:
                snapshot[prefix + "voltage"] = round(volt / 100, 2)
            if (vmax := _number(pack.get("maxVol"))) is not None:
                snapshot[prefix + "max_cell_voltage"] = round(vmax / 100, 3)
            if (vmin := _number(pack.get("minVol"))) is not None:
                snapshot[prefix + "min_cell_voltage"] = round(vmin / 100, 3)
            if (temp := _decikelvin_to_celsius(pack.get("maxTemp"))) is not None:
                snapshot[prefix + "temperature"] = temp
            if (power := _number(pack.get("power"))) is not None:
                snapshot[prefix + "power"] = power
            if pack.get("sn") is not None:
                snapshot[prefix + "sn"] = pack["sn"]
                snapshot[prefix + "model"] = zendure_pack_model(pack["sn"], pack.get("packType"))[0]
            if pack.get("state") is not None:
                snapshot[prefix + "state"] = pack["state"]
        capacity = packs_capacity_kwh(packs)
        if capacity is not None:
            snapshot["battery_total_energy"] = capacity

        snapshot["battery_power"] = self._power("outputPackPower") - self._power("packInputPower")
        snapshot[DELIVERED_AC_POWER_KEY] = (
            self._power("gridInputPower") - self._power("outputHomePower")
        )
        if self._last_command_w is not None:
            snapshot[_COMMANDED_KEY] = self._last_command_w
        return snapshot

    async def read_telemetry(self, keys: Optional[list[str]] = None) -> TelemetrySnapshot:
        """Return the merged report cache, or nothing once the feed went quiet."""
        if not self._is_live():
            return {}
        snapshot = self._snapshot()
        if keys is not None:
            wanted = set(keys)
            snapshot = {
                k: v for k, v in snapshot.items()
                if k in wanted
                or k in ("max_charge_power", DELIVERED_AC_POWER_KEY, _COMMANDED_KEY)
                or _PACK_KEY_RE.match(k)
            }
        return snapshot

    # --- outbound -----------------------------------------------------------

    def _envelope(self, body: dict) -> str:
        self._message_id += 1
        body["messageId"] = self._message_id
        body["deviceId"] = self.device_id
        body["timestamp"] = int(time.time())
        return json.dumps(body, separators=(",", ":"))

    async def _publish(self, suffix: str, body: dict) -> bool:
        topic = self._iot_topic(suffix)
        if topic is None:
            return False
        await self._transport.publish(topic, self._envelope(body))
        return True

    async def _request_full_report(self) -> bool:
        try:
            return await self._publish("properties/read", {"properties": ["getAll"]})
        except Exception as err:
            if not self._shutting_down:
                _LOGGER.debug("Zendure MQTT getAll for %s failed: %s", self.device_id, err)
            return False

    async def _reply_time_sync(self) -> None:
        try:
            await self._publish(
                "time-sync/reply",
                {"zoneOffset": "+00:00"},
            )
        except Exception as err:
            _LOGGER.debug("Zendure MQTT time-sync reply failed: %s", err)

    async def _write_properties(self, properties: dict) -> bool:
        try:
            return await self._publish("properties/write", {"properties": properties})
        except Exception as err:
            if not self._shutting_down:
                _LOGGER.warning("Zendure MQTT write to %s failed: %s", self.device_id, err)
            return False

    def _automation_body(self, net_power_w: int) -> dict:
        if net_power_w > 0:
            argument = {
                "autoModelProgram": 1,
                "autoModelValue": {
                    "chargingType": 1,
                    "price": 2,
                    "chargingPower": net_power_w,
                    "prices": [1] * 24,
                    "outPower": 0,
                    "freq": 0,
                },
                "msgType": 1,
                "autoModel": 8,
            }
        else:
            argument = {
                "autoModelProgram": 2,
                "autoModelValue": {
                    "chargingType": 0,
                    "chargingPower": 0,
                    "freq": 0,
                    "outPower": -net_power_w,
                },
                "msgType": 1,
                "autoModel": 8,
            }
        return {
            "arguments": [argument],
            "function": "deviceAutomation",
            "deviceKey": self.device_id,
        }

    @staticmethod
    def _properties_body(net_power_w: int) -> dict:
        if net_power_w > 0:
            return {"acMode": _AC_MODE_CHARGE, "inputLimit": net_power_w, "outputLimit": 0, "smartMode": 1}
        return {"acMode": _AC_MODE_DISCHARGE, "outputLimit": -net_power_w, "inputLimit": 0, "smartMode": 1}

    def _wire_power(self, request_w: int, *, kick: bool | None = None) -> int:
        """What is actually sent for ``request_w``: floored, and kicked if active."""
        if request_w == 0:
            return 0
        sign = 1 if request_w > 0 else -1
        limit = (
            self._capabilities.max_charge_power_w
            if sign > 0
            else self._capabilities.max_discharge_power_w
        )
        magnitude = max(abs(request_w), _MIN_POWER_W)
        if self._kicking if kick is None else kick:
            magnitude = max(magnitude, _KICK_POWER_W)
        return sign * min(magnitude, limit)

    async def _send_setpoint(self, request_w: int) -> bool:
        """Send ``request_w`` (as floored/kicked wire power) and remember the request.

        The request, not the wire power, is what ``commanded_net_power`` reports,
        so the controller's skip-if-unchanged guard keeps working while a small
        request is being rounded up or kicked.
        """
        wire_w = self._wire_power(request_w)
        if self._control_method == CONTROL_PROPERTIES:
            sent = await self._publish("properties/write", {"properties": self._properties_body(wire_w)})
        else:
            sent = await self._publish("function/invoke", self._automation_body(wire_w))
        if sent:
            self._last_command_w = request_w
            self._last_command_at = self._clock()
        return sent

    # --- kickstart ------------------------------------------------------------

    def _running_toward(self, sign: int) -> bool:
        """Whether the device is moving power in direction ``sign`` right now."""
        if not self._is_live():
            return False
        snapshot = self._snapshot()
        delivered = max(
            sign * float(snapshot.get("battery_power") or 0),
            sign * float(snapshot.get(DELIVERED_AC_POWER_KEY) or 0),
        )
        return delivered >= _RUNNING_W

    async def _cancel_kick(self) -> None:
        task, self._kick_task = self._kick_task, None
        self._kicking = False
        if task is not None and not task.done() and task is not asyncio.current_task():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    async def _kick_if_stuck(self, sign: int) -> None:
        """Nudge a small start that has produced nothing, then restore the request."""
        await asyncio.sleep(self._kick_after_s)
        if (
            not self._connected
            or self._last_command_w is None
            or (1 if self._last_command_w > 0 else -1 if self._last_command_w < 0 else 0) != sign
            or self._running_toward(sign)
        ):
            return
        _LOGGER.info(
            "Zendure MQTT: %s did not start on %d W; kickstarting at %d W",
            self.device_id, self._wire_power(self._last_command_w, kick=False), sign * _KICK_POWER_W,
        )
        self._kicking = True
        try:
            async with self._write_lock:
                await self._send_setpoint(self._last_command_w)
            deadline = self._clock() + self._kick_hold_s
            while self._clock() < deadline and not self._running_toward(sign):
                await asyncio.sleep(min(1.0, self._kick_hold_s))
        finally:
            self._kicking = False
        if (
            self._connected
            and self._last_command_w is not None
            and (1 if self._last_command_w > 0 else -1 if self._last_command_w < 0 else 0) == sign
        ):
            async with self._write_lock:
                await self._send_setpoint(self._last_command_w)

    def _schedule_kick(self, sign: int) -> None:
        coro = self._kick_if_stuck(sign)
        create = getattr(self.hass, "async_create_background_task", None)
        if create is not None:
            self._kick_task = create(coro, f"omnibattery_zendure_kick_{self.device_id}")
        else:
            self._kick_task = asyncio.ensure_future(coro)

    def _clamp(self, net_power_w: int) -> int:
        return max(
            -self._capabilities.max_discharge_power_w,
            min(self._capabilities.max_charge_power_w, int(round(net_power_w))),
        )

    async def apply_setpoint(
        self,
        net_power_w: int,
        *,
        mode_hint: Optional[str] = None,
        read_back: bool = True,
    ) -> SetpointResult:
        if not self._connected:
            return SetpointResult(False, 0, False, failure_reason="not_connected")
        applied = self._clamp(net_power_w)
        sign = 1 if applied > 0 else -1 if applied < 0 else 0
        previous = self._last_command_w or 0
        previous_sign = 1 if previous > 0 else -1 if previous < 0 else 0
        starting = sign != previous_sign
        if starting:
            # A new start (or stop): any kick belongs to the old one.
            await self._cancel_kick()
        try:
            async with self._write_lock:
                sent = await self._send_setpoint(applied)
        except Exception as err:
            if not self._shutting_down:
                _LOGGER.warning("Zendure MQTT setpoint for %s failed: %s", self.device_id, err)
            return SetpointResult(False, applied, False, failure_reason="write_failed")
        if not sent:
            return SetpointResult(False, applied, False, failure_reason="product_key_unknown")
        if (
            starting
            and sign != 0
            and abs(self._wire_power(applied, kick=False)) < _KICK_POWER_W
            and not self._running_toward(sign)
        ):
            self._schedule_kick(sign)
        battery_power = None
        if read_back and self._is_live():
            battery_power = int(self._snapshot()["battery_power"])
        # Report the power the device is asked for in steady state (floored);
        # commanded_net_power keeps the controller's own request.
        return SetpointResult(
            True,
            self._wire_power(applied, kick=False),
            False,
            battery_power_w=battery_power,
            applied={_COMMANDED_KEY: applied},
        )

    async def write_control(self, key: str, value: int) -> bool:
        if key not in _WRITABLE_KEYS:
            _LOGGER.debug("ZendureMqttDriver: no writable property for key %r", key)
            return False
        prop = _KEY_TO_PROP[key]
        wire = int(round(value * 10)) if key in _DECIPERCENT_KEYS else int(value)
        properties: dict[str, Any] = {prop: wire}
        if self._control_method == CONTROL_PROPERTIES:
            # smartMode is sticky and the power loop holds it at 1 (RAM); send 0
            # so configuration lands in flash and survives a reboot.
            properties["smartMode"] = 0
        ok = await self._write_properties(properties)
        if ok:
            self._props[prop] = wire
        return ok

    def net_power_from_data(self, data: dict) -> Optional[int]:
        value = data.get(_COMMANDED_KEY)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        return int(value)

    @property
    def control_dependency_keys(self) -> frozenset:
        return frozenset({
            "battery_soc", "battery_power", _COMMANDED_KEY, DELIVERED_AC_POWER_KEY,
        })

    async def apply_config(
        self,
        *,
        max_soc_pct: float,
        min_soc_pct: float,
        max_charge_power_w: int,
        max_discharge_power_w: int,
    ) -> bool:
        """Write the SOC window (socSet 70–100 %, minSoc 0–50 %, deci-percent)."""
        soc_set = max(70, min(100, int(max_soc_pct)))
        min_soc = max(0, min(50, int(min_soc_pct)))
        properties: dict[str, Any] = {"socSet": soc_set * 10, "minSoc": min_soc * 10}
        if self._control_method == CONTROL_PROPERTIES:
            properties["smartMode"] = 0
        return await self._write_properties(properties)

    async def set_charge_cutoff(self, soc_pct: float) -> bool:
        soc_set = max(70, min(100, int(soc_pct)))
        properties: dict[str, Any] = {"socSet": soc_set * 10}
        if self._control_method == CONTROL_PROPERTIES:
            properties["smartMode"] = 0
        return await self._write_properties(properties)

    async def standby(self) -> bool:
        """Hold the battery idle (0 W) — used on teardown."""
        await self._cancel_kick()
        try:
            async with self._write_lock:
                return await self._send_setpoint(0)
        except Exception:
            return False

    async def set_rs485_control(self, enable: bool) -> bool:
        return False

    # --- setup helpers ------------------------------------------------------

    @classmethod
    async def probe(
        cls,
        transport: ZendureMqttTransport,
        device_id: str,
        product_key: str | None = None,
        *,
        timeout: float = _FIRST_REPORT_TIMEOUT_S,
    ) -> tuple[bool, dict]:
        """Ask the device for a full report over an already started transport.

        Returns ``(answered, info)``; ``info`` carries the product key learned
        from the topic, the pack count and the battery SOC when reported.
        """
        device_id = device_id.strip()
        info: dict[str, Any] = {}
        answered = asyncio.Event()
        packs_seen = asyncio.Event()
        packs_by_sn: dict[str, dict] = {}

        def _on_message(topic: str, payload: Any) -> None:
            parts = _split_topic(topic)
            if parts is None or parts[1] != device_id:
                return
            if parts[2] not in ("properties/report", "properties/read/reply"):
                return
            data = _decode(payload)
            if not data:
                return
            props = data.get("properties")
            packs = data.get("packData")
            if not (isinstance(props, dict) and props) and not isinstance(packs, list):
                return
            known = packs_by_sn
            info.setdefault("product_key", parts[0])
            if isinstance(props, dict):
                if (soc := _number(props.get("electricLevel"))) is not None:
                    info["battery_soc"] = soc
                if (count := _number(props.get("packNum"))) is not None:
                    info["pack_count"] = int(count)
            if isinstance(packs, list):
                for pack in packs:
                    if isinstance(pack, dict) and pack.get("sn"):
                        known[str(pack["sn"])] = {**known.get(str(pack["sn"]), {}), **pack}
                if known:
                    info["packs"] = [
                        zendure_pack_model(p.get("sn"), p.get("packType"))[0]
                        for p in known.values()
                    ]
                    info["pack_summary"] = describe_packs(list(known.values()))
                    capacity = packs_capacity_kwh(list(known.values()))
                    if capacity is not None:
                        info["capacity_kwh"] = capacity
                    info["pack_count"] = len(known)
                    packs_seen.set()
            answered.set()

        unsubscribe = None
        try:
            unsubscribe = await transport.subscribe(_report_topic(product_key, device_id), _on_message)
            keys = (product_key,) if product_key else HYPER_2000_PRODUCT_KEYS
            body = json.dumps({
                "properties": ["getAll"],
                "messageId": 1,
                "deviceId": device_id,
                "timestamp": int(time.time()),
            })
            for key in keys:
                await transport.publish(f"iot/{key}/{device_id}/properties/read", body)
            await asyncio.wait_for(answered.wait(), timeout)
            if not packs_seen.is_set():
                # Pack data can trail the property report; capacity needs it.
                try:
                    await asyncio.wait_for(packs_seen.wait(), min(timeout, _PACK_WAIT_S))
                except (asyncio.TimeoutError, TimeoutError):
                    pass
            return True, info
        except (asyncio.TimeoutError, TimeoutError):
            return False, info
        except Exception as err:
            _LOGGER.debug("Zendure MQTT probe of %s failed: %s", device_id, err)
            return False, info
        finally:
            if unsubscribe is not None:
                try:
                    result = unsubscribe()
                    if hasattr(result, "__await__"):
                        await result
                except Exception:
                    pass

    @classmethod
    async def discover(
        cls,
        transport: ZendureMqttTransport,
        *,
        timeout: float = 30.0,
    ) -> dict[str, str]:
        """Listen for device traffic and return ``{device_id: product_key}``.

        Any message a device publishes counts, not only property reports: an
        idle Hyper (no sun, no load change) can go minutes without a report,
        but still sends clock-sync, log and event messages, and a burst of
        them right after it (re)connects. Only Hyper 2000 product keys are
        returned when any are seen, so other Zendure devices on the same
        broker do not make the choice ambiguous.
        """
        seen: dict[str, str] = {}

        def _on_message(topic: str, payload: Any) -> None:
            parts = _split_topic(topic)
            if parts is None:
                return
            product_key, device_id, _ = parts
            if _ZENDURE_KEY_RE.fullmatch(product_key) and _ZENDURE_ID_RE.fullmatch(device_id):
                seen.setdefault(device_id, product_key)

        unsubscribe = None
        try:
            unsubscribe = await transport.subscribe("/+/+/#", _on_message)
            await asyncio.sleep(timeout)
        except Exception as err:
            _LOGGER.debug("Zendure MQTT discovery failed: %s", err)
        finally:
            if unsubscribe is not None:
                try:
                    result = unsubscribe()
                    if hasattr(result, "__await__"):
                        await result
                except Exception:
                    pass
        hyper = {d: k for d, k in seen.items() if k in HYPER_2000_PRODUCT_KEYS}
        return hyper or seen
