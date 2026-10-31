"""Zendure Hyper 2000 MQTT driver contract tests.

The driver is exercised through an in-memory broker double, so nothing here
needs Home Assistant's MQTT integration or a real Mosquitto.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest

from custom_components.omnibattery.drivers.base import DELIVERED_AC_POWER_KEY
from custom_components.omnibattery.drivers import zendure_mqtt as zm
from custom_components.omnibattery.drivers.zendure_mqtt import (
    CONTROL_PROPERTIES,
    HomeAssistantMqttTransport,
    PahoMqttTransport,
    ZendureMqttDriver,
    ZendureMqttTransport,
)

PK = "ja72U0ha"
DEV = "Ab12Cd34"
REPORT = f"/{PK}/{DEV}/properties/report"

FULL_REPORT = {
    "properties": {
        "electricLevel": 63,
        "outputPackPower": 410,
        "outputHomePower": 0,
        "gridInputPower": 650,
        "solarInputPower": 0,
        "solarPower1": 0,
        "solarPower2": 0,
        "hyperTmp": 2981,
        "socSet": 1000,
        "minSoc": 100,
        "inverseMaxPower": 1200,
        "outputLimit": 0,
        "inputLimit": 650,
        "acMode": 1,
        "packNum": 2,
        "remainOutTime": 59940,
    },
    "packData": [
        {"sn": "CO4EAAA1", "socLevel": 62, "maxTemp": 2961, "maxVol": 334,
         "minVol": 331, "totalVol": 5300, "power": 205, "state": 1},
        {"sn": "CO4EAAA2", "socLevel": 64, "maxTemp": 2971, "maxVol": 335,
         "minVol": 332, "totalVol": 5310, "power": 205, "state": 1},
    ],
}


def _matches(topic_filter: str, topic: str) -> bool:
    f_parts, t_parts = topic_filter.split("/"), topic.split("/")
    for i, part in enumerate(f_parts):
        if part == "#":
            return True
        if i >= len(t_parts):
            return False
        if part not in ("+", t_parts[i]):
            return False
    return len(f_parts) == len(t_parts)


class FakeBroker(ZendureMqttTransport):
    def __init__(self, *, reply_with: dict | None = FULL_REPORT, start_ok: bool = True,
                 device_pk: str = PK):
        self.device_pk = device_pk
        self.handlers: dict[str, list] = {}
        self.published: list[tuple[str, dict]] = []
        self.reply_with = reply_with
        self.start_ok = start_ok
        self.started = False
        self.stopped = 0

    async def start(self) -> bool:
        self.started = self.start_ok
        return self.start_ok

    async def stop(self) -> None:
        self.started = False
        self.stopped += 1

    async def subscribe(self, topic, handler):
        self.handlers.setdefault(topic, []).append(handler)
        return lambda: self.handlers[topic].remove(handler)

    async def publish(self, topic, payload):
        body = json.loads(payload)
        self.published.append((topic, body))
        if self.reply_with is not None and topic.endswith("/properties/read"):
            _, pk, dev, *_ = topic.split("/")
            if pk != self.device_pk:
                return  # nobody with that product key on this broker
            self.deliver(f"/{pk}/{dev}/properties/report", self.reply_with)

    def deliver(self, topic, body):
        payload = json.dumps(body) if isinstance(body, dict) else body
        for topic_filter, handlers in list(self.handlers.items()):
            if _matches(topic_filter, topic):
                for handler in list(handlers):
                    handler(topic, payload)

    def last(self, suffix):
        return [body for topic, body in self.published if topic.endswith(suffix)][-1]


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def _hass():
    return SimpleNamespace(
        async_create_background_task=lambda coro, name: asyncio.create_task(coro, name=name),
        async_create_task=asyncio.ensure_future,
        loop=None,
    )


async def _driver(broker=None, **kwargs):
    broker = broker or FakeBroker()
    clock = kwargs.pop("clock", Clock())
    driver = ZendureMqttDriver(
        _hass(), DEV, product_key=kwargs.pop("product_key", PK),
        transport=broker, clock=clock, first_report_timeout=kwargs.pop("timeout", 0.2),
        **kwargs,
    )
    return driver, broker, clock


@pytest.mark.asyncio
async def test_connect_requests_full_dump_and_maps_report():
    driver, broker, _ = await _driver()
    assert await driver.connect()
    topic, body = broker.published[0]
    assert topic == f"iot/{PK}/{DEV}/properties/read"
    assert body["properties"] == ["getAll"] and body["deviceId"] == DEV
    assert f"/{PK}/{DEV}/#" in broker.handlers

    data = await driver.read_telemetry()
    assert data["battery_soc"] == 63
    assert data["battery_power"] == 410          # +charge
    assert data[DELIVERED_AC_POWER_KEY] == 650   # grid in, nothing out
    assert data["internal_temperature"] == 25.0  # 2981 dK
    assert data["soc_set"] == 100 and data["min_soc"] == 10
    assert data["remain_discharge_time"] is None  # idle sentinel
    assert data["solar_power"] == 0 and data["mppt1_power"] == 0
    assert data["pack_count"] == 2
    assert data["max_cell_voltage"] == 335 and data["min_cell_voltage"] == 331
    assert data["pack1_temperature"] == 23.0 and data["pack2_soc"] == 64
    assert data["pack1_voltage"] == 53.0 and data["pack1_sn"] == "CO4EAAA1"
    assert data["pack1_model"] == "AB2000X"
    assert data["battery_total_energy"] == 3.84  # 2 × 1.92 kWh from the serials
    assert driver.capabilities.has_nominal_capacity
    assert driver.model_label == "Hyper 2000"
    assert driver.capabilities.max_charge_power_w == 1200
    assert driver.capabilities.max_discharge_power_w == 1200
    assert driver.capabilities.push_telemetry and driver.capabilities.has_solar_telemetry
    assert not driver.capabilities.has_mppt_pv
    await driver.close()
    assert broker.stopped == 1


@pytest.mark.asyncio
async def test_internal_temperature_definition_is_not_rescaled():
    definition = next(d for d in zm.SENSOR_DEFINITIONS if d["key"] == "internal_temperature")
    assert definition["scale"] == 1


@pytest.mark.asyncio
async def test_connect_fails_without_a_report_and_releases_broker():
    driver, broker, _ = await _driver(FakeBroker(reply_with=None))
    assert not await driver.connect()
    assert broker.stopped == 1
    assert not driver.connected


@pytest.mark.asyncio
async def test_connect_fails_when_broker_unreachable():
    driver, broker, _ = await _driver(FakeBroker(start_ok=False))
    assert not await driver.connect()
    assert broker.published == []


@pytest.mark.asyncio
async def test_deltas_merge_and_exclusive_directions_reset():
    driver, broker, _ = await _driver()
    assert await driver.connect()
    # Switch to discharging: the delta names only the new direction.
    broker.deliver(REPORT, {"properties": {"packInputPower": 300, "outputHomePower": 290}})
    data = await driver.read_telemetry()
    assert data["battery_power"] == -300
    assert data[DELIVERED_AC_POWER_KEY] == -290
    assert data["battery_soc"] == 63  # untouched by the delta
    broker.deliver(REPORT, {"packData": [{"sn": "CO4EAAA1", "socLevel": 61}]})
    data = await driver.read_telemetry()
    assert data["pack1_soc"] == 61 and data["pack1_temperature"] == 23.0
    await driver.close()


@pytest.mark.asyncio
async def test_power_not_reported_for_several_dumps_reads_zero():
    clock = Clock()
    driver, broker, _ = await _driver(clock=clock)
    assert await driver.connect()
    broker.deliver(REPORT, {"properties": {"solarInputPower": 500, "solarPower1": 500}})
    assert (await driver.read_telemetry())["solar_power"] == 500
    clock.now += 80  # > 2.5 refresh intervals, other messages keep arriving
    broker.deliver(REPORT, {"properties": {"electricLevel": 64}})
    data = await driver.read_telemetry()
    assert data["solar_power"] == 0 and data["mppt1_power"] == 0
    assert data["battery_soc"] == 64
    await driver.close()


@pytest.mark.asyncio
async def test_quiet_feed_is_not_reported_as_live():
    clock = Clock()
    driver, broker, _ = await _driver(clock=clock)
    assert await driver.connect()
    assert await driver.read_telemetry(["battery_soc"])
    clock.now += 121
    assert await driver.read_telemetry(["battery_soc"]) == {}
    assert driver.capabilities.telemetry_liveness_checked
    broker.deliver(REPORT, {"properties": {"electricLevel": 60}})
    assert (await driver.read_telemetry(["battery_soc"]))["battery_soc"] == 60
    await driver.close()


@pytest.mark.asyncio
async def test_key_filter_keeps_control_keys_and_packs():
    driver, _, _ = await _driver()
    assert await driver.connect()
    await driver.apply_setpoint(-200, read_back=False)
    data = await driver.read_telemetry(["battery_soc"])
    assert set(data) >= {"battery_soc", DELIVERED_AC_POWER_KEY, "commanded_net_power", "pack1_soc"}
    assert "battery_power" not in data
    await driver.close()


@pytest.mark.asyncio
async def test_automation_setpoints():
    driver, broker, _ = await _driver()
    assert await driver.connect()

    result = await driver.apply_setpoint(800)
    assert result.ok and result.net_power_w == 800 and not result.confirmed
    assert result.applied == {"commanded_net_power": 800}
    assert result.battery_power_w == 410
    body = broker.last("/function/invoke")
    assert broker.published[-1][0] == f"iot/{PK}/{DEV}/function/invoke"
    assert body["function"] == "deviceAutomation" and body["deviceKey"] == DEV
    arg = body["arguments"][0]
    assert arg["autoModel"] == 8 and arg["autoModelProgram"] == 1
    assert arg["autoModelValue"]["chargingPower"] == 800
    assert arg["autoModelValue"]["outPower"] == 0

    result = await driver.apply_setpoint(-5000)  # clamped to 1200 W
    arg = broker.last("/function/invoke")["arguments"][0]
    assert result.net_power_w == -1200
    assert arg["autoModelProgram"] == 2 and arg["autoModelValue"]["outPower"] == 1200
    assert arg["autoModelValue"]["chargingPower"] == 0

    await driver.apply_setpoint(0)
    arg = broker.last("/function/invoke")["arguments"][0]
    assert arg["autoModel"] == 8 and arg["autoModelValue"]["outPower"] == 0
    assert driver.net_power_from_data({"commanded_net_power": 0}) == 0
    assert driver.net_power_from_data({}) is None
    await driver.close()


@pytest.mark.asyncio
async def test_property_setpoints():
    driver, broker, _ = await _driver(control_method=CONTROL_PROPERTIES)
    assert await driver.connect()
    await driver.apply_setpoint(600)
    assert broker.last("/properties/write")["properties"] == {
        "acMode": 1, "inputLimit": 600, "outputLimit": 0, "smartMode": 1,
    }
    await driver.apply_setpoint(-450)
    assert broker.last("/properties/write")["properties"] == {
        "acMode": 2, "outputLimit": 450, "inputLimit": 0, "smartMode": 1,
    }
    assert await driver.write_control("min_soc", 15)
    assert broker.last("/properties/write")["properties"] == {"minSoc": 150, "smartMode": 0}
    await driver.close()


@pytest.mark.asyncio
async def test_setpoint_requires_connection():
    driver, broker, _ = await _driver()
    result = await driver.apply_setpoint(100)
    assert not result.ok and result.failure_reason == "not_connected"
    assert broker.published == []


@pytest.mark.asyncio
async def test_config_writes_use_deci_percent():
    driver, broker, _ = await _driver()
    assert await driver.connect()
    assert await driver.write_control("soc_set", 90)
    assert broker.last("/properties/write")["properties"] == {"socSet": 900}
    assert (await driver.read_telemetry())["soc_set"] == 90
    assert await driver.write_control("inverse_max_power", 800)
    assert broker.last("/properties/write")["properties"] == {"inverseMaxPower": 800}
    assert not await driver.write_control("lamp_switch", 1)
    assert await driver.apply_config(
        max_soc_pct=110, min_soc_pct=3, max_charge_power_w=1200, max_discharge_power_w=1200
    )
    assert broker.last("/properties/write")["properties"] == {"socSet": 1000, "minSoc": 30}
    assert await driver.set_charge_cutoff(50)
    assert broker.last("/properties/write")["properties"] == {"socSet": 700}
    await driver.close()


@pytest.mark.asyncio
async def test_product_key_is_learned_from_the_topic():
    broker = FakeBroker(reply_with=None)
    driver, broker, _ = await _driver(broker, product_key=None, timeout=1.0)
    task = asyncio.ensure_future(driver.connect())
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert f"/+/{DEV}/#" in broker.handlers
    assert broker.published == []  # nothing to address before the key is known
    broker.deliver(f"/gDa3tb/{DEV}/properties/report", FULL_REPORT)
    assert await task
    assert driver.product_key == "gDa3tb"
    await driver.apply_setpoint(100)
    assert broker.published[-1][0] == f"iot/gDa3tb/{DEV}/function/invoke"
    await driver.close()


@pytest.mark.asyncio
async def test_other_devices_are_ignored():
    driver, broker, _ = await _driver()
    assert await driver.connect()
    broker.handlers[f"/{PK}/{DEV}/#"][0](f"/{PK}/Other123/properties/report",
                                          json.dumps({"properties": {"electricLevel": 1}}))
    assert (await driver.read_telemetry())["battery_soc"] == 63
    await driver.close()


@pytest.mark.asyncio
async def test_refresh_requests_dump_and_keeps_setpoint_alive():
    clock = Clock()
    driver, broker, _ = await _driver(clock=clock)
    assert await driver.connect()
    await driver.apply_setpoint(-300)
    sent = len(broker.published)
    await driver._refresh_once()
    assert broker.published[sent][0].endswith("/properties/read")
    assert len(broker.published) == sent + 1  # setpoint still fresh
    clock.now += 61
    await driver._refresh_once()
    arg = broker.last("/function/invoke")["arguments"][0]
    assert arg["autoModelValue"]["outPower"] == 300
    await driver.close()


@pytest.mark.asyncio
async def test_time_sync_is_answered():
    driver, broker, _ = await _driver()
    assert await driver.connect()
    broker.deliver(f"/{PK}/{DEV}/time-sync", {"messageId": 5})
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    topic, body = broker.published[-1]
    assert topic == f"iot/{PK}/{DEV}/time-sync/reply" and "timestamp" in body
    await driver.close()


@pytest.mark.asyncio
async def test_probe_tries_known_product_keys_and_cleans_up():
    broker = FakeBroker()
    ok, info = await ZendureMqttDriver.probe(broker, DEV, timeout=0.2)
    assert ok
    assert info == {
        "product_key": PK, "battery_soc": 63, "pack_count": 2,
        "packs": ["AB2000X", "AB2000X"], "pack_summary": "2 × AB2000X",
        "capacity_kwh": 3.84,
    }
    assert {t for t, _ in broker.published} >= {f"iot/{k}/{DEV}/properties/read" for k in zm.HYPER_2000_PRODUCT_KEYS}
    assert all(not handlers for handlers in broker.handlers.values())

    ok, _ = await ZendureMqttDriver.probe(FakeBroker(reply_with=None), DEV, PK, timeout=0.05)
    assert not ok


@pytest.mark.asyncio
async def test_discover_prefers_hyper_devices():
    broker = FakeBroker(reply_with=None)

    async def _chatter():
        await asyncio.sleep(0.01)
        broker.deliver("/73bkTV/HubDevice/properties/report", {"properties": {"electricLevel": 5}})
        broker.deliver(f"/{PK}/{DEV}/properties/report", {"properties": {"electricLevel": 50}})

    task = asyncio.ensure_future(_chatter())
    found = await ZendureMqttDriver.discover(broker, timeout=0.05)
    await task
    assert found == {DEV: PK}


@pytest.mark.asyncio
async def test_home_assistant_transport_uses_integration(monkeypatch):
    calls = {"subs": [], "pubs": []}

    async def subscribe(hass, topic, cb, qos=0):
        calls["subs"].append((topic, cb))
        return lambda: None

    async def publish(hass, topic, payload, qos=0, retain=False):
        calls["pubs"].append((topic, payload, qos, retain))

    async def wait_for_client(hass):
        return True

    monkeypatch.setattr(zm.mqtt, "async_subscribe", subscribe, raising=False)
    monkeypatch.setattr(zm.mqtt, "async_publish", publish, raising=False)
    monkeypatch.setattr(zm.mqtt, "async_wait_for_mqtt_client", wait_for_client, raising=False)
    transport = HomeAssistantMqttTransport(SimpleNamespace())
    assert await transport.start()
    received = []
    await transport.subscribe("/+/x/#", lambda t, p: received.append((t, p)))
    calls["subs"][0][1](SimpleNamespace(topic="/pk/x/properties/report", payload="{}"))
    assert received == [("/pk/x/properties/report", "{}")]
    await transport.publish("iot/pk/x/properties/read", "{}")
    assert calls["pubs"] == [("iot/pk/x/properties/read", "{}", 0, False)]


class _FakePahoClient:
    def __init__(self, refuse=False):
        self.refuse = refuse
        self.subscribed = []
        self.callbacks = {}
        self.credentials = None
        self.published = []
        self.on_connect = self.on_disconnect = None

    def username_pw_set(self, user, password):
        self.credentials = (user, password)

    def reconnect_delay_set(self, min_delay, max_delay):
        pass

    def connect_async(self, host, port, keepalive):
        self.target = (host, port)

    def loop_start(self):
        loop = asyncio.get_running_loop()
        reason = SimpleNamespace(is_failure=self.refuse)
        loop.call_soon(lambda: self.on_connect(self, None, None, reason, None))

    def loop_stop(self):
        pass

    def disconnect(self):
        pass

    def subscribe(self, topic, qos):
        self.subscribed.append(topic)

    def unsubscribe(self, topic):
        self.subscribed.remove(topic)

    def message_callback_add(self, topic, cb):
        self.callbacks[topic] = cb

    def message_callback_remove(self, topic):
        self.callbacks.pop(topic, None)

    def publish(self, topic, payload, qos=0, retain=False):
        self.published.append((topic, payload))
        return SimpleNamespace(rc=0)


@pytest.mark.asyncio
async def test_paho_transport_connects_and_replays_subscriptions():
    client = _FakePahoClient()
    transport = PahoMqttTransport(
        asyncio.get_running_loop(), "192.168.1.5", 1883,
        username="hyper", password="secret", client_factory=lambda cid: client,
    )
    assert await transport.start()
    assert client.target == ("192.168.1.5", 1883)
    assert client.credentials == ("hyper", "secret")
    received = []
    unsubscribe = await transport.subscribe(f"/{PK}/{DEV}/#", lambda t, p: received.append((t, p)))
    assert client.subscribed == [f"/{PK}/{DEV}/#"]
    # A reconnect re-subscribes everything.
    client.on_connect(client, None, None, SimpleNamespace(is_failure=False), None)
    assert client.subscribed.count(f"/{PK}/{DEV}/#") == 2
    client.callbacks[f"/{PK}/{DEV}/#"](client, None, SimpleNamespace(topic=REPORT, payload=b"{}"))
    await asyncio.sleep(0)
    assert received == [(REPORT, b"{}")]
    await transport.publish("iot/x", "{}")
    assert client.published == [("iot/x", "{}")]
    unsubscribe()
    assert f"/{PK}/{DEV}/#" not in client.callbacks
    await transport.stop()
    with pytest.raises(ConnectionError):
        await transport.publish("iot/x", "{}")


@pytest.mark.asyncio
async def test_paho_transport_reports_refused_connection():
    transport = PahoMqttTransport(
        asyncio.get_running_loop(), "broker", 1883, connect_timeout=0.05,
        client_factory=lambda cid: _FakePahoClient(refuse=True),
    )
    assert not await transport.start()
    assert transport.last_error.startswith("connection refused")


def test_build_transport_selects_broker():
    hass = SimpleNamespace(loop=None)
    assert isinstance(zm.build_transport(hass), HomeAssistantMqttTransport)
    custom = zm.build_transport(hass, zm.BROKER_CUSTOM, host=" 10.0.0.2 ", port=1884)
    assert isinstance(custom, PahoMqttTransport)
    assert (custom._host, custom._port) == ("10.0.0.2", 1884)


@pytest.mark.parametrize(
    ("sn", "pack_type", "expected"),
    [
        ("CO4EAAA1", None, ("AB2000X", 1.92)),
        ("CO4FAAA1", None, ("AB2000S", 1.92)),
        ("CO4AAAA1", None, ("AB2000", 1.92)),
        ("AO4AAAA1", None, ("AB1000", 0.96)),
        ("BO4AAAA1", 70, ("I8000", 8.0)),
        ("FO4AAAA1", None, ("AB3000", 2.88)),
        ("ZZZZ", None, ("Unknown", 0.0)),
        ("", None, ("Unknown", 0.0)),
    ],
)
def test_pack_model_from_serial(sn, pack_type, expected):
    assert zm.zendure_pack_model(sn, pack_type) == expected


@pytest.mark.asyncio
async def test_capacity_follows_the_stack_and_skips_unknown_packs():
    driver, broker, _ = await _driver()
    assert await driver.connect()
    broker.deliver(REPORT, {"packData": [{"sn": "CO4EAAA3", "socLevel": 60}]})
    assert (await driver.read_telemetry())["battery_total_energy"] == 5.76
    broker.deliver(REPORT, {"packData": [{"sn": "XX99", "socLevel": 60}]})
    data = await driver.read_telemetry()
    # An unrecognised pack makes the total untrustworthy; the configured
    # capacity stands in (coordinator side), so the driver reports none.
    assert "battery_total_energy" not in data
    assert data["pack4_model"] == "Unknown"
    await driver.close()


@pytest.mark.asyncio
async def test_probe_waits_for_trailing_pack_data():
    broker = FakeBroker(reply_with={"properties": {"electricLevel": 50}})

    async def _late_packs():
        await asyncio.sleep(0.05)
        broker.deliver(REPORT, {"packData": [{"sn": "CO4EAAA1"}, {"sn": "CO4EAAA2"}, {"sn": "CO4EAAA3"}]})

    task = asyncio.ensure_future(_late_packs())
    ok, info = await ZendureMqttDriver.probe(broker, DEV, PK, timeout=1.0)
    await task
    assert ok and info["capacity_kwh"] == 5.76 and info["pack_summary"] == "3 × AB2000X"


@pytest.mark.asyncio
async def test_discover_counts_any_device_traffic_and_ignores_noise():
    """An idle Hyper sends no report but still logs and asks for the time."""
    broker = FakeBroker(reply_with=None)

    async def _chatter():
        await asyncio.sleep(0.01)
        broker.deliver(f"/{PK}/{DEV}/properties/report", {"properties": {"electricLevel": 50}})
        broker.deliver(f"/{PK}/Idle5678/time-sync", {"messageId": 1})
        broker.deliver("/gDa3tb/Quiet999/log", {"log": "x"})
        broker.deliver("/not a key/x/y", {})
        broker.deliver("/home/livingroom/temperature", {})

    task = asyncio.ensure_future(_chatter())
    found = await ZendureMqttDriver.discover(broker, timeout=0.05)
    await task
    assert found == {DEV: PK, "Idle5678": PK, "Quiet999": "gDa3tb"}


@pytest.mark.asyncio
async def test_failed_publish_is_reported_and_not_cached_as_commanded():
    class FailingBroker(FakeBroker):
        fail = False

        async def publish(self, topic, payload):
            if self.fail and "/function/invoke" in topic:
                raise ConnectionError("broker gone")
            await super().publish(topic, payload)

    broker = FailingBroker()
    driver, broker, _ = await _driver(broker)
    assert await driver.connect()
    assert (await driver.apply_setpoint(-300)).ok
    broker.fail = True
    result = await driver.apply_setpoint(500)
    assert not result.ok and result.failure_reason == "write_failed"
    # The last command that reached the broker is still the one reported.
    assert (await driver.read_telemetry())["commanded_net_power"] == -300
    assert not await driver.standby()
    await driver.close()


@pytest.mark.asyncio
async def test_standby_sends_zero_and_close_then_reconnect_works():
    driver, broker, _ = await _driver()
    assert await driver.connect()
    assert await driver.standby()
    arg = broker.last("/function/invoke")["arguments"][0]
    assert arg["autoModelValue"]["outPower"] == 0
    assert arg["autoModelValue"]["chargingPower"] == 0
    await driver.close()
    assert not driver.connected and broker.stopped == 1
    assert all(not handlers for handlers in broker.handlers.values())
    assert await driver.connect()
    assert driver.connected and broker.started
    await driver.close()


@pytest.mark.asyncio
async def test_repeated_connect_only_nudges_the_device():
    driver, broker, _ = await _driver()
    assert await driver.connect()
    subscriptions = sum(len(h) for h in broker.handlers.values())
    sent = len(broker.published)
    assert await driver.connect()
    assert sum(len(h) for h in broker.handlers.values()) == subscriptions
    assert broker.published[sent][0].endswith("/properties/read")
    await driver.close()


@pytest.mark.asyncio
async def test_default_paho_client_is_built_off_the_event_loop(monkeypatch):
    import threading

    built_in = []

    def _fake_default(client_id):
        built_in.append(threading.current_thread() is threading.main_thread())
        return _FakePahoClient()

    monkeypatch.setattr(PahoMqttTransport, "_default_client", staticmethod(_fake_default))
    transport = PahoMqttTransport(asyncio.get_running_loop(), "broker", 1883)
    assert await transport.start()
    assert built_in == [False]
    await transport.stop()


# ----------------------------------------------------------------------
# Minimum power and kickstart
# ----------------------------------------------------------------------


def _out_power(broker):
    value = broker.last("/function/invoke")["arguments"][0]["autoModelValue"]
    return value["chargingPower"] or -value["outPower"]


@pytest.mark.asyncio
async def test_small_requests_are_rounded_up_to_the_floor():
    driver, broker, _ = await _driver()
    assert await driver.connect()
    assert driver.capabilities.min_charge_power_w == 50
    assert driver.capabilities.min_discharge_power_w == 50

    result = await driver.apply_setpoint(30)
    assert _out_power(broker) == 50 and result.net_power_w == 50
    # The controller's own request stays the cache key, so its skip guard works.
    assert result.applied == {"commanded_net_power": 30}
    assert driver.net_power_from_data(await driver.read_telemetry()) == 30

    await driver.apply_setpoint(-20)
    assert _out_power(broker) == -50
    await driver.apply_setpoint(0)
    assert _out_power(broker) == 0
    await driver.apply_setpoint(400)
    assert _out_power(broker) == 400
    await driver.close()


@pytest.mark.asyncio
async def test_floor_also_applies_to_property_writes_and_keepalive():
    clock = Clock()
    driver, broker, _ = await _driver(control_method=CONTROL_PROPERTIES, clock=clock)
    assert await driver.connect()
    await driver.apply_setpoint(-10)
    assert broker.last("/properties/write")["properties"]["outputLimit"] == 50
    clock.now += 61
    await driver._refresh_once()
    assert broker.last("/properties/write")["properties"]["outputLimit"] == 50
    await driver.close()


async def _stuck_start(request_w, *, delivering=None):
    """Start ``request_w`` on a device that reports no power unless told to."""
    broker = FakeBroker(reply_with={"properties": {"electricLevel": 50}})
    driver, broker, _ = await _driver(broker)
    driver._kick_after_s = 0.02
    driver._kick_hold_s = 0.3
    assert await driver.connect()
    await driver.apply_setpoint(request_w)
    return driver, broker


@pytest.mark.asyncio
async def test_a_small_start_that_does_not_run_is_kicked_then_restored():
    driver, broker = await _stuck_start(-60)
    assert _out_power(broker) == -60
    await asyncio.sleep(0.05)
    assert _out_power(broker) == -100  # kickstart
    broker.deliver(REPORT, {"properties": {"packInputPower": 100, "outputHomePower": 95}})
    await asyncio.sleep(0.05)
    for _ in range(40):
        if _out_power(broker) == -60:
            break
        await asyncio.sleep(0.05)
    assert _out_power(broker) == -60  # back to the request once running
    assert driver.net_power_from_data(await driver.read_telemetry()) == -60
    await driver.close()


@pytest.mark.asyncio
async def test_no_kick_when_the_device_starts_on_its_own_or_the_request_is_large():
    driver, broker = await _stuck_start(70)
    broker.deliver(REPORT, {"properties": {"outputPackPower": 70, "gridInputPower": 70}})
    await asyncio.sleep(0.1)
    assert _out_power(broker) == 70
    await driver.close()

    driver, broker = await _stuck_start(400)
    sent = len(broker.published)
    await asyncio.sleep(0.1)
    assert len([t for t, _ in broker.published[sent:] if t.endswith("/function/invoke")]) == 0
    await driver.close()


@pytest.mark.asyncio
async def test_a_kick_is_dropped_on_direction_change_stop_and_close():
    driver, broker = await _stuck_start(60)
    await driver.apply_setpoint(-300)
    await asyncio.sleep(0.1)
    assert _out_power(broker) == -300  # the charge kick never fired
    await driver.close()

    driver, broker = await _stuck_start(60)
    await asyncio.sleep(0.05)
    assert _out_power(broker) == 100
    assert await driver.standby()
    await asyncio.sleep(0.1)
    assert _out_power(broker) == 0  # no restore after the stop
    await driver.close()

    driver, broker = await _stuck_start(60)
    await driver.close()
    sent = len(broker.published)
    await asyncio.sleep(0.1)
    assert len(broker.published) == sent
