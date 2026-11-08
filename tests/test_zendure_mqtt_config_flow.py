"""Setup-flow tests for the Zendure Hyper 2000 (MQTT) battery type."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from custom_components.omnibattery import config_flow as cf
from custom_components.omnibattery.config_flow import MarstekVenusConfigFlow
from custom_components.omnibattery.drivers.zendure_mqtt import ZendureMqttDriver

from .test_zendure_mqtt_driver import DEV, PK, FakeBroker

HYPER_A, HYPER_B = "HyperAAA", "HyperBBB"


def _stack(prefix):
    return [{"sn": f"CO4E{prefix}{i}", "socLevel": 50} for i in range(3)]


class TwoHypers(FakeBroker):
    """Two Hyper 2000s with 3 × AB2000X each, both chatting on the broker."""

    def __init__(self):
        super().__init__(reply_with=None)
        self.reports = {
            HYPER_A: {"properties": {"electricLevel": 41}, "packData": _stack("A")},
            HYPER_B: {"properties": {"electricLevel": 77}, "packData": _stack("B")},
        }

    async def subscribe(self, topic, handler):
        unsubscribe = await super().subscribe(topic, handler)
        if topic == "/+/+/#":
            for device, report in self.reports.items():
                asyncio.get_running_loop().call_soon(
                    self.deliver, f"/{PK}/{device}/properties/report", report
                )
        return unsubscribe

    async def publish(self, topic, payload):
        await super().publish(topic, payload)
        _, pk, device, *_ = topic.split("/")
        if topic.endswith("/properties/read") and pk == PK and device in self.reports:
            self.deliver(f"/{PK}/{device}/properties/report", self.reports[device])


def _flow(*, mqtt_configured=True):
    flow = MarstekVenusConfigFlow()
    flow.config_data = {"num_batteries": 1}
    flow.hass = SimpleNamespace(
        config_entries=SimpleNamespace(
            async_entries=lambda domain: [object()] if mqtt_configured and domain == "mqtt" else []
        ),
        loop=None,
    )
    return flow


def _use_broker(monkeypatch, broker):
    built = []

    def _build(hass, broker_choice, **kwargs):
        built.append((broker_choice, kwargs))
        return broker

    monkeypatch.setattr(cf, "_build_zendure_mqtt_transport", _build)
    return built


def _fields(form):
    return {marker.schema for marker in form["data_schema"].schema}


@pytest.mark.asyncio
async def test_brand_step_offers_and_routes_hyper():
    flow = _flow()
    form = await flow.async_step_battery_brand()
    options = next(iter(form["data_schema"].schema.values())).config["options"]
    assert "zendure_mqtt" in {option["value"] for option in options}

    routed = await flow.async_step_battery_brand({"brand": "zendure_mqtt"})
    assert routed["step_id"] == "battery_connection_zendure_mqtt"
    assert _fields(routed) >= {
        "name", "device_id", "zendure_mqtt_broker", "mqtt_host", "mqtt_port",
        "username", "password", "zendure_control_method",
    }


@pytest.mark.asyncio
async def test_home_assistant_broker_with_device_id(monkeypatch):
    flow = _flow()
    broker = FakeBroker()
    built = _use_broker(monkeypatch, broker)
    flow._current_battery_data = {"brand": "zendure_mqtt"}
    result = await flow.async_step_battery_connection_zendure_mqtt({
        "name": "Hyper", "device_id": DEV, "zendure_mqtt_broker": "homeassistant",
        "mqtt_port": 1883, "zendure_control_method": "automation",
    })
    assert result["step_id"] == "battery_limits"
    assert built[0][0] == "homeassistant"
    data = flow._current_battery_data
    assert data["brand"] == "zendure"
    assert data["zendure_transport"] == "mqtt"
    assert (data["host"], data["port"], data["device_id"]) == (DEV, 0, DEV)
    assert data["zendure_product_key"] == PK
    assert data["zendure_model"] == "hyper_2000"
    assert data["username"] == "" and data["password"] == "" and data["mqtt_host"] == ""
    assert not broker.started  # released after probing
    # Capacity comes from the packs (2 × AB2000X), so it is not asked for.
    assert data["battery_capacity_kwh"] == 3.84
    assert "battery_capacity_kwh" not in _fields(result)
    # The limits step offers the Hyper's 1200 W envelope.
    charge = next(
        v for k, v in result["data_schema"].schema.items() if k.schema == "max_charge_power"
    )
    assert charge.config["max"] == 1200


@pytest.mark.asyncio
async def test_own_broker_discovers_the_device(monkeypatch):
    flow = _flow(mqtt_configured=False)
    broker = FakeBroker(reply_with=None)
    built = _use_broker(monkeypatch, broker)
    monkeypatch.setattr(cf, "_ZENDURE_MQTT_DISCOVERY_S", 0.05)

    async def _chatter():
        await asyncio.sleep(0.01)
        broker.deliver(f"/{PK}/{DEV}/properties/report", {"properties": {"electricLevel": 40}})
        broker.reply_with = {"properties": {"electricLevel": 40}}

    task = asyncio.ensure_future(_chatter())
    result = await flow.async_step_battery_connection_zendure_mqtt({
        "name": "Hyper", "zendure_mqtt_broker": "custom", "mqtt_host": " 192.168.1.10 ",
        "mqtt_port": 1884, "username": "ha", "password": "pw",
        "zendure_control_method": "properties",
    })
    await task
    assert result["step_id"] == "battery_limits"
    assert built[0] == ("custom", {
        "host": "192.168.1.10", "port": 1884, "username": "ha", "password": "pw",
    })
    data = flow._current_battery_data
    assert data["device_id"] == DEV and data["zendure_product_key"] == PK
    assert (data["mqtt_host"], data["mqtt_port"]) == ("192.168.1.10", 1884)
    assert (data["username"], data["password"]) == ("ha", "pw")
    assert data["zendure_control_method"] == "properties"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("flow_kwargs", "user_input", "broker", "error"),
    [
        ({}, {"zendure_mqtt_broker": "custom", "mqtt_host": ""}, FakeBroker(), ("mqtt_host", "mqtt_host_required")),
        ({"mqtt_configured": False}, {"zendure_mqtt_broker": "homeassistant"}, FakeBroker(), ("base", "mqtt_not_configured")),
        ({}, {"zendure_mqtt_broker": "homeassistant", "device_id": DEV}, FakeBroker(start_ok=False), ("base", "mqtt_broker_unreachable")),
    ],
)
async def test_connection_errors(monkeypatch, flow_kwargs, user_input, broker, error):
    flow = _flow(**flow_kwargs)
    _use_broker(monkeypatch, broker)
    result = await flow.async_step_battery_connection_zendure_mqtt({"name": "Hyper", **user_input})
    assert result["step_id"] == "battery_connection_zendure_mqtt"
    assert result["errors"] == {error[0]: error[1]}


@pytest.mark.asyncio
async def test_silent_device_and_ambiguous_broker(monkeypatch):
    flow = _flow()
    broker = FakeBroker(reply_with=None)
    _use_broker(monkeypatch, broker)
    monkeypatch.setattr(ZendureMqttDriver, "probe", AsyncMock(return_value=(False, {})))
    result = await flow.async_step_battery_connection_zendure_mqtt({
        "name": "Hyper", "device_id": DEV, "zendure_mqtt_broker": "homeassistant",
    })
    assert result["errors"] == {"base": "zendure_no_response"}

    monkeypatch.setattr(ZendureMqttDriver, "discover", AsyncMock(return_value={"AAA": PK}))
    flow.battery_configs = [{"brand": "zendure", "zendure_transport": "mqtt", "device_id": "AAA"}]
    result = await flow.async_step_battery_connection_zendure_mqtt({
        "name": "Hyper", "zendure_mqtt_broker": "homeassistant",
    })
    assert result["errors"] == {"base": "zendure_all_configured"}
    assert result["description_placeholders"]["devices"] == "AAA"
    flow.battery_configs = []

    monkeypatch.setattr(ZendureMqttDriver, "discover", AsyncMock(return_value={}))
    result = await flow.async_step_battery_connection_zendure_mqtt({
        "name": "Hyper", "zendure_mqtt_broker": "homeassistant",
    })
    assert result["errors"] == {"base": "zendure_no_device_found"}


def test_stored_hyper_maps_back_to_its_selector_value():
    assert cf._brand_selector_value({"brand": "zendure", "zendure_transport": "mqtt"}) == "zendure_mqtt"
    assert cf._brand_selector_value({"brand": "zendure"}) == "zendure"
    assert cf._brand_selector_value({"brand": "zendure", "zendure_transport": "http"}) == "zendure"


def test_coordinator_builds_the_mqtt_driver_for_a_hyper(monkeypatch):
    """brand zendure + transport mqtt selects the MQTT driver, not HTTP."""
    from unittest.mock import MagicMock

    from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

    from custom_components.omnibattery.drivers.zendure import ZendureLocalDriver
    from custom_components.omnibattery.drivers.zendure_mqtt import (
        HomeAssistantMqttTransport,
        PahoMqttTransport,
    )
    from custom_components.omnibattery.infra.coordinator import (
        MarstekVenusDataUpdateCoordinator,
    )

    monkeypatch.setattr(DataUpdateCoordinator, "__init__", lambda *_args, **_kwargs: None)
    hass = MagicMock()

    def build(**kwargs):
        return MarstekVenusDataUpdateCoordinator(
            hass, "Hyper", DEV, 0, "sensor.grid_power",
            brand="zendure", zendure_model="hyper_2000",
            max_charge_power=1200, max_discharge_power=1200, **kwargs,
        )

    hyper = build(zendure_transport="mqtt", zendure_product_key=PK)
    assert isinstance(hyper.driver, ZendureMqttDriver)
    assert isinstance(hyper.driver._transport, HomeAssistantMqttTransport)
    assert hyper.driver.product_key == PK
    assert hyper.device_max_charge_power == 1200
    assert hyper.battery_device_info["manufacturer"] == "Zendure"
    assert hyper.battery_device_info["model"] == "Hyper 2000"

    own = build(
        zendure_transport="mqtt", zendure_mqtt_broker="custom",
        mqtt_host="10.0.0.5", mqtt_port=1884, username="u", password="p",
        zendure_control_method="properties",
    )
    assert isinstance(own.driver._transport, PahoMqttTransport)
    assert own.driver.control_method == "properties"

    assert isinstance(build().driver, ZendureLocalDriver)


@pytest.mark.asyncio
async def test_two_hypers_are_picked_one_after_the_other(monkeypatch):
    """Battery 1 chooses among both; battery 2 gets the remaining one."""
    flow = _flow()
    flow.config_data = {"num_batteries": 2}
    _use_broker(monkeypatch, TwoHypers())
    monkeypatch.setattr(cf, "_ZENDURE_MQTT_DISCOVERY_S", 0.05)

    form = {"zendure_mqtt_broker": "homeassistant", "zendure_control_method": "automation"}
    flow._current_battery_data = {"brand": "zendure_mqtt"}
    pick = await flow.async_step_battery_connection_zendure_mqtt({"name": "Stack 1", **form})
    assert pick["step_id"] == "battery_connection_zendure_mqtt_pick"
    selector = next(iter(pick["data_schema"].schema.values()))
    options = {o["value"]: o["label"] for o in selector.config["options"]}
    assert options == {
        HYPER_A: f"{HYPER_A} — 3 × AB2000X, 5.76 kWh, 41 %",
        HYPER_B: f"{HYPER_B} — 3 × AB2000X, 5.76 kWh, 77 %",
    }

    limits = await flow.async_step_battery_connection_zendure_mqtt_pick({"device_id": HYPER_B})
    assert limits["step_id"] == "battery_limits"
    assert flow._current_battery_data["device_id"] == HYPER_B
    assert flow._current_battery_data["name"] == "Stack 1"
    assert flow._current_battery_data["battery_capacity_kwh"] == 5.76
    # Capacity came from the packs: the limits form does not ask for it.
    assert "battery_capacity_kwh" not in _fields(limits)
    await flow.async_step_battery_limits({
        "max_charge_power": 1200, "max_discharge_power": 800, "max_soc": 100,
        "min_soc": 10, "charge_hysteresis_percent": 2, "backup_offgrid_threshold": 50,
    })
    assert flow.battery_configs[0]["battery_capacity_kwh"] == 5.76

    flow._current_battery_data = {"brand": "zendure_mqtt"}
    second = await flow.async_step_battery_connection_zendure_mqtt({"name": "Stack 2", **form})
    assert second["step_id"] == "battery_limits"
    assert flow._current_battery_data["device_id"] == HYPER_A
    assert flow._current_battery_data["battery_capacity_kwh"] == 5.76


@pytest.mark.asyncio
async def test_unknown_packs_fall_back_to_asking_for_capacity(monkeypatch):
    flow = _flow()
    _use_broker(monkeypatch, FakeBroker(reply_with={
        "properties": {"electricLevel": 50}, "packData": [{"sn": "XX01"}],
    }))
    flow._current_battery_data = {"brand": "zendure_mqtt"}
    result = await flow.async_step_battery_connection_zendure_mqtt({
        "name": "Hyper", "device_id": DEV, "zendure_mqtt_broker": "homeassistant",
    })
    assert result["step_id"] == "battery_limits"
    assert "battery_capacity_kwh" not in flow._current_battery_data
    assert "battery_capacity_kwh" in _fields(result)


@pytest.mark.asyncio
async def test_idle_hyper_without_pack_list_is_not_asked_for_capacity(monkeypatch):
    """The pack list can trail the reply; the driver reads it later."""
    flow = _flow()
    _use_broker(monkeypatch, FakeBroker(reply_with={"properties": {"electricLevel": 50}}))
    monkeypatch.setattr(ZendureMqttDriver, "probe", AsyncMock(
        return_value=(True, {"product_key": PK, "battery_soc": 50})
    ))
    flow._current_battery_data = {"brand": "zendure_mqtt"}
    result = await flow.async_step_battery_connection_zendure_mqtt({
        "name": "Hyper", "device_id": DEV, "zendure_mqtt_broker": "homeassistant",
    })
    assert result["step_id"] == "battery_limits"
    assert "battery_capacity_kwh" not in _fields(result)
    await flow.async_step_battery_limits({
        "max_charge_power": 1200, "max_discharge_power": 1200, "max_soc": 100,
        "min_soc": 10, "charge_hysteresis_percent": 2, "backup_offgrid_threshold": 50,
    })
    assert flow.battery_configs[0]["battery_capacity_kwh"] == 0.0
