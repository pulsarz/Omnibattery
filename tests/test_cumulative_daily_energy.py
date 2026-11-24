"""Daily energy derived from lifetime hardware counters."""
from __future__ import annotations

from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from homeassistant.core import State
from homeassistant.util import dt as dt_util

from custom_components.omnibattery.sensors.calculated_sensors import (
    CumulativeDailyEnergySensor,
    SyntheticEnergySensor,
    _CumulativeDailyEnergyData,
    _highest_daily_energy_value,
    _legacy_daily_energy_value,
)


def test_accumulates_counter_deltas_within_same_day():
    state = _CumulativeDailyEnergyData(0.0, None, "2026-07-20")

    state.update(491.0, "2026-07-20")
    state.update(491.4, "2026-07-20")
    state.update(492.1, "2026-07-20")

    assert state.kwh == pytest.approx(1.1)
    assert state.last_total == 492.1


def test_roundtrip_restores_value_and_baseline():
    original = _CumulativeDailyEnergyData(1.7, 492.7, "2026-07-20")
    restored = _CumulativeDailyEnergyData.from_dict(original.as_dict())

    assert restored == original
    restored.update(493.0, "2026-07-20")
    assert restored.kwh == pytest.approx(2.0)


def test_first_sample_after_midnight_starts_new_day():
    state = _CumulativeDailyEnergyData(2.4, 492.4, "2026-07-20")

    state.update(492.6, "2026-07-21")

    assert state.kwh == 0.0
    assert state.last_total == 492.6
    assert state.reset_date == "2026-07-21"


def test_counter_reset_preserves_daily_value_and_rebases():
    state = _CumulativeDailyEnergyData(1.4, 492.4, "2026-07-20")

    state.update(0.0, "2026-07-20")
    assert state.kwh == 1.4
    state.update(0.3, "2026-07-20")

    assert state.kwh == pytest.approx(1.7)
    assert state.last_total == 0.3


@pytest.mark.parametrize(
    "payload",
    (
        {},
        {"kwh": "unavailable", "last_total": 10, "reset_date": "2026-07-20"},
        {"kwh": 1, "last_total": "bad", "reset_date": "2026-07-20"},
    ),
)
def test_malformed_restore_payload_is_rejected(payload):
    assert _CumulativeDailyEnergyData.from_dict(payload) is None


def test_legacy_daily_value_preserves_current_day_sensor_state():
    now = dt_util.now()
    state = State("sensor.battery_daily_charge", "1.7", last_updated=now)

    assert _legacy_daily_energy_value(state, now.date().isoformat()) == 1.7


def test_legacy_daily_value_rejects_a_previous_day_state():
    yesterday = dt_util.now() - timedelta(days=1)
    state = State("sensor.battery_daily_charge", "1.7", last_updated=yesterday)

    assert _legacy_daily_energy_value(state, dt_util.now().date().isoformat()) is None


def test_recorder_recovery_uses_highest_value_after_a_partial_new_total():
    now = dt_util.now()
    states = [
        State("sensor.battery_daily_charge", "1.7", last_updated=now),
        State("sensor.battery_daily_charge", "0.2", last_updated=now),
        State("sensor.battery_daily_charge", "unavailable", last_updated=now),
    ]

    assert _highest_daily_energy_value(states) == 1.7


def test_cumulative_sensor_ignores_callbacks_until_restore_finishes():
    sensor = CumulativeDailyEnergySensor.__new__(CumulativeDailyEnergySensor)
    sensor._restore_complete = False
    sensor._accumulate = Mock()
    sensor._publish_daily = Mock()

    sensor._handle_coordinator_update()

    sensor._accumulate.assert_not_called()
    sensor._publish_daily.assert_not_called()


def test_synthetic_sensor_ignores_callbacks_until_restore_finishes():
    sensor = SyntheticEnergySensor.__new__(SyntheticEnergySensor)
    sensor._restore_complete = False
    sensor._accumulate = Mock()
    sensor._publish_total = Mock()

    sensor._handle_coordinator_update()

    sensor._accumulate.assert_not_called()
    sensor._publish_total.assert_not_called()


def test_daily_sources_publish_value_and_reset_date_metadata():
    cumulative = CumulativeDailyEnergySensor.__new__(CumulativeDailyEnergySensor)
    cumulative.coordinator = SimpleNamespace(data={})
    cumulative._key = "total_daily_charging_energy"
    cumulative._energy_data = _CumulativeDailyEnergyData(
        1.7, 492.7, "2026-07-28"
    )

    cumulative._publish_daily()

    assert cumulative.coordinator.data == {
        "total_daily_charging_energy": 1.7,
        "total_daily_charging_energy_reset_date": "2026-07-28",
    }

    synthetic = SyntheticEnergySensor.__new__(SyntheticEnergySensor)
    synthetic.coordinator = SimpleNamespace(data={})
    synthetic._key = "total_daily_discharging_energy"
    synthetic._kwh = 0.8
    synthetic._daily = True
    synthetic._reset_date = date(2026, 7, 28)

    synthetic._publish_total()

    assert synthetic.coordinator.data == {
        "total_daily_discharging_energy": 0.8,
        "total_daily_discharging_energy_reset_date": "2026-07-28",
    }


def test_synthetic_charge_skips_pack_self_heating(monkeypatch):
    """Zendure packs that heat themselves draw the heater through the pack
    input; that energy is not stored, so it must not count as charge."""
    import custom_components.omnibattery.sensors.calculated_sensors as cs

    clock = iter([100.0, 110.0, 120.0])
    monkeypatch.setattr(cs.time, "monotonic", lambda: next(clock))
    sensor = SyntheticEnergySensor.__new__(SyntheticEnergySensor)
    sensor._direction = "charge"
    sensor._daily = False
    sensor._kwh = 0.0
    sensor._last_mono = None
    sensor.coordinator = SimpleNamespace(data={"battery_power": 360, "battery_heating": True})

    sensor._accumulate()  # first sample only sets the clock
    sensor._accumulate()  # heating: 10 s at 360 W not counted
    assert sensor._kwh == 0.0
    sensor.coordinator.data = {"battery_power": 360, "battery_heating": False}
    sensor._accumulate()  # 10 s at 360 W = 0.001 kWh
    assert sensor._kwh == pytest.approx(0.001)
