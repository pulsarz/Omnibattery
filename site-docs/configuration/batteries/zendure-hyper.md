# Zendure Hyper 2000 (MQTT)

The Hyper 2000 has no local HTTP API, so Omnibattery talks to it over MQTT on a broker in your own network. You can use the broker Home Assistant's MQTT integration already uses, or point Omnibattery directly at another one, such as a Mosquitto container.

## Do I need it?

| Model | Maximum AC charge | Maximum AC discharge | Solar input visible to Omnibattery |
|---|---:|---:|---|
| Hyper 2000 | 1,200 W | 1,200 W (or the lower `inverseMaxPower` the device reports) | Total PV plus both MPPT inputs |

**Use it if** your Hyper 2000 publishes to a broker you control. For the SolarFlow 800 / 1600 / 2400 / 3000 / 4000 series, use the [local HTTP route](zendure.md) instead.

## Before you start

1. **Move the Hyper to your broker.** Out of the box it only talks to Zendure's cloud. Redirect it once over Bluetooth, for example with *solarflow-bt-manager* or the local-MQTT option of the Zendure Home Assistant integration. Afterwards it publishes to topics such as `/ja72U0ha/<deviceId>/properties/report`.
2. **Check the traffic.** In MQTT Explorer (or `mosquitto_sub -t '/+/+/properties/report' -v`) you should see JSON reports from the Hyper every few seconds. The second part of the topic is the **product key**, the third the **device ID**.
3. **Disable HEMS / smart CT mode** in the Zendure app, so it does not fight Omnibattery's setpoints.

!!! note "Mosquitto in Docker"
    Both routes work with your own container. If Home Assistant's MQTT integration already connects to it, choose **Home Assistant MQTT broker**. Otherwise choose **Own broker** and enter the container host's IP address and published port (usually `1883`), plus the user and password from your Mosquitto `password_file`, if you use one. `localhost` only works when Home Assistant runs in the host network.

## How to add it

1. In the Omnibattery setup flow, choose **Zendure Hyper 2000 (MQTT)**.
2. Enter a **Name** and leave **Device ID** empty: Omnibattery listens for 30 seconds and finds every Zendure on the broker. With one new device it is taken straight away; with several you pick from a list showing each device's ID, packs, capacity and charge level. Hypers you have already added are left out, so the second of two stacks is picked automatically.
3. Choose the **MQTT broker** and, for your own broker, its host, port and credentials.
4. Keep **Control method** at **Automation** unless your firmware ignores it (see below).
5. Set the power and state-of-charge limits. There is no capacity to enter: Omnibattery reads it from the packs.

Omnibattery asks the Hyper for a full report while you submit the form; setup only continues once it answers.

### Capacity is read from the packs

Each pack's serial number tells its model, and with it the nominal capacity: AB1000 / AB1000S 0.96 kWh, AB2000 / AB2000S / AB2000X 1.92 kWh, AB3000 2.88 kWh. A Hyper with 3 × AB2000 therefore reports **5.76 kWh**, shown as **Battery Total Energy**. An added pack is picked up live; after removing one, reload the integration so it drops out of the total. Only if a pack's serial is not recognised does the setup ask for the capacity, which is then used instead.

### Finding a device ID

If discovery misses a Hyper, read its ID from the broker. In Home Assistant open **Settings → Devices & services → MQTT → Configure**, enter `/+/+/#` under **Listen to a topic** and start listening. Every Hyper message arrives on `/<productKey>/<deviceId>/...`; the third part is the ID. An idle Hyper may take a while to say anything; restarting it makes it publish at once.

## What you will see

The same controls and readings as other Zendure batteries: automatic control, software **Force Mode** / **Set Charge Power** / **Set Discharge Power** under **Battery Manual Control**, **Target SOC**, **Minimum SOC** and **Max Inverter Output**, SOC, battery power, solar power (total and per MPPT input), temperature, and per-pack SOC, voltages and temperature. Charged and discharged energy is integrated from power, because the Hyper reports no energy counters.

## If it does not work

| Symptom | Likely cause | What to check |
|---|---|---|
| "Cannot connect to the MQTT broker" | Wrong host, port or credentials, or the container port is not published | Connect with MQTT Explorer from another machine using the same values |
| "Only Zendure devices that are already configured were heard" | The other Hyper was idle and sent nothing during the 30 s window, or it is not redirected to this broker | Enter its device ID (see below), or restart that Hyper and submit again; it talks right after reconnecting |
| "No Zendure device reported on this broker" | The Hyper still talks to Zendure's cloud | Repeat the Bluetooth redirect; check the topics with MQTT Explorer |
| "The device did not answer" | Wrong device ID, or the broker forwards reports but not commands | Copy the ID from the topic; a bridge must forward `iot/<productKey>/<deviceId>/#` **to** the device |
| Power changes briefly and returns to the app's value | HEMS / smart CT mode is enabled | Disable it in the Zendure app |
| Setpoints are ignored | Firmware without automation commands | Reconfigure the battery with **Control method: Properties** |
| The battery goes unavailable after a while | The Hyper stopped reporting for two minutes | Check its Wi-Fi and the broker; it recovers on its own once reports resume |

??? "Advanced details"
    **Topics.** Reports arrive on `/<productKey>/<deviceId>/properties/report` (deltas). Omnibattery requests a full dump every 30 seconds on `iot/<productKey>/<deviceId>/properties/read`, answers the device's `time-sync` request, and writes configuration on `.../properties/write`. Known Hyper 2000 product keys are `ja72U0ha` and `gDa3tb`; any other key is learned from the topic the device answers on.

    **Control methods.** *Automation* sends the `deviceAutomation` function (`autoModel` 8) on `.../function/invoke`, the same command the Zendure app's smart mode uses; it lives in RAM and is refreshed every minute. *Properties* writes `acMode` with `inputLimit` / `outputLimit` and `smartMode=1`, like the HTTP driver. Configuration changes (SOC limits, inverter output) are written as properties in both modes.

    **Zero powers.** The Hyper leaves zero-valued powers out of its reports. A power that has not appeared for about 75 seconds is read as 0, and charging/discharging directions are mutually exclusive, so a report naming one clears the other.

    **Units.** `socSet` and `minSoc` are per-mille, temperatures are deci-Kelvin, cell voltages are centivolts; all are converted.

    For shared runtime controls and system limits, see [Choose your battery connection](index.md).
