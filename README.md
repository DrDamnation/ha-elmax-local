# Elmax MQTT (Local) - Home Assistant Integration

[![hacs_badge](https://img.shields.io/badge/HACS-Custom-41BDF5.svg)](https://github.com/hacs/integration)
[![GitHub Release](https://img.shields.io/github/v/release/DrDamnation/ha-elmax-mqtt)](https://github.com/DrDamnation/ha-elmax-mqtt/releases)
[![License](https://img.shields.io/github/license/DrDamnation/ha-elmax-mqtt)](LICENSE)

A **local-only** Home Assistant custom integration for **Elmax** alarm panels (Phantom64 and compatible) using a **hybrid MQTT + HTTP** architecture — no cloud connection required.

## Why This Integration?

The official Elmax integration for Home Assistant relies on the Elmax Cloud API, which means:
- Your alarm data travels through the internet
- If the cloud is down, you lose control
- Latency depends on your internet connection

**Elmax MQTT (Local)** uses a hybrid approach for maximum reliability:
- **MQTT** for real-time status monitoring (zones, areas, outputs, scenarios)
- **HTTP local API** for sending commands (arm, disarm, toggle outputs, trigger scenarios)
- **Automatic failover** — if MQTT goes down, falls back to HTTP polling; auto-recovers when MQTT is back
- **Zero cloud dependency** — works even without internet
- **Instant response** — commands execute in milliseconds over LAN
- **Full local control** — your security data stays in your network

## Requirements

- **Elmax Phantom64** alarm panel (or compatible model with MQTT support)
- **MQTT Broker** (e.g., Mosquitto) running on your local network
- **Home Assistant** with the [MQTT integration](https://www.home-assistant.io/integrations/mqtt/) configured and connected to the same broker
- The Elmax panel must be configured to connect to your MQTT broker (via the Elmax installer app)

## Installation

### HACS (Recommended)

1. Open HACS in Home Assistant
2. Click the three dots in the top right corner and select **Custom repositories**
3. Add `https://github.com/DrDamnation/ha-elmax-mqtt` with category **Integration**
4. Search for "Elmax MQTT" and install it
5. Restart Home Assistant

### Manual

1. Download the latest release from the [Releases page](https://github.com/DrDamnation/ha-elmax-mqtt/releases)
2. Copy the `custom_components/elmax_mqtt` folder to your `config/custom_components/` directory
3. Restart Home Assistant

## Configuration

1. Go to **Settings** > **Devices & Services** > **Add Integration**
2. Search for **Elmax MQTT (Local)**
3. The integration will automatically discover your panel via MQTT
4. Select your panel, enter the **PIN code** and the panel's **local IP address**
5. Done! All entities will be created automatically

## Supported Entities

### Alarm Control Panel
One entity per **area** configured on your panel. Supports:
- **Arm Away** — full perimeter + interior protection
- **Arm Home** — perimeter only
- **Arm Night** — night mode (perimeter + selected interior zones)
- **Disarm** — deactivate the area

### Binary Sensors
One entity per **zone** (sensor) on your panel:
- Doors, windows, motion detectors, radar sensors
- Device class is automatically inferred from the zone name
- Extra attributes: `esclusa` (bypassed), `indice` (zone index)

### Switches
One entity per **output** on your panel:
- Control electric locks, gates, shutters, and other connected outputs
- Toggle on/off

### Buttons
One entity per **scenario** configured on your panel:
- Activate predefined scenarios (e.g., "Leaving Home", "Night Mode", "Arriving")
- Press to execute

## How It Works

```
                    ┌─────────────┐
                    │ Elmax Panel  │
                    │ (local LAN)  │
                    └──┬────────┬──┘
          MQTT (status)│        │HTTP (commands)
                       │        │
                    ┌──┴────────┴──┐
                    │  MQTT Broker  │
                    │  (Mosquitto)  │
                    └──────┬───────┘
                           │
                    ┌──────┴───────┐
                    │Home Assistant │
                    │ (elmax_mqtt)  │
                    └──────────────┘
```

The integration uses a **hybrid architecture**:

### Status (MQTT primary, HTTP fallback)
1. **Discovery** — Publishes to `/elmax/request/id` and listens for panel responses
2. **Authentication** — Sends panel PIN to `/elmax/request/login/{panel_id}`, receives a JWT token
3. **Status Polling** — Periodically requests full panel status via MQTT (every 5s by default)
4. **Automatic Failover** — If MQTT fails 3 consecutive times, switches to HTTP polling (`GET /api/v2/discovery`)
5. **Auto-Recovery** — Periodically probes MQTT while in HTTP fallback; switches back when MQTT recovers

### Commands (HTTP only)
6. **Arm/Disarm** — `POST https://{panel_ip}/api/v2/cmd/{endpoint_id}/{command}`
7. **Toggle Outputs** — `POST https://{panel_ip}/api/v2/cmd/{endpoint_id}/on|off`
8. **Trigger Scenarios** — `POST https://{panel_ip}/api/v2/cmd/{endpoint_id}/on`
9. **Auto-retry** — Retries on HTTP 401 (re-login) and 422 (panel busy)

All communication happens locally on your network.

## MQTT Topics

| Direction | Topic | Description |
|-----------|-------|-------------|
| Request | `/elmax/request/id` | Discover panels on the broker |
| Response | `/elmax/response/id` | Panel announces itself |
| Request | `/elmax/request/login/{panel_id}` | Authenticate with PIN |
| Response | `/elmax/response/login/{panel_id}` | JWT token response |
| Request | `/elmax/request/status/{panel_id}` | Request full panel status |
| Response | `/elmax/response/status/{panel_id}` | Zones, areas, outputs, scenarios |

## Entity Naming

Entities are named using the zone/area/output names as configured on your Elmax panel. For example:
- `alarm_control_panel.elmax_XXXXXX_area_name`
- `binary_sensor.elmax_XXXXXX_zone_name`
- `switch.elmax_XXXXXX_output_name`
- `button.elmax_XXXXXX_scenario_name`

Where `XXXXXX` is the last 6 characters of your panel ID. You can rename entities in the Home Assistant UI.

## Troubleshooting

### Panel not discovered
- Verify the Elmax panel is connected to the same MQTT broker as Home Assistant
- Check the Mosquitto logs for the client `elmax-api-server-*`
- Ensure the MQTT broker allows authentication with the credentials configured on the panel

### Cannot connect (wrong PIN)
- Double-check the PIN code — it must match the one configured on the panel
- The panel responds on `/elmax/response/login/{panel_id}` — check for error messages in the MQTT logs

### Entities show "unavailable"
- The panel may have disconnected from the MQTT broker
- The integration will automatically fall back to HTTP polling and reconnect when possible

### Token errors (401)
- The JWT token has expired — the integration handles this automatically with re-login
- If persistent, restart the integration from Settings > Devices & Services

## Compatibility

| Panel | Tested | Notes |
|-------|--------|-------|
| Phantom64 PRO GSM | Yes | Full support |
| Other Elmax panels | Unknown | Should work if MQTT is supported |

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Credits

- **Elmax S.r.l.** — for the Phantom64 alarm panel hardware
- **Home Assistant** — for the amazing home automation platform
- Built with the help of [Claude Code](https://claude.ai/claude-code)
