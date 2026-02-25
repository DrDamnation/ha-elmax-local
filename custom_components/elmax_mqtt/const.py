"""Constants for Elmax MQTT integration."""

DOMAIN = "elmax_mqtt"

CONF_PANEL_PIN = "panel_pin"
CONF_PANEL_ID = "panel_id"
CONF_PANEL_HOST = "panel_host"
CONF_SCAN_INTERVAL = "scan_interval"
DEFAULT_SCAN_INTERVAL = 5

# MQTT Topics (for status reading)
TOPIC_REQUEST_ID = "/elmax/request/id"
TOPIC_REQUEST_LOGIN = "/elmax/request/login/{panel_id}"
TOPIC_REQUEST_STATUS = "/elmax/request/status/{panel_id}"
TOPIC_REQUEST_REFRESH = "/elmax/request/refresh/{panel_id}"

TOPIC_RESPONSE_ID = "/elmax/response/id"
TOPIC_RESPONSE_LOGIN = "/elmax/response/login/{panel_id}"
TOPIC_RESPONSE_STATUS = "/elmax/response/status/{panel_id}"

# HTTP API (for commands)
HTTP_BASE_URL = "https://{host}/api/v2"
HTTP_CMD_PATH = "/cmd/{endpoint_id}/{command}"

# HTTP Area commands
HTTP_CMD_ARM_TOTALLY = "4"
HTTP_CMD_ARM_P1_P2 = "3"
HTTP_CMD_ARM_P2 = "2"
HTTP_CMD_ARM_P1 = "1"
HTTP_CMD_DISARM = "off"
HTTP_CMD_ON = "on"
HTTP_CMD_OFF = "off"
HTTP_CMD_TRIGGER = "on"

# Dispatcher signal
SIGNAL_UPDATE = f"{DOMAIN}_update_{{panel_id}}"

# Elmax alarm states (from panel status)
ELMAX_TO_HA_STATE = {
    0: "disarmed",
    1: "armed_home",      # P1
    2: "armed_night",     # P2
    3: "armed_home",      # P1+P2
    4: "armed_away",      # Totally
}

HA_TO_ELMAX_CMD = {
    "disarm": HTTP_CMD_DISARM,
    "arm_away": HTTP_CMD_ARM_TOTALLY,
    "arm_home": HTTP_CMD_ARM_P1_P2,
    "arm_night": HTTP_CMD_ARM_P2,
}

PLATFORMS = ["alarm_control_panel", "binary_sensor", "switch", "button"]
