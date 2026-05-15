"""Constants for Elmax Local integration."""
from __future__ import annotations

from typing import Final

DOMAIN: Final = "elmax_local"
LEGACY_DOMAIN: Final = "elmax_mqtt"

PLATFORMS: Final = ["alarm_control_panel", "binary_sensor", "switch", "button"]

CONF_PANEL_ID: Final = "panel_id"
CONF_PANEL_PIN: Final = "panel_pin"
CONF_PANEL_HOST: Final = "panel_host"
CONF_ENABLE_WS: Final = "enable_ws"
CONF_ENABLE_MQTT: Final = "enable_mqtt"
CONF_RECONCILE_INTERVAL: Final = "reconcile_interval"

DEFAULT_RECONCILE_INTERVAL: Final = 90
MIN_RECONCILE_INTERVAL: Final = 30
MAX_RECONCILE_INTERVAL: Final = 600

TOPIC_REQUEST_ID: Final = "/elmax/request/id"
TOPIC_RESPONSE_ID: Final = "/elmax/response/id"
TOPIC_REQUEST_LOGIN: Final = "/elmax/request/login/{panel_id}"
TOPIC_RESPONSE_LOGIN: Final = "/elmax/response/login/{panel_id}"
TOPIC_REQUEST_REFRESH: Final = "/elmax/request/refresh/{panel_id}"
TOPIC_RESPONSE_REFRESH: Final = "/elmax/response/refresh/{panel_id}"
TOPIC_REQUEST_STATUS: Final = "/elmax/request/status/{panel_id}"
TOPIC_RESPONSE_STATUS: Final = "/elmax/response/status/{panel_id}"
TOPIC_REQUEST_COMMAND: Final = "/elmax/request/command/{panel_id}"
TOPIC_RESPONSE_COMMAND: Final = "/elmax/response/command/{panel_id}"

HTTP_BASE_URL: Final = "https://{host}/api/v2"
WS_BASE_URL: Final = "wss://{host}/api/v2/push"

CMD_AREA_DISARM: Final = "0"
CMD_AREA_ARM_P1: Final = "1"
CMD_AREA_ARM_P2: Final = "2"
CMD_AREA_ARM_P1P2: Final = "3"
CMD_AREA_ARM_TOTAL: Final = "4"

CMD_OUTPUT_TOGGLE: Final = "0"
CMD_OUTPUT_ON: Final = "1"
CMD_OUTPUT_OFF: Final = "2"

ELMAX_TO_HA_STATE: Final = {
    0: "disarmed", 1: "armed_home", 2: "armed_night",
    3: "armed_home", 4: "armed_away",
}

HA_TO_ELMAX_CMD: Final = {
    "disarm": CMD_AREA_DISARM,
    "arm_away": CMD_AREA_ARM_TOTAL,
    "arm_home": CMD_AREA_ARM_P1P2,
    "arm_night": CMD_AREA_ARM_P2,
}

SERVICE_MIGRATE: Final = "migrate_from_legacy"
SERVICE_ROLLBACK: Final = "rollback_migration"
