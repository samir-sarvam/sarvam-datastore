from datetime import datetime, timedelta
from typing import Annotated
from zoneinfo import ZoneInfo, available_timezones
import logging

from pydantic_settings import BaseSettings
from pydantic import Field


logger = logging.getLogger(__name__)

# TODO: solve issue on windows, where "Asia/Kolkata seems not to be available"
DEFAULT_TIME_ZONE = "Asia/Kolkata"


def set_default_time_zone():
    global DEFAULT_TIME_ZONE
    if DEFAULT_TIME_ZONE in available_timezones():
        return

    logger.warning(f"ZoneInfo {DEFAULT_TIME_ZONE} not found.")

    # try calcutta
    ALT_DEFAULT_TIME_ZONE = "Asia/Calcutta"
    if ALT_DEFAULT_TIME_ZONE in available_timezones():
        DEFAULT_TIME_ZONE = ALT_DEFAULT_TIME_ZONE
        logger.warning(f"Using default time zone of {ALT_DEFAULT_TIME_ZONE}")
        return

    default_time_delta = timedelta(hours=5, minutes=30)
    # try for any timezone not with 5 hours, 30 minutes offset
    for tz_name in available_timezones():
        tz = ZoneInfo(tz_name)
        if tz.utcoffset(datetime.now()) == default_time_delta:
            DEFAULT_TIME_ZONE = tz_name

    logger.error(
        (
            f"No timezone with {default_time_delta} offset found."
            " - you MUST set the LOCAL_TIME_ZONE environment variable."
        )
    )


set_default_time_zone()


class ModelSettings(BaseSettings):
    """Use environment variable LOCAL_TIME_ZONE, to capture current time zone"""

    local_time_zone: Annotated[str, Field(env="LOCAL_TIME_ZONE")] = DEFAULT_TIME_ZONE


model_settings = ModelSettings()

local_tz = ZoneInfo(model_settings.local_time_zone)
"""local_tz contains the current time zone"""


def local_time_zone():
    """local_time_zone returns the local_time_zone"""
    return local_tz
