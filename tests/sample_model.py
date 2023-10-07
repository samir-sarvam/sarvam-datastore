from datetime import datetime, timezone
from enum import IntEnum, StrEnum
from typing import Dict, List

from sarvam_datastore import GeoPoint
from pydantic import AwareDatetime, BaseModel


class StandAloneEnumInt(IntEnum):
    FIRST = 1
    SECOND = 2


class StandAloneEnumStr(StrEnum):
    FIRST = "first"
    SECOND = "second"


class EmbeddedEntity(BaseModel):
    x: float = 20.0
    y: float = 30.0


class StandAloneEntity(BaseModel):
    astr: str = "akeyvalue"
    atime: AwareDatetime = datetime.now(tz=timezone.utc)
    abool: bool = False
    afloat: float = 123.45
    anint: int = 20
    abytes: bytes = bytes("some bytes", "utf-8")
    adict: Dict[str, int] = {"one": 1, "two": 2}
    anarray: List[str] = ["one", "two"]
    ageopoint: GeoPoint = GeoPoint(latitude=12.97, longitude=77.59)
    anenumint: StandAloneEnumInt = StandAloneEnumInt.FIRST
    anenumstr: StandAloneEnumStr = StandAloneEnumStr.FIRST
    anone: str | None = None
    aunindexed: str = "unindexed"
    aembedded: EmbeddedEntity = EmbeddedEntity()
    aref: int

    class Config:
        arbitrary_types_allowed = True

    class DatastoreConfig:
        key = [("StandAlone", "astr")]
        key_references = [[("Referred", "aref")]]
        exclude_from_indexes = ["aunindexed"]


class AllocatedIdEntity(BaseModel):
    aint: int | None = None
    abool: bool = False
    astr: str | None = None

    class DatastoreConfig:
        key = [("AllocatedId", "aint")]
