from datetime import date, datetime, time, timedelta, timezone
from enum import IntEnum, StrEnum
from typing import Any, Dict
import logging
from pydantic import BaseModel, NaiveDatetime, AwareDatetime
from sarvam_datastore import (
    DatastoreModelHelper,
    DatastoreModelHelperRegistry,
    DatastoreProperty,
    AtomicProperty,
    GeoPoint,
    EntityProperty,
    GenericType,
)
from faker import Faker
from faker_enum import EnumProvider

logger = logging.getLogger(__name__)

faker = Faker()
faker.add_provider(EnumProvider)


class SampleIntEnum(IntEnum):
    A = 1
    B = 2
    C = 3


class SampleStrEnum(StrEnum):
    A = "a"
    B = "b"
    C = "c"


class SampleEmbedded(BaseModel):
    a: int
    b: str


class DatamodelHelperMock:
    def __init__(self, registry: DatastoreModelHelperRegistry) -> None:
        self.registry = registry

    def generate(self, helper: DatastoreModelHelper) -> Any:
        if hasattr(helper.cls, "model_construct"):
            obj = helper.cls.model_construct()
        else:
            obj = helper.cls()
        obj = helper.cls.model_construct()
        if helper.key is not None:
            for path_item in helper.key.path_items:
                setattr(
                    obj,
                    path_item.field_name,
                    faker.pyint() if path_item.field_type == int else faker.pystr(),
                )

        for datastore_field_name, property in helper.properties.items():
            if property.generic_type == GenericType.LIST:
                setattr(obj, property.field_name, self.generate_list(property))
            elif property.generic_type == GenericType.DICT:
                setattr(obj, property.field_name, self.generate_dict(property))
            elif isinstance(property, AtomicProperty):
                setattr(
                    obj,
                    property.field_name,
                    self.generate_atomic(property),
                )
            elif isinstance(property, EntityProperty):
                setattr(
                    obj,
                    property.field_name,
                    self.generate_entity(property),
                )

        return obj

    def generate_dict(self, property_def: DatastoreProperty):
        len = faker.pyint(min_value=0, max_value=10)
        if isinstance(property_def, AtomicProperty):
            return {
                faker.pystr(): self.generate_atomic(property_def) for _ in range(len)
            }
        elif isinstance(property_def, EntityProperty):
            return {
                faker.pystr(): self.generate_entity(property_def) for _ in range(len)
            }

    def generate_list(self, property_def: DatastoreProperty):
        len = faker.pyint(min_value=0, max_value=10)
        if isinstance(property_def, AtomicProperty):
            return [self.generate_atomic(property_def) for _ in range(len)]
        elif isinstance(property_def, EntityProperty):
            return [self.generate_entity(property_def) for _ in range(len)]

    def generate_atomic(self, property_def: AtomicProperty) -> Any:
        if property_def.is_optional:
            if faker.pybool():
                return None

        prop_type = property_def.field_type
        if prop_type == int:
            return faker.pyint()
        elif prop_type == IntEnum:
            return faker.enum(property_def.enum_class)
        elif prop_type == str:
            return faker.pystr()
        elif prop_type == StrEnum:
            return faker.enum(property_def.enum_class)
        elif prop_type == float:
            return faker.pyfloat()
        elif prop_type == bool:
            return faker.pybool()
        elif prop_type == GeoPoint:
            return GeoPoint(latitude=faker.pyfloat(), longitude=faker.pyfloat())
        elif prop_type == bytes:
            return faker.binary(length=faker.pyint())
        elif prop_type == datetime:
            return faker.date_time(tzinfo=timezone.utc)
        elif prop_type == date:
            return faker.date_this_decade()
        elif prop_type == time:
            return faker.date_time(tzinfo=timezone.utc).timetz()
        elif prop_type == time:
            return faker.date_time(tzinfo=timezone.utc).timetz()
        elif prop_type == timedelta:
            return faker.date_time() - faker.date_time()
        elif prop_type == NaiveDatetime:
            return faker.date_time()
        elif prop_type == AwareDatetime:
            return faker.date_time(tzinfo=timezone.utc)
        else:
            raise NotImplementedError(f"Unsupported type {prop_type}")

    def generate_entity(self, property_def: EntityProperty) -> Any:
        if property_def.entity_type == "entity":
            return self.generate_class_entity(property_def)
        elif property_def.entity_type == "dict":
            return self.generate_dict_entity(property_def)

    def generate_class_entity(self, property_def: EntityProperty) -> Any:
        if property_def.clazz is not None:
            helper = self.registry.get_by_class(property_def.clazz)
            if helper is None:
                raise ValueError(f"Helper not found for {property_def.clazz}")
            return self.generate(helper)
        else:
            raise ValueError(f"No class for property {property_def.field_name}")

    def generate_dict_entity(self, property_def: EntityProperty) -> Any:
        dict_val: Dict[str, Any] = {}
        cEntries = faker.pyint(min_value=0, max_value=10)
        for i in range(0, cEntries):
            key = faker.pystr()
            val = faker.pyint()
            dict_val[key] = val

        return dict_val
