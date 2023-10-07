from datetime import date, datetime, time, timedelta, timezone
from enum import IntEnum, StrEnum
from typing import Any
from pydantic import NaiveDatetime, AwareDatetime
from ._model_helper import (
    DatastoreModelHelper,
    DatastoreProperty,
    AtomicProperty,
    EntityProperty,
    ReferenceProperty,
    DatastoreModelKey,
    GeoPoint,
    ATOMIC_TYPE_TO_DATASTORE_TYPE,
    GenericType,
)
from google.cloud.datastore_v1.types import (
    Entity as Entitypb,
    Key as Keypb,
    Value as Valuepb,
)
from google.type import latlng_pb2
from google.cloud.datastore_v1.types import entity as entity_pb2
from google.protobuf import struct_pb2
from google.protobuf.timestamp_pb2 import Timestamp as Timestamppb
from proto.datetime_helpers import DatetimeWithNanoseconds
from ._model_registry import DatastoreModelHelperRegistry
import logging

logger = logging.getLogger(__name__)

UTC_EPOCH = datetime.fromtimestamp(0, tz=timezone.utc)


class EntityProtobufConverterException(Exception):
    pass


class EntityProtobufConverter:
    def __init__(self, registry: DatastoreModelHelperRegistry) -> None:
        self.registry = registry

    def _get_helper_from_entity_pb(self, entity_pb: Entitypb):
        if entity_pb.key is None:
            raise EntityProtobufConverterException("from_protobuf: Entity has no key")

        if len(entity_pb.key.path) == 0:
            raise EntityProtobufConverterException(
                "from_protobuf: Entity key has no path"
            )

        kind = entity_pb.key.path[0].kind
        helper = self.registry.get_by_kind(kind)
        if helper is None:
            raise EntityProtobufConverterException(
                f"Model helper for kind {kind} not found"
            )
        return helper

    def _get_helper(
        self, entity_pb: Entitypb, clazz: type | None = None
    ) -> DatastoreModelHelper:
        if clazz is not None:
            return self.registry.get_by_class(clazz)
        else:
            return self._get_helper_from_entity_pb(entity_pb)

    def to_protobuf(
        self,
        obj: Any,
        project: str = "",
        namespace: str = "",
        *,
        entity_property: EntityProperty | None = None,
    ) -> Entitypb:
        clazz = type(obj)
        helper = self.registry.get_by_class(clazz)
        if helper is None:
            raise EntityProtobufConverterException(
                f"Model helper for class {clazz} not found"
            )

        entity_pb = Entitypb()
        if helper.key is not None:
            key_pb = self.to_protobuf_key(
                obj,
                helper.key,
                project_id=project,
                namespace_id=namespace,
            )
            entity_pb._pb.key.CopyFrom(key_pb._pb)

        for datastore_property_name, property in helper.properties.items():
            value_pb = entity_pb.properties._pb.get_or_create(  # type: ignore
                datastore_property_name
            )
            value = getattr(obj, property.field_name, None)
            if value is None:
                self.to_protobuf_null_value(value_pb, property)
            elif property.generic_type == GenericType.LIST:
                self.to_protobuf_list(value_pb, value, property)
            elif property.generic_type == GenericType.DICT:
                self.to_protobuf_dict(value_pb, value, property)
            elif isinstance(property, AtomicProperty):
                self.to_protobuf_atomic(value_pb, value, property)
            elif isinstance(property, EntityProperty):
                self.to_protobuf_entity(value_pb, value, property)
            elif isinstance(property, ReferenceProperty):
                ref_key_pb = self.to_protobuf_key(
                    obj, property.key, project_id=project, namespace_id=namespace
                )
                value_pb.key_value.CopyFrom(ref_key_pb._pb)

            else:
                raise EntityProtobufConverterException(
                    f"Unknown property type {type(property)}"
                    f" for field {property.field_name}"
                )

            if (
                property.exclude_from_indexes
                and property.generic_type == GenericType.NONE
            ):
                value_pb.exclude_from_indexes = True

        return entity_pb

    def from_protobuf(self, entity_pb: Entitypb, clazz: type | None = None) -> Any:
        helper = self._get_helper(entity_pb, clazz)

        obj = helper.cls.model_construct()

        if helper.key is not None:
            self.from_protobuf_key(obj, helper.key, entity_pb.key)

        for datastore_property_name, property in helper.properties.items():
            if datastore_property_name in entity_pb.properties:
                value_pb = entity_pb.properties[datastore_property_name]
                pb_type = value_pb._pb.WhichOneof("value_type")
                if isinstance(property, ReferenceProperty):
                    self.from_protobuf_key(obj, property.key, value_pb.key_value)
                else:
                    if pb_type == "null_value":
                        value = self.from_protobuf_null_value(property)
                    elif property.generic_type == GenericType.LIST:
                        value = self.from_protobuf_list(value_pb, pb_type, property)
                    elif property.generic_type == GenericType.DICT:
                        value = self.from_protobuf_dict(value_pb, pb_type, property)
                    elif isinstance(property, AtomicProperty):
                        value = self.from_protobuf_atomic(value_pb, pb_type, property)
                    elif isinstance(property, EntityProperty):
                        value = self.from_protobuf_entity(value_pb, pb_type, property)
                    else:
                        raise EntityProtobufConverterException(
                            f"Unknown property type {type(property)}"
                            f" for field {property.field_name}"
                        )
                    setattr(obj, property.field_name, value)
            else:
                self.to_protobuf_null_value(value_pb, property)

        return obj

    def to_protobuf_null_value(
        self, value_pb: Valuepb, property_def: DatastoreProperty
    ):
        if (
            property_def.generic_type == GenericType.LIST
            or property_def.generic_type == GenericType.DICT
            or property_def.is_optional
        ):
            setattr(value_pb, "null_value", struct_pb2.NULL_VALUE)
        else:
            raise EntityProtobufConverterException(
                f"Non-optional property {property_def.field_name} is None"
            )

    def from_protobuf_null_value(self, property_def: DatastoreProperty):
        if property_def.generic_type == GenericType.LIST:
            return []
        elif property_def.generic_type == GenericType.DICT:
            return {}
        elif property_def.is_optional:
            return None
        else:
            raise EntityProtobufConverterException(
                f"Non-optional property {property_def.field_name} is not set"
                " or is null_value"
            )

    def to_protobuf_key(
        self,
        obj: Any,
        key_def: DatastoreModelKey,
        project_id: str | None = None,
        namespace_id: str | None = None,
        database_id: str | None = None,
    ) -> Keypb:
        key_pb = Keypb()

        if project_id is not None:
            key_pb.partition_id.project_id = project_id

        if database_id is not None:
            key_pb.partition_id.database_id = database_id

        if namespace_id is not None:
            key_pb.partition_id.namespace_id = namespace_id

        is_first = True
        for path_item in key_def.path_items:
            element_pb = key_pb.PathElement()
            element_pb.kind = path_item.kind

            value = getattr(obj, path_item.field_name, None)
            if value is None:
                if not is_first:
                    raise EntityProtobufConverterException(
                        f"Value for key field {path_item.field_name} is None,"
                        f" for path kind {path_item.kind}"
                    )
            else:
                if isinstance(value, int):
                    element_pb.id = value
                elif isinstance(value, str):
                    element_pb.name = value
                else:
                    raise EntityProtobufConverterException(
                        f"Key field {path_item.field_name} is of type {type(value)}."
                        "It can only be an int or str"
                    )

            key_pb.path.append(element_pb)
            is_first = False

        return key_pb

    def from_protobuf_key(
        self, obj: Any, key_def: DatastoreModelKey, key_pb: Keypb
    ) -> None:
        if len(key_pb.path) != len(key_def.path_items):
            raise EntityProtobufConverterException(
                f"Key path length mismatch. Expected {len(key_def.path_items)},"
                f" got {len(key_pb.path)}"
            )

        for idx, path_item in enumerate(key_def.path_items):
            element_pb = key_pb.path[idx]
            if key_def.path_items[idx].kind != element_pb.kind:
                raise EntityProtobufConverterException(
                    f"Key path kind mismatch. Expected {key_def.path_items[idx].kind},"
                    f" got {element_pb.kind}"
                )
            if key_def.path_items[idx].field_type == int:
                if element_pb.id is None:
                    raise EntityProtobufConverterException(
                        "Key path id mismatch. Expected int, got None"
                    )
                setattr(obj, path_item.field_name, element_pb.id)
            elif key_def.path_items[idx].field_type == str:
                if element_pb.name is None:
                    raise EntityProtobufConverterException(
                        "Key path name mismatch. Expected str, got None"
                    )

                setattr(
                    obj,
                    path_item.field_name,
                    element_pb.name,
                )

    def to_protobuf_list(
        self, value_pb: Valuepb, value: Any, property_def: DatastoreProperty
    ):
        if len(value) == 0:
            array_value = entity_pb2.ArrayValue(values=[])._pb
            value_pb.array_value.CopyFrom(array_value)
        else:
            l_pb = value_pb.array_value.values
            for item in value:
                i_pb = l_pb.add()  # type: ignore
                if isinstance(property_def, AtomicProperty):
                    self.to_protobuf_atomic(i_pb, item, property_def)
                elif isinstance(property_def, EntityProperty):
                    self.to_protobuf_entity(i_pb, item, property_def)

                if property_def.exclude_from_indexes:
                    i_pb.exclude_from_indexes = True

    def from_protobuf_list(
        self, value_pb: Valuepb, pb_type: str, property_def: DatastoreProperty
    ):
        if pb_type != "array_value":
            raise EntityProtobufConverterException(
                f"Got pb_type {pb_type} for list property {property_def.field_name}"
            )

        def get_value(item_value_pb: Valuepb):
            pb_type = item_value_pb._pb.WhichOneof("value_type")

            if isinstance(property_def, AtomicProperty):
                return self.from_protobuf_atomic(item_value_pb, pb_type, property_def)
            elif isinstance(property_def, EntityProperty):
                return self.from_protobuf_entity(item_value_pb, pb_type, property_def)

        return [
            get_value(item_value_pb) for item_value_pb in value_pb.array_value.values
        ]

    def to_protobuf_dict(
        self, value_pb: Valuepb, value: Any, property_def: DatastoreProperty
    ):
        if len(value) == 0:
            setattr(value_pb, "null_value", struct_pb2.NULL_VALUE)
        else:
            entity_pb = Entitypb()
            for key, item in value.items():
                item_value_pb = entity_pb.properties._pb.get_or_create(  # type: ignore
                    key
                )
                if isinstance(property_def, AtomicProperty):
                    self.to_protobuf_atomic(item_value_pb, item, property_def)
                elif isinstance(property_def, EntityProperty):
                    self.to_protobuf_entity(item_value_pb, item, property_def)

                if property_def.exclude_from_indexes:
                    item_value_pb.exclude_from_indexes = True

            value_pb.entity_value.CopyFrom(entity_pb._pb)

    def from_protobuf_dict(
        self, value_pb: Valuepb, pb_type: str, property_def: DatastoreProperty
    ):
        if pb_type != "entity_value":
            raise EntityProtobufConverterException(
                f"Got pb_type {pb_type} for dict property {property_def.field_name}"
            )

        def get_value(item_value_pb: Valuepb):
            pb_type = item_value_pb._pb.WhichOneof("value_type")

            if isinstance(property_def, AtomicProperty):
                return self.from_protobuf_atomic(item_value_pb, pb_type, property_def)
            elif isinstance(property_def, EntityProperty):
                return self.from_protobuf_entity(item_value_pb, pb_type, property_def)

        return {
            field_name: get_value(item_value_pb)
            for field_name, item_value_pb in value_pb.entity_value.properties.items()
        }

    def to_protobuf_entity(
        self, value_pb: Valuepb, embedded_obj: Any, property_def: EntityProperty
    ) -> None:
        embedded_pb = self.to_protobuf(embedded_obj, entity_property=property_def)
        value_pb.entity_value.CopyFrom(embedded_pb._pb)

    def from_protobuf_entity(
        self, value_pb: Valuepb, pb_type: str, property_def: EntityProperty
    ) -> Any:
        if pb_type != "entity_value":
            raise EntityProtobufConverterException(
                f"Got property type {pb_type} for an entity type value"
            )
        embedded_obj = self.from_protobuf(value_pb.entity_value, property_def.clazz)
        return embedded_obj

    def to_protobuf_atomic(
        self, value_pb: Valuepb, value: Any, property_def: AtomicProperty
    ) -> None:
        pb_type = ATOMIC_TYPE_TO_DATASTORE_TYPE[property_def.field_type]
        if isinstance(value, GeoPoint):
            pb_value = latlng_pb2.LatLng(
                latitude=value.latitude, longitude=value.longitude
            )
            value_pb.geo_point_value.CopyFrom(pb_value)
        elif pb_type == "timestamp_value":
            pb_value = self.to_protobuf_timestamp(value)
            value_pb.timestamp_value.CopyFrom(pb_value)
        else:
            pb_value = value

            if property_def.field_type in [IntEnum, StrEnum]:
                pb_value = self.to_protobuf_enum(value, property_def)

            setattr(value_pb, pb_type, pb_value)

    def from_protobuf_atomic(
        self, value_pb: Valuepb, pb_type: str, property_def: AtomicProperty
    ) -> Any:
        expected_type = ATOMIC_TYPE_TO_DATASTORE_TYPE[property_def.field_type]

        if pb_type != expected_type:
            raise EntityProtobufConverterException(
                f"Expected {expected_type}, got {pb_type}"
            )

        value = getattr(value_pb, pb_type)
        if property_def.field_type == GeoPoint:
            value = GeoPoint(
                latitude=value.latitude,
                longitude=value.longitude,
            )
        if property_def.field_type in [IntEnum, StrEnum]:
            value = self.from_protobuf_enum(value, property_def)
        if pb_type == "timestamp_value":
            value = self.from_protobuf_timestamp(value, property_def)
        return value

    def to_protobuf_enum(
        self, value: Any, property_def: AtomicProperty
    ) -> IntEnum | StrEnum:
        if property_def.enum_class is None:
            raise EntityProtobufConverterException(
                f"No enum_class for field {property_def.field_name}"
            )

        return property_def.enum_class(value)

    def from_protobuf_enum(
        self, valuepb: Any, property_def: AtomicProperty
    ) -> IntEnum | StrEnum:
        if property_def.enum_class is None:
            raise EntityProtobufConverterException(
                f"No enum_class for field {property_def.field_name}"
            )

        return property_def.enum_class(valuepb)

    def to_protobuf_timestamp(self, value: datetime | time | timedelta) -> Any:
        if isinstance(value, time):
            value = datetime.combine(UTC_EPOCH, value)
        elif isinstance(value, timedelta):
            value = UTC_EPOCH + value
        elif isinstance(value, date) and not isinstance(value, datetime):
            value = datetime.combine(
                value, time(hour=0, minute=0, second=0), timezone.utc
            )

        if not value.tzinfo:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)

        return Timestamppb(
            seconds=int(value.timestamp()), nanos=value.microsecond * 1000
        )

    def from_protobuf_timestamp(
        self,
        valuepb: DatetimeWithNanoseconds,
        property_def: AtomicProperty,
    ) -> Any:
        value = datetime(
            valuepb.year,
            valuepb.month,
            valuepb.day,
            valuepb.hour,
            valuepb.minute,
            valuepb.second,
            microsecond=valuepb.nanosecond // 1000,
            tzinfo=timezone.utc,
        )

        if property_def.field_type == datetime:
            return value
        elif property_def.field_type == date:
            return value.date()
        elif property_def.field_type == time:
            return value.timetz()
        elif property_def.field_type == timedelta:
            return value - UTC_EPOCH
        elif property_def.field_type == NaiveDatetime:
            return value.replace(tzinfo=None)
        elif property_def.field_type == AwareDatetime:
            return value
        else:
            return value
