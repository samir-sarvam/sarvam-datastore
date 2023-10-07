from typing import Dict, Tuple, get_args, get_origin, _GenericAlias  # type: ignore
from types import GenericAlias, UnionType
from dataclasses import is_dataclass
from pydantic import BaseModel, AwareDatetime, NaiveDatetime
from enum import Enum, StrEnum, IntEnum
from datetime import datetime, date, time, timedelta
from ._geo_point import GeoPoint

ATOMIC_TYPES = (
    bool
    | int
    | IntEnum
    | float
    | str
    | StrEnum
    | GeoPoint
    | bytes
    | datetime
    | date
    | time
    | timedelta
    | NaiveDatetime
    | AwareDatetime
)

ATOMIC_TYPE_TO_DATASTORE_TYPE = {
    bool: "boolean_value",
    int: "integer_value",
    IntEnum: "integer_value",
    float: "double_value",
    str: "string_value",
    StrEnum: "string_value",
    GeoPoint: "geo_point_value",
    bytes: "blob_value",
    datetime: "timestamp_value",
    date: "timestamp_value",
    time: "timestamp_value",
    timedelta: "timestamp_value",
    NaiveDatetime: "timestamp_value",
    AwareDatetime: "timestamp_value",
}


class EntityType(StrEnum):
    PYDANTIC = "entity"
    DATA_CLASS = "data_class"
    DICT = "dict"


class DatastoreModelPathItem(BaseModel):
    kind: str
    field_name: str
    field_type: type[int] | type[str]


class DatastoreModelKey(BaseModel):
    path_items: list[DatastoreModelPathItem]

    @property
    def length(self):
        return len(self.path_items)

    def kind(self, idx: int = -1):
        return self.path_items[idx].kind

    def field_name(self, idx: int = -1):
        return self.path_items[idx].field_name


class GenericType(Enum):
    NONE = "none"
    DICT = "dict"
    LIST = "list"


class DatastoreProperty(BaseModel):
    datastore_field_name: str
    field_name: str
    is_optional: bool = False
    exclude_from_indexes: bool = False
    generic_type: GenericType = GenericType.NONE


class AtomicProperty(DatastoreProperty):
    field_type: type  # TODO: ATOMIC_TYPES
    enum_class: type | None = None


class EntityProperty(DatastoreProperty):
    entity_type: EntityType
    clazz: type | None = None


class ReferenceProperty(DatastoreProperty):
    key: DatastoreModelKey


class DatastoreConfig(BaseModel):
    key: list[tuple[str, str]] = []
    key_references: list[list[tuple[str, str]]] = []
    exclude_from_indexes: list[str] = []
    renamed_fields: dict[str, str] = {}
    ignore_fields: list[str] = []


class DatastoreModelException(Exception):
    pass


class DatastoreModelHelper:
    def __init__(self, cls: type[BaseModel], config: DatastoreConfig | None = None):
        self.kind: str | None = None
        self.cls: type[BaseModel] = cls
        self.properties: Dict[str, DatastoreProperty] = {}
        self.key: DatastoreModelKey | None = None

        if config is not None:
            self.config = config
        else:
            self.config = self._process_class_config()

        # fields which should not be created as properties
        self.key_or_ignore_fields = set(
            [
                *self.config.ignore_fields,
                *[f for _, f in self.config.key],
                *[f for ref_list in self.config.key_references for _, f in ref_list],
            ]
        )

        self._process_class_fields()

    @classmethod
    def _parse_optional(cls, field_name, annotation: type) -> Tuple[type, bool]:
        is_optional = False
        actual_annotation = annotation
        if isinstance(annotation, UnionType):
            args = get_args(annotation)
            if len(args) > 2:
                raise DatastoreModelException(
                    f"field {field_name}: Union type {annotation}"
                    " has more than 2 args"
                )
            if type(None) in args:
                is_optional = True
                for arg in args:
                    if type(None) != arg:
                        actual_annotation = arg
            else:
                raise DatastoreModelException(
                    f"field {field_name}: Union type {annotation} has no non-None args"
                )

        return actual_annotation, is_optional

    def _process_class_config(self):
        if hasattr(self.cls, "DatastoreConfig"):
            self.config = DatastoreConfig(**self.cls.DatastoreConfig.__dict__)
        else:
            self.config = DatastoreConfig()

        return self.config

    def _process_key_fields(self, key_config: list[tuple[str, str]]):
        path_items: list[DatastoreModelPathItem] = []
        for kind, field_name in key_config:
            field_type = self.cls.model_fields[field_name].annotation
            if field_type is not None:
                field_type, is_optional = self._parse_optional(field_name, field_type)
            if field_type not in [int, str]:
                raise DatastoreModelException(
                    f"Key field {field_name} is of type {field_type}."
                    "It can only be an int or str"
                )
            path_items.append(
                DatastoreModelPathItem(
                    kind=kind,
                    field_name=field_name,
                    field_type=field_type,
                )
            )
        return DatastoreModelKey(path_items=path_items)

    def _add_key(self):
        if len(self.config.key) > 0:
            self.key = self._process_key_fields(self.config.key)
            self.kind = self.key.kind()

    def _add_property(self, property: DatastoreProperty):
        if property.field_name in self.config.renamed_fields:
            property.datastore_field_name = self.config.renamed_fields[
                property.datastore_field_name
            ]

        if property.field_name in self.config.exclude_from_indexes:
            property.exclude_from_indexes = True

        self.properties[property.datastore_field_name] = property

    def _add_references(self):
        # Handle optional references
        for key_ref in self.config.key_references:
            key = self._process_key_fields(key_ref)
            field_name = key.field_name()
            self._add_property(
                ReferenceProperty(
                    datastore_field_name=field_name, field_name=field_name, key=key
                )
            )

    def _add_entity_property(
        self,
        field_name: str,
        annotation: type,
        entity_type: EntityType,
        generic_type: GenericType = GenericType.NONE,
        is_optional=False,
    ):
        property = EntityProperty(
            datastore_field_name=field_name,
            field_name=field_name,
            entity_type=entity_type,
            generic_type=generic_type,
            is_optional=is_optional,
            clazz=None if entity_type == EntityType.DICT else annotation,
        )
        self._add_property(property)

    def _add_atomic_property(
        self,
        field_name: str,
        annotation: type,
        item_enum_class: type | None,
        generic_type: GenericType = GenericType.NONE,
        is_optional=False,
    ):
        property = AtomicProperty(
            datastore_field_name=field_name,
            field_name=field_name,
            generic_type=generic_type,
            field_type=annotation,
            is_optional=is_optional,
            enum_class=item_enum_class,
        )
        self._add_property(property)

    def add_datastore_property(self, field_name: str, annotation: type):
        item_annotation, is_optional = self._parse_optional(field_name, annotation)

        generic_type: GenericType = GenericType.NONE
        if (
            type(item_annotation) == GenericAlias
            or type(item_annotation) == _GenericAlias
        ):
            origin = get_origin(item_annotation)
            if origin == list:
                generic_type = GenericType.LIST
                args = get_args(item_annotation)
                if len(args) > 1:
                    raise DatastoreModelException(
                        f"{field_name} List Generic Type has more than 1 arg"
                    )
                item_annotation = args[0]
            elif origin == dict:
                generic_type = GenericType.DICT
                args = get_args(item_annotation)
                if len(args) != 2:
                    raise DatastoreModelException(
                        f"{field_name} Dict Generic Type should have only 2 args"
                    )
                if args[0] != str:
                    raise DatastoreModelException(
                        f"{field_name} Dict Generic Type first arg should be str"
                    )
                item_annotation = args[1]
            else:
                raise DatastoreModelException(
                    f"{field_name} Only List or Dict Generic Types allowed"
                    f" - got {annotation}"
                )

        assert isinstance(item_annotation, type)

        item_enum_class: type | None = None
        if issubclass(item_annotation, StrEnum):
            item_enum_class = item_annotation
            item_annotation = StrEnum
        elif issubclass(item_annotation, IntEnum):
            item_enum_class = item_annotation
            item_annotation = IntEnum

        if item_annotation in get_args(ATOMIC_TYPES):
            self._add_atomic_property(
                field_name, item_annotation, item_enum_class, generic_type, is_optional
            )
        elif issubclass(item_annotation, BaseModel):
            self._add_entity_property(
                field_name, item_annotation, EntityType.PYDANTIC, generic_type
            )
        elif is_dataclass(annotation):
            self._add_entity_property(
                field_name,
                item_annotation,
                EntityType.DATA_CLASS,
                generic_type,
                is_optional,
            )
        else:
            raise DatastoreModelException(
                f"Field {field_name} has unsupported type {annotation}"
                f"allowed atomic types are {get_args(ATOMIC_TYPES)})"
            )

    def _process_class_fields(self):
        self._add_key()
        self._add_references()

        for field_name, field in self.cls.model_fields.items():
            if field_name in self.key_or_ignore_fields:
                continue
            self.add_datastore_property(field_name, field.annotation)


# TODO
# List of References
# Reference as an object / dict
