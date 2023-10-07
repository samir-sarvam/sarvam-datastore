from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from enum import IntEnum
from typing import Any, List, Tuple
from pydantic import BaseModel, create_model, NaiveDatetime, AwareDatetime
import pytest
from sarvam_datastore._model_helper import (
    DatastoreModelHelper,
    AtomicProperty,
    EntityProperty,
    DatastoreConfig,
    DatastoreModelException,
    ReferenceProperty,
    GenericType,
)
from sarvam_datastore import GeoPoint

from contextlib import nullcontext


@pytest.mark.parametrize(
    "key_params, key_types, expectation",
    [
        [[], [], nullcontext()],
        [[("Single", "amember")], [str], nullcontext()],
        [[("Parent", "aparent"), ("Child", "achild")], [str, str], nullcontext()],
        [[("Parent", "aparent"), ("Child", "achild")], [str, int], nullcontext()],
        [
            [("Parent", "aparent"), ("Child", "achild")],
            [str, float],
            pytest.raises(DatastoreModelException),
        ],
    ],
)
def test_key(
    key_params: List[Tuple[str, str]], key_types: list[type], expectation
) -> None:
    with expectation:
        if len(key_params) > 0:
            member_dict = {x[1]: (y, ...) for x, y in zip(key_params, key_types)}
            test_model = create_model("TestModel", **member_dict)  # type: ignore
            helper = DatastoreModelHelper(test_model, DatastoreConfig(key=key_params))
            assert helper.kind == key_params[-1][0]
            assert helper.key is not None
            assert helper.key.length == len(key_params)
            for idx, key_param in enumerate(key_params):
                assert helper.key.kind(idx) == key_param[0]
                assert helper.key.field_name(idx) == key_param[1]
        else:
            member_dict = None
            test_model = create_model("TestModel", random_member=(str, ...))
            helper = DatastoreModelHelper(test_model)
            assert helper.key is None


@pytest.mark.parametrize(
    "key_refs, key_types, expectation",
    [
        [[], [], nullcontext()],
        [[[("Single", "amember")]], [[str]], nullcontext()],
        [[[("Parent", "aparent"), ("Child", "achild")]], [[str, str]], nullcontext()],
        [[[("Parent", "aparent"), ("Child", "achild")]], [[str, int]], nullcontext()],
        [
            [[("Parent", "aparent"), ("Child", "achild")]],
            [[str, float]],
            pytest.raises(DatastoreModelException),
        ],
    ],
)
def test_key_reference(
    key_refs: list[list[tuple[str, str]]], key_types: list[list[type]], expectation
) -> None:
    with expectation:
        if len(key_refs) > 0:
            field_names: list[str] = []
            member_dict: dict[str, Any] = {}
            for key_ref, key_type in zip(key_refs, key_types):
                member_dict |= {x[1]: (y, ...) for x, y in zip(key_ref, key_type)}
            field_names.append(key_ref[-1][1])
            test_model = create_model("TestModel", **member_dict)
            helper = DatastoreModelHelper(
                test_model, DatastoreConfig(key_references=key_refs)
            )

            for idx, field_name in enumerate(field_names):
                key_ref = key_refs[idx]
                property = helper.properties[field_name]
                assert property is not None
                assert isinstance(property, ReferenceProperty)
                key = property.key
                assert len(key_ref) == key.length
                for path_idx, key_param in enumerate(key_ref):
                    assert key.kind(path_idx) == key_param[0]
                    assert key.field_name(path_idx) == key_param[1]


@pytest.mark.parametrize(
    "member_type",
    [
        bool,
        int,
        IntEnum,
        str,
        float,
        GeoPoint,
        bytes,
        datetime,
        date,
        time,
        timedelta,
        NaiveDatetime,
        AwareDatetime,
    ],
)
@pytest.mark.parametrize(
    "is_optional,is_list", [[False, False], [True, False], [False, True], [True, True]]
)
def test_atomic_property_parsing(
    member_type: Any,
    is_optional: bool,
    is_list: bool,
) -> None:
    if is_optional and not is_list:
        trial_member_type = member_type | None
    elif not is_optional and is_list:
        trial_member_type = list[member_type]
    elif is_optional and is_list:
        trial_member_type = list[member_type] | None
    else:
        trial_member_type = member_type

    test_model = create_model("TestModel", test_member=(trial_member_type, ...))
    helper = DatastoreModelHelper(test_model)
    assert isinstance(helper.properties["test_member"], AtomicProperty)
    assert helper.properties["test_member"].is_optional == is_optional
    assert (
        helper.properties["test_member"].generic_type == GenericType.LIST
        if is_list
        else GenericType.NONE
    )


def test_pydantic_property():
    class PydanticModel(BaseModel):
        test_member: str

    test_model = create_model("TestModel", test_member=(PydanticModel, ...))
    helper = DatastoreModelHelper(test_model)

    assert isinstance(helper.properties["test_member"], EntityProperty)
    assert helper.properties["test_member"].entity_type == "entity"
    assert helper.properties["test_member"].clazz == PydanticModel


def test_dataclass_property():
    @dataclass
    class DataclassModel:
        test_member: str

    test_model = create_model("TestModel", test_member=(DataclassModel, ...))
    helper = DatastoreModelHelper(test_model)

    assert isinstance(helper.properties["test_member"], EntityProperty)
    assert helper.properties["test_member"].entity_type == "data_class"
    assert helper.properties["test_member"].clazz == DataclassModel
