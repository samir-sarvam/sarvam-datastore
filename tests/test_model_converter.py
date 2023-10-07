from datetime import date, datetime, time, timedelta
from typing import Dict, List
from pydantic import BaseModel, NaiveDatetime, AwareDatetime
import pytest
from sarvam_datastore import (
    DatastoreModelHelperRegistry,
    DatastoreModelHelper,
    DatastoreConfig,
    EntityProtobufConverter,
    GeoPoint,
)
from .model_mocker import (
    DatamodelHelperMock,
    SampleEmbedded,
    SampleIntEnum,
    SampleStrEnum,
)


@pytest.mark.parametrize(
    "model_dict, model_config",
    [
        [{"a": (int, ...), "b": (int, ...)}, DatastoreConfig(key=[("Parent", "a")])],
        [{"a": (bool, ...)}, None],
        [{"a": (int, ...)}, None],
        [{"a": (SampleIntEnum, ...)}, None],
        [{"a": (float, ...)}, None],
        [{"a": (str, ...)}, None],
        [{"a": (SampleStrEnum, ...)}, None],
        [{"a": (GeoPoint, ...)}, None],
        [{"a": (bytes, ...)}, None],
        [{"a": (datetime, ...)}, None],
        [{"a": (date, ...)}, None],
        [{"a": (time, ...)}, None],
        [{"a": (timedelta, ...)}, None],
        [{"a": (NaiveDatetime, ...)}, None],
        [{"a": (AwareDatetime, ...)}, None],
        [{"a": (SampleEmbedded, ...)}, None],
        [{"a": (List[int], ...)}, None],
        [{"a": (List[SampleEmbedded], ...)}, None],
        [{"a": (Dict[str, int], ...)}, None],
        [{"a": (Dict[str, SampleEmbedded], ...)}, None],
        [{"a": (int | None, ...)}, None],
        [{"a": (NaiveDatetime | None, ...)}, None],
    ],
)
def test_converter(
    model_dict,
    model_config,
    model: type[BaseModel],
    registry: DatastoreModelHelperRegistry,
    helper_model: DatastoreModelHelper,
    converter: EntityProtobufConverter,
) -> None:
    mocker = DatamodelHelperMock(registry)
    expected_obj = mocker.generate(helper_model)
    entity_pb = converter.to_protobuf(expected_obj, "test", "test")
    actual_obj = converter.from_protobuf(entity_pb, model)
    assert actual_obj == expected_obj


@pytest.mark.parametrize(
    "model_dict, model_config",
    [[{"a": (int, ...), "b": (int, ...)}, DatastoreConfig(exclude_from_indexes=["b"])]],
)
def test_exclude(
    model_dict,
    model_config,
    model: type[BaseModel],
    registry: DatastoreModelHelperRegistry,
    helper_model: DatastoreModelHelper,
    converter: EntityProtobufConverter,
    caplog,
) -> None:
    mocker = DatamodelHelperMock(registry)
    expected_obj = mocker.generate(helper_model)
    entity_pb = converter.to_protobuf(expected_obj, "test", "test")

    for field_name, prop in entity_pb.properties.items():
        if field_name in model_config.exclude_from_indexes:
            assert prop.exclude_from_indexes
