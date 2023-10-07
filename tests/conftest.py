import asyncio
import inspect
from typing import List, Tuple
import pytest
import pytest_asyncio
from pydantic import create_model, BaseModel

from sarvam_datastore import (
    DatastoreRepository,
    DatastoreConfig,
    DatastoreModelHelper,
    DatastoreModelHelperRegistry,
    EntityProtobufConverter,
)
from tests.fixture_helper import delete_items_of_kind
from tests.model_mocker import SampleEmbedded
from tests.sample_model import AllocatedIdEntity, EmbeddedEntity, StandAloneEntity
from .sample_settings import SampleSettings


@pytest_asyncio.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop()
    loop.slow_callback_duration = 1
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="session")
def config():
    return SampleSettings()


@pytest.fixture()
def model(model_dict: List[type | Tuple[type, ...]] | None) -> type[BaseModel] | None:
    if model_dict:
        test_model = create_model("TestModel", **model_dict)  # type: ignore
        return test_model
    return None


@pytest.fixture()
def helper_model(
    model: type[BaseModel] | None,
    model_config: DatastoreConfig | None,
):
    if model is None:
        return None
    else:
        return DatastoreModelHelper(model, model_config)


@pytest.fixture()
def registry(
    helper_model: DatastoreModelHelper,
):
    registry = DatastoreModelHelperRegistry()
    embedded_helper = DatastoreModelHelper(SampleEmbedded)
    if embedded_helper is not None:
        registry.register(embedded_helper)
    if helper_model is not None:
        registry.register(helper_model)
    yield registry


@pytest.fixture()
def converter(
    registry: DatastoreModelHelperRegistry,
):
    converter = EntityProtobufConverter(registry)
    yield converter


@pytest_asyncio.fixture()
async def repo(
    config: SampleSettings,
    registry: DatastoreModelHelperRegistry,
    converter: EntityProtobufConverter,
):
    se_helper = DatastoreModelHelper(StandAloneEntity)
    ae_helper = DatastoreModelHelper(AllocatedIdEntity)
    emb_helper = DatastoreModelHelper(EmbeddedEntity)
    registry.register(se_helper)
    registry.register(ae_helper)
    registry.register(emb_helper)
    repo = DatastoreRepository(
        converter, config.datastore_project, config.datastore_namespace
    )
    yield repo
    await delete_items_of_kind("StandAlone", repo, config.datastore_namespace)
    await delete_items_of_kind("AllocatedId", repo, config.datastore_namespace)


def pytest_collection_modifyitems(config, items):
    for item in items:
        if inspect.iscoroutinefunction(item.function):
            item.add_marker(pytest.mark.asyncio)
