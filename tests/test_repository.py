import pytest
from sarvam_datastore import DatastoreRepository
from google.cloud import datastore
from .sample_model import AllocatedIdEntity, EmbeddedEntity, StandAloneEntity
from .sample_settings import SampleSettings


@pytest.fixture()
def model_dict():
    return None


@pytest.fixture()
def model_config():
    return None


async def test_repo_insert(config: SampleSettings, repo: DatastoreRepository):
    expected = StandAloneEntity(aref=20)

    mr = await repo.insert(expected)

    assert mr.version > 0

    key = datastore.Key(
        "StandAlone",
        expected.astr,
        project=config.datastore_project,
        namespace=config.datastore_namespace,
    )

    actual: StandAloneEntity | None = await repo.get(key)  # type: ignore

    for field_name, fieldspec in expected.__fields__.items():
        if field_name != "astr":
            assert getattr(expected, field_name) == getattr(actual, field_name)

    assert actual is not None
    assert isinstance(actual.aembedded, EmbeddedEntity)


async def test_id_allocation(repo: DatastoreRepository):
    expected = AllocatedIdEntity()
    mr = await repo.insert(expected)
    assert mr.version > 0

    actual: AllocatedIdEntity = await repo.get(mr.key)  # type: ignore
    assert mr.key.id == actual.aint
    assert actual.abool == expected.abool


async def test_query(config: SampleSettings, repo: DatastoreRepository):
    query = datastore.Query(
        client=None,
        kind="StandAlone",
        project=config.datastore_project,
        namespace=config.datastore_namespace,
    )
    iterator = repo.run_query(query)
    async for i in iterator:
        print(i)


async def test_update_entity(repo: DatastoreRepository):
    expected = AllocatedIdEntity()
    mr = await repo.insert(expected)
    assert mr.version > 0

    actual: AllocatedIdEntity = await repo.get(mr.key)  # type: ignore
    assert mr.key.id == actual.aint
    assert actual.abool == expected.abool

    actual.abool = True
    actual.astr = "12"
    resp = await repo.upsert_multi([actual])
    print(resp)
    actual2: AllocatedIdEntity = await repo.get(mr.key)  # type: ignore
    assert mr.key.id == actual2.aint
    assert actual2.astr == "12"
