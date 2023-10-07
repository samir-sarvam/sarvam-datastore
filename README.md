# sarvam-datastore

A pydantic-gcp datastore mapper, which uses the datastore async client to provide asynchronous read-write to the datastore.

## Usage

```shell
# make sure you add sarvam-python-ci as a source in your pyproject.toml file
poetry add sarvam-datastore --source sarvam-python-ci
```

You can then map pydantic objects to datastore:

```python
class Dataset(BaseModel):
    id: int | None
    name: str
    tags: List[str]
    description: str

    class DataConfig:
        key = [("Dataset", "id")]
        exclude_from_indexes = ["description"]

```

You can then use the datastore repository:

```python
from sarvam_datastore import (
    DatastoreRepository,
    DatastoreModelHelper,
    DatastoreModelHelperRegistry,
    EntityProtobufConverter,
)

registry = DatastoreModelHelperRegistry() # A registry to register all your model classes
dataset_helper = DatastoreModelHelper(StandAloneEntity) # helper creates meta-data for one model class
registry.register(dataset_helper) # now register the helper for your model class
converter = EntityProtobufConverter(registry) # a converter uses the registry to do the conversion from 
                                              # model to protobuf and back

# repository uses async grpc client to interact with datastore
repository = DatastoreRepository(converter, "<your-project-id", "your-namespace-id") 

# insert an entity
repository.insert(Dataset(...))
```

See the tests (esp test_repository.py) for more usages of repository.

## Developer notes

To run tests, run

```shell
poetry run poe test
```

Note that the tests use project "gpu-reservation-sarvam" and namespace "sarvam-test"