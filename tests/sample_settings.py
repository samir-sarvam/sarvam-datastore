import os
from pydantic_settings import BaseSettings

DATASTORE_TEST_PROJECT = os.environ.get(
    "DATASTORE_TEST_PROJECT", "gpu-reservation-sarvam"
)
DATASTORE_TEST_NAMESPACE = os.environ.get("DATASTORE_TEST_NAMESPACE", "sarvam-test")


class SampleSettings(BaseSettings):
    datastore_project: str = DATASTORE_TEST_PROJECT
    datastore_namespace: str = DATASTORE_TEST_NAMESPACE
