from datetime import datetime, timezone
from ._model_helper import (
    DatastoreModelHelper,
)
import logging

logger = logging.getLogger(__name__)

UTC_EPOCH = datetime.fromtimestamp(0, tz=timezone.utc)


class DatastoreModelHelperRegistryException(Exception):
    pass


class DatastoreModelHelperRegistry:
    def __init__(self) -> None:
        self._model_helpers: dict[str, DatastoreModelHelper] = {}
        self._model_helpers_embedded: dict[type, DatastoreModelHelper] = {}

    def register(self, model_helper: DatastoreModelHelper):
        self._model_helpers_embedded[model_helper.cls] = model_helper

        if model_helper.kind is not None:
            if model_helper.kind in self._model_helpers:
                raise DatastoreModelHelperRegistryException(
                    f"Model helper with kind {model_helper.kind} already registered"
                )
            self._model_helpers[model_helper.kind] = model_helper

    def get_by_kind(self, kind: str):
        return self._model_helpers[kind]

    def get_by_class(self, clazz: type):
        return self._model_helpers_embedded[clazz]
