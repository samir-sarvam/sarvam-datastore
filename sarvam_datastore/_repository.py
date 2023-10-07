import logging
from typing import Any, List, Sequence
from google.cloud import datastore
from google.cloud.datastore_v1 import DatastoreAsyncClient
from google.cloud.datastore_v1.types import Mutation
from google.cloud.datastore.helpers import key_from_protobuf
from google.cloud.datastore_v1.types import entity as entity_pb2
from ._converter import EntityProtobufConverter

from ._datastore_iterator import DatastoreIterator


class DatastoreMutationResult:
    """Represents the result of a Mutation (insert, delete, update, upsert)"""

    def __init__(self, key: datastore.Key, version: int):
        """a new mutation result

        Args:
            key (datastore.Key): the key
            version (int): the version of the object, post mutation
        """
        self.key = key
        self.version = version

    def __repr__(self):
        return f"Key - {self.key or 'None'}, version - {self.version}"


class DatastoreBatch:
    """A batch of mutations"""

    def __init__(
        self,
        converter: EntityProtobufConverter,
        project: str,
        namespace: str = "",
    ):
        """create batch

        Args:
            project (str): datastore project for batch
            namespace (str): (Optional) datastore namespace for batch
        """
        self._converter = converter
        self._mutations: List[Mutation] = []
        self._mutation_results: List[DatastoreMutationResult] = []
        self._project = project
        self._namespace = namespace

    def has_pending(self):
        """Are there unsubmitted mutations?

        Returns:
            _type_: bool
        """
        return len(self._mutations) > 0

    def has_capacity(self, citems: int):
        """does the batch have capacity to take the given number of items.

        Args:
            citems (int): the number of items to add

        Returns:
            _type_: bool
        """
        return len(self._mutations) + citems < 500

    def clear(self):
        """clear all mutations"""
        self._mutations = []

    def add_item(self, object: Any):
        """add one item to the list of mutations

        Args:
            object (Any): An object to add
        """
        mut = Mutation(
            upsert=self._converter.to_protobuf(object, self._project, self._namespace)
        )
        self._mutations.append(mut)

    def add_items(self, objects: Sequence[Any] | None):
        """Add multiple objects to the batch

        Args:
            objects (Sequence[Any]] | None): a sequence of items
        """
        if objects is not None:
            for object in objects:
                self.add_item(object)

    def get_mutations(self):
        """get all the mutations

        Returns:
            _type_: the mutations
        """
        return self._mutations

    def set_mutation_results(self, results: List[DatastoreMutationResult]):
        """Result of submitting the mutations

        Args:
            results (List[DatastoreMutationResult]): the results
        """
        self._mutation_results = results

    def get_mutation_results(self):
        """mutation results

        Returns:
            _type_: a list of mutation results
        """
        return self._mutation_results


class DatastoreRepository:
    def __init__(
        self, converter: EntityProtobufConverter, project: str, namespace: str
    ):
        self._converter = converter
        self._project = project
        self._namespace_default = namespace
        self.client = DatastoreAsyncClient()

    def get_key(self, *args, namespace=None) -> datastore.Key:
        return datastore.Key(
            *args, project=self._project, namespace=self._namespace(namespace)
        )

    def _namespace(self, namespace: str):
        if namespace is None:
            return self._namespace_default
        else:
            return namespace

    def get_batch(self, namespace=None) -> DatastoreBatch:
        return DatastoreBatch(
            self._converter, self._project, self._namespace(namespace)
        )

    async def submit_batch(self, batch: DatastoreBatch):
        logging.info(f"submit batch called : {len(batch._mutations)}")
        mrs = await self._mutate_multi(batch.get_mutations())
        return mrs

    def get_query_filtered(
        self, kind: str, filters=[], namespace=None
    ) -> datastore.Query:
        return datastore.Query(
            client=None,
            kind=kind,
            filters=filters,
            project=self._project,
            namespace=self._namespace(namespace),
        )

    def get_query_ancestor(
        self, kind: str, ancestor: datastore.Key, namespace=None
    ) -> datastore.Query:
        return datastore.Query(
            client=None,
            kind=kind,
            ancestor=ancestor,
            project=self._project,
            namespace=self._namespace(namespace),
        )

    async def insert(
        self, object: Any, exists_ok=True, namespace=None
    ) -> DatastoreMutationResult:
        mut = Mutation(
            upsert=self._converter.to_protobuf(
                object, self._project, self._namespace(namespace)
            )
        )

        multi_response = await self._mutate_multi([mut], exists_ok)
        return multi_response[0]

    async def upsert_multi(
        self, objects: Sequence[Any], exists_ok=True, namespace=None
    ) -> List[DatastoreMutationResult]:
        mutations = [
            Mutation(
                upsert=self._converter.to_protobuf(
                    object, self._project, self._namespace(namespace)
                )
            )
            for object in objects
        ]

        return await self._mutate_multi(mutations, exists_ok)

    async def delete_multi(self, keys: List[datastore.Key], namespace=None):
        mutations = [Mutation(delete=key.to_protobuf()) for key in keys]
        return await self._mutate_multi(mutations, True)

    async def _mutate_multi(self, mutations: List[Mutation], exists_ok=True):
        txn = await self.client.begin_transaction(project_id=self._project)
        cr = await self.client.commit(
            transaction=txn.transaction,
            mutations=mutations,
            project_id=self._project,
        )

        def key_or_none(key_pb):
            if len(key_pb.path) == 0:
                return None
            else:
                return key_from_protobuf(key_pb)

        response = [
            DatastoreMutationResult(key_or_none(mr.key), mr.version)
            for mr in cr.mutation_results
        ]

        return response

    async def get(self, key: datastore.Key) -> Any | None:
        lr = await self.client.lookup(
            keys=[key.to_protobuf()], project_id=self._project
        )

        if len(lr.found) == 1:
            entity_pb = lr.found[0].entity
            return self._converter.from_protobuf(entity_pb)

        return None

    def run_query(
        self, query: datastore.Query, limit: int | None = None
    ) -> DatastoreIterator:
        return DatastoreIterator(query, self.client, limit=limit)

    def run_query_raw(self, query: datastore.Query) -> DatastoreIterator:
        return DatastoreIterator(query, self.client, raw_entity=True)

    async def delete_multi_raw(self, keys_pb: List[entity_pb2.Key], namespace=None):
        mutations = [Mutation(delete=key_pb) for key_pb in keys_pb]
        return await self._mutate_multi(mutations, True)
