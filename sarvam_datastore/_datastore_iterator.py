import base64
from google.api_core import page_iterator_async, page_iterator
from google.cloud.datastore.query import _pb_from_query

from google.cloud.datastore_v1.types import entity as entity_pb2
from google.cloud.datastore_v1.types import query as query_pb2
from google.cloud.datastore import helpers
from ._converter import EntityProtobufConverter


_NOT_FINISHED = query_pb2.QueryResultBatch.MoreResultsType.NOT_FINISHED
_NO_MORE_RESULTS = query_pb2.QueryResultBatch.MoreResultsType.NO_MORE_RESULTS

_FINISHED = (
    _NO_MORE_RESULTS,
    query_pb2.QueryResultBatch.MoreResultsType.MORE_RESULTS_AFTER_LIMIT,
    query_pb2.QueryResultBatch.MoreResultsType.MORE_RESULTS_AFTER_CURSOR,
)


class DatastoreIterator(page_iterator_async.AsyncIterator):
    """This iterator has been adapted from the sync iterators
    in the google.cloud.datastore api, and adapted to use the async
    client APIs, and to integrate with the pydantic model <--> protobuf
    conversion.

    Represent the state of a given execution of a Query.

    :type query: :class:`~google.cloud.datastore.query.Query`
    :param query: Query object holding permanent configuration (i.e.
                  things that don't change on with each page in
                  a results set).

    :type client: :class:`~google.cloud.datastore.client.Client`
    :param client: The client used to make a request.

    :type limit: int
    :param limit: (Optional) Limit the number of results returned.

    :type offset: int
    :param offset: (Optional) Offset used to begin a query.

    :type start_cursor: bytes
    :param start_cursor: (Optional) Cursor to begin paging through
                         query results.

    :type end_cursor: bytes
    :param end_cursor: (Optional) Cursor to end paging through
                       query results.

    :type eventual: bool
    :param eventual: (Optional) Defaults to strongly consistent (False).
                                Setting True will use eventual consistency,
                                but cannot be used inside a transaction or
                                with read_time, otherwise will raise ValueError.

    :type retry: :class:`google.api_core.retry.Retry`
    :param retry:
        A retry object used to retry requests. If ``None`` is specified,
        requests will be retried using a default configuration.

    :type timeout: float
    :param timeout:
        Time, in seconds, to wait for the request to complete.
        Note that if ``retry`` is specified, the timeout applies
        to each individual attempt.

    :type read_time: datetime
    :param read_time: (Optional) Runs the query with read time consistency.
                      Cannot be used with eventual consistency or inside a
                      transaction, otherwise will raise ValueError.
                      This feature is in private preview.


    :type raw_entity: bool
    :param raw_entity: (Optional) return the protobuf entity, rather than the
                       converted object.

    """

    next_page_token = None

    def __init__(
        self,
        query,
        client,
        limit=None,
        offset=None,
        start_cursor=None,
        end_cursor=None,
        eventual=False,
        retry=None,
        timeout=None,
        read_time=None,
        raw_entity=False,
        converter: EntityProtobufConverter | None = None,
    ):
        super(DatastoreIterator, self).__init__(
            client=client,
            item_to_value=_item_to_entity if not raw_entity else _item_to_entity_raw,
            page_token=start_cursor,
            max_results=limit,
        )
        self._query = query
        self._offset = offset
        self._end_cursor = end_cursor
        self._eventual = eventual
        self._retry = retry
        self._timeout = timeout
        self._read_time = read_time
        # The attributes below will change over the life of the iterator.
        self._more_results = True
        self._skipped_results = 0
        self._converter: EntityProtobufConverter | None = converter

    def _build_protobuf(self):
        """Build a query protobuf.

        Relies on the current state of the iterator.

        :rtype:
            :class:`.query_pb2.Query`
        :returns: The query protobuf object for the current
                  state of the iterator.
        """
        pb = _pb_from_query(self._query)

        start_cursor = self.next_page_token
        if start_cursor is not None:
            pb.start_cursor = base64.urlsafe_b64decode(start_cursor)

        end_cursor = self._end_cursor
        if end_cursor is not None:
            pb.end_cursor = base64.urlsafe_b64decode(end_cursor)

        if self.max_results is not None:
            pb.limit = self.max_results - self.num_results

        if start_cursor is None and self._offset is not None:
            # NOTE: We don't need to add an offset to the request protobuf
            #       if we are using an existing cursor, because the offset
            #       is only relative to the start of the result set, not
            #       relative to each page (this method is called per-page)
            pb.offset = self._offset

        return pb

    def _process_query_results(self, response_pb):
        """Process the response from a datastore query.

        :type response_pb: :class:`.datastore_pb2.RunQueryResponse`
        :param response_pb: The protobuf response from a ``runQuery`` request.

        :rtype: iterable
        :returns: The next page of entity results.
        :raises ValueError: If ``more_results`` is an unexpected value.
        """
        self._skipped_results = response_pb.batch.skipped_results
        if response_pb.batch.more_results == _NO_MORE_RESULTS:
            self.next_page_token = None
        else:
            self.next_page_token = base64.urlsafe_b64encode(
                response_pb.batch.end_cursor
            )
        self._end_cursor = None

        if response_pb.batch.more_results == _NOT_FINISHED:
            self._more_results = True
        elif response_pb.batch.more_results in _FINISHED:
            self._more_results = False
        else:
            raise ValueError("Unexpected value returned for `more_results`.")

        return [result.entity for result in response_pb.batch.entity_results]

    async def _next_page(self):
        """Get the next page in the iterator.

        :rtype: :class:`~google.cloud.iterator.Page`
        :returns: The next page in the iterator (or :data:`None` if
                  there are no pages left).
        """
        if not self._more_results:
            return None

        query_pb = self._build_protobuf()
        # transaction = self.client.current_transaction TODO:Need to handle transactions
        transaction = None
        if transaction is None:
            transaction_id = None
        else:
            transaction_id = transaction.id
        read_options = helpers.get_read_options(
            self._eventual, transaction_id, self._read_time
        )

        partition_id = entity_pb2.PartitionId(
            project_id=self._query.project, namespace_id=self._query.namespace
        )

        kwargs = {}

        if self._retry is not None:
            kwargs["retry"] = self._retry

        if self._timeout is not None:
            kwargs["timeout"] = self._timeout

        response_pb = await self.client.run_query(
            request={
                "project_id": self._query.project,
                "partition_id": partition_id,
                "read_options": read_options,
                "query": query_pb,
            },
            **kwargs,
        )

        while (
            response_pb.batch.more_results == _NOT_FINISHED
            and response_pb.batch.skipped_results < query_pb.offset
        ):
            # We haven't finished processing. A likely reason is we haven't
            # skipped all of the results yet. Don't return any results.
            # Instead, rerun query, adjusting offsets. Datastore doesn't process
            # more than 1000 skipped results in a query.
            old_query_pb = query_pb
            query_pb = query_pb2.Query()
            query_pb._pb.CopyFrom(old_query_pb._pb)  # copy for testability
            query_pb.start_cursor = response_pb.batch.skipped_cursor
            query_pb.offset -= response_pb.batch.skipped_results

            response_pb = self.client.run_query(
                request={
                    "project_id": self._query.project,
                    "partition_id": partition_id,
                    "read_options": read_options,
                    "query": query_pb,
                },
                **kwargs,
            )

        entity_pbs = self._process_query_results(response_pb)
        return page_iterator.Page(self, entity_pbs, self.item_to_value)


def _item_to_entity_raw(iterator, entity_pb):
    return entity_pb


def _item_to_entity(iterator: DatastoreIterator, entity_pb):
    """Convert a raw protobuf entity to the native object.

    :type iterator: :class:`~google.api_core.page_iterator.Iterator`
    :param iterator: The iterator that is currently in use.

    :type entity_pb:
        :class:`.entity_pb2.Entity`
    :param entity_pb: An entity protobuf to convert to a native entity.

    :rtype: :class:`~google.cloud.datastore.entity.Entity`
    :returns: The next entity in the page.
    """

    if iterator._converter is None:
        return helpers.entity_from_protobuf(entity_pb)
    else:
        try:
            datastore_entity = iterator._converter.from_protobuf(entity_pb)
        except Exception:
            datastore_entity = helpers.entity_from_protobuf(entity_pb)

        return datastore_entity
