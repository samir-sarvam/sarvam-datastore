import logging
from sarvam_datastore import DatastoreRepository

logger = logging.getLogger(__name__)


async def delete_items_of_kind(kind: str, ds_rep: DatastoreRepository, namespace: str):
    if not namespace.startswith("sarvam-test"):
        raise Exception(f"Cannot proceed with namespace {namespace}")

    query = ds_rep.get_query_filtered(kind, [], namespace=namespace)
    query.keys_only()
    delete_keys_pb = [key_pb.key async for key_pb in ds_rep.run_query_raw(query)]

    if len(delete_keys_pb) > 0:
        for i in range(0, len(delete_keys_pb), 499):
            delete_keys_chunk_pb = delete_keys_pb[i : i + 499]
            await ds_rep.delete_multi_raw(delete_keys_chunk_pb, namespace=namespace)
