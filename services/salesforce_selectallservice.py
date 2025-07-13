from services.salesforce_service import get_items, get_sf_fields
from services.arango_service import ensure_collection
from utils.transfer import transfer_data_with_progress

def migrate_all_salesforce_objects(sf, adb, logger=None, progress_callback=None):
    """
    Migrates all Salesforce objects and all their fields to ArangoDB as document collections.
    Returns a summary list of {object, collection, records_migrated}.
    """
    summary = []
    items = get_items(sf)
    for idx, item in enumerate(items):
        try:
            fields = get_sf_fields(sf, item)
            field_map = {f: f for f in fields}
            collection_name = item.lower()
            collection = ensure_collection(adb, collection_name, is_edge=False)
            def item_progress_callback(current, total):
                if progress_callback:
                    progress_callback(item, idx, len(items), current, total)
            count, _ = transfer_data_with_progress(
                sf=sf,
                adb=adb,
                object_name=item,
                collection_name=collection_name,
                field_mappings=field_map,
                is_edge=False,
                progress_callback=item_progress_callback
            )
            actual = collection.count()
            summary.append({
                "object": item,
                "collection": collection_name,
                "records": actual
            })
        except Exception as e:
            if logger:
                logger.error(f"Migration failed for {item}: {e}")
            summary.append({
                "object": item,
                "collection": collection_name,
                "records": 0,
                "error": str(e)
            })
    return summary 