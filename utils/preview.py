# === FILE: utils/preview.py ===
def preview_salesforce_data(sf, object_name, field_mappings, limit=20):
    try:
        fields = list(field_mappings.keys())
        soql = f"SELECT {', '.join(fields)} FROM {object_name} LIMIT {limit}"
        records = sf.query(soql)['records']

        transformed_docs = []
        for idx, record in enumerate(records):
            doc = {
                field_mappings[f]: record.get(f) for f in field_mappings
            }
            doc["_key"] = record.get("Id", f"{object_name}_{idx}")
            transformed_docs.append(doc)

        return records, transformed_docs
    except Exception as e:
        raise Exception(f"Preview failed for {object_name}: {e}")