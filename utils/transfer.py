# === FILE: utils/transfer.py ===
def transfer_data_with_progress(sf, adb, object_name, collection_name, field_mappings, *, is_edge=False, batch_size=1000, progress_callback=None):
    try:
        # Build SOQL query
        fields = list(field_mappings.keys())
        soql = f"SELECT {', '.join(fields)} FROM {object_name}"
        
        # Query Salesforce
        print(f"Querying Salesforce: {soql}")
        records = sf.query_all(soql)['records']
        total = len(records)
        
        print(f"Found {total} records for {object_name}")
        
        if not total:
            print(f"No records found for {object_name}")
            return 0
        
        # Get collection (it should already exist)
        collection = adb.collection(collection_name)
        
        # Process records in batches
        processed = 0
        for i in range(0, total, batch_size):
            batch = records[i:i + batch_size]
            docs = []
            
            for idx, record in enumerate(batch):
                doc = {}
                
                # Map fields according to field_mappings
                for sf_field, arango_field in field_mappings.items():
                    doc[arango_field] = record.get(sf_field)
                
                # Set _key (required for ArangoDB)
                if 'Id' in record:
                    doc["_key"] = record['Id']
                else:
                    doc["_key"] = f"{object_name}_{i + idx}"
                
                docs.append(doc)
            
            # Insert batch
            try:
                result = collection.insert_many(docs, overwrite=True)
                processed += len(docs)
                
                # Call progress callback if provided
                if progress_callback:
                    progress_callback(processed, total)
                
                print(f"Inserted batch {i//batch_size + 1}: {len(docs)} documents")
                
            except Exception as e:
                print(f"Failed to insert batch {i//batch_size + 1}: {e}")
                raise
        
        print(f"Successfully transferred {total} records from {object_name} to {collection_name}")
        return total
        
    except Exception as e:
        print(f"Transfer failed for {object_name}: {e}")
        raise Exception(f"Transfer failed for {object_name}: {e}")
