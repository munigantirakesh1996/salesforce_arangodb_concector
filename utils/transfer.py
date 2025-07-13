
# === FILE: utils/transfer.py ===
import sys
import json

def calculate_data_size(data):
    """Calculate the size of data in MB"""
    try:
        if isinstance(data, list):
            # Convert to JSON string and calculate size
            json_str = json.dumps(data)
            size_bytes = len(json_str.encode('utf-8'))
            size_mb = size_bytes / (1024 * 1024)
            return size_mb
        else:
            return 0.0
    except Exception as e:
        print(f"Error calculating data size: {e}")
        return 0.0

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
            return 0, 0.0
        
        # Get collection (it should already exist)
        collection = adb.collection(collection_name)
        
        # Process records in batches
        processed = 0
        all_docs = []
        
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
                all_docs.append(doc)
            
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
        
        # Calculate data size
        data_size_mb = calculate_data_size(all_docs)
        
        print(f"Successfully transferred {total} records from {object_name} to {collection_name}")
        print(f"Data size: {data_size_mb:.2f} MB")
        
        return total, data_size_mb
        
    except Exception as e:
        print(f"Transfer failed for {object_name}: {e}")
        raise Exception(f"Transfer failed for {object_name}: {e}")

def transfer_arango_to_salesforce_with_progress(adb, sf, collection_name, object_name, field_mappings, operation_type="insert", external_id_field=None, *, batch_size=200, progress_callback=None):
    """
    Transfer data from ArangoDB to Salesforce
    
    Args:
        adb: ArangoDB database connection
        sf: Salesforce connection
        collection_name: ArangoDB collection name
        object_name: Salesforce object name
        field_mappings: Dictionary mapping ArangoDB fields to Salesforce fields
        operation_type: "insert", "update", or "upsert"
        external_id_field: Field to use for upsert operations
        batch_size: Number of records to process in each batch
        progress_callback: Callback function for progress updates
    """
    try:
        from services.arango_service import get_collection_data
        from services.salesforce_service import insert_salesforce_records, update_salesforce_records, upsert_salesforce_records
        
        # Get all data from ArangoDB collection
        print(f"Fetching data from ArangoDB collection: {collection_name}")
        arango_docs = get_collection_data(adb, collection_name, limit=0)  # Get all records (0 means no limit)
        total = len(arango_docs)
        
        print(f"Found {total} records in {collection_name}")
        
        if not total:
            print(f"No records found in {collection_name}")
            return 0, 0.0
        
        # Transform ArangoDB documents to Salesforce records
        sf_records = []
        for doc in arango_docs:
            record = {}
            
            # Map fields according to field_mappings
            for arango_field, sf_field in field_mappings.items():
                if arango_field in doc:
                    record[sf_field] = doc[arango_field]
            
            # Add Id field if it exists in the document
            if '_key' in doc:
                record['Id'] = doc['_key']
            
            sf_records.append(record)
        
        # Process records based on operation type
        processed = 0
        
        if operation_type == "insert":
            processed = insert_salesforce_records(sf, object_name, sf_records, batch_size)
        elif operation_type == "update":
            processed = update_salesforce_records(sf, object_name, sf_records, batch_size)
        elif operation_type == "upsert":
            if not external_id_field:
                raise Exception("External ID field is required for upsert operations")
            processed = upsert_salesforce_records(sf, object_name, sf_records, external_id_field, batch_size)
        else:
            raise Exception(f"Invalid operation type: {operation_type}")
        
        # Calculate data size
        data_size_mb = calculate_data_size(sf_records)
        
        print(f"Successfully transferred {processed} records from {collection_name} to {object_name}")
        print(f"Data size: {data_size_mb:.2f} MB")
        
        return processed, data_size_mb
        
    except Exception as e:
        print(f"Transfer failed for {collection_name}: {e}")
        raise Exception(f"Transfer failed for {collection_name}: {e}")

def preview_arango_data(adb, collection_name, field_mappings, limit=20):
    """
    Preview ArangoDB data transformed for Salesforce
    
    Args:
        adb: ArangoDB database connection
        collection_name: ArangoDB collection name
        field_mappings: Dictionary mapping ArangoDB fields to Salesforce fields
        limit: Number of records to preview
    
    Returns:
        Tuple of (original_arango_docs, transformed_sf_records)
    """
    try:
        from services.arango_service import get_collection_data
        
        # Get sample data from ArangoDB
        arango_docs = get_collection_data(adb, collection_name, limit=limit)
        
        # Transform to Salesforce format
        sf_records = []
        for doc in arango_docs:
            record = {}
            
            # Map fields according to field_mappings
            for arango_field, sf_field in field_mappings.items():
                if arango_field in doc:
                    record[sf_field] = doc[arango_field]
            
            # Add Id field if it exists in the document
            if '_key' in doc:
                record['Id'] = doc['_key']
            
            sf_records.append(record)
        
        return arango_docs, sf_records
        
    except Exception as e:
        print(f"Preview failed for {collection_name}: {e}")
        raise Exception(f"Preview failed for {collection_name}: {e}")

