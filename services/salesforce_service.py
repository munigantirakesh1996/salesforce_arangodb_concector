

# === FILE: services/salesforce_service.py ===
from simple_salesforce.api import Salesforce

def connect_salesforce(username, password, token, instance_url):
    try:
        sf = Salesforce(
            username=username,
            password=password,
            security_token=token,
            instance_url=instance_url,
            version="60.0"
        )
        # Test connection by making a simple query
        sf.query("SELECT Id FROM User LIMIT 1")
        return sf
    except Exception as e:
        raise Exception(f"Salesforce connection failed: {e}")

def get_items(sf):
    try:
        return [obj['name'] for obj in sf.describe()['sobjects'] if obj['queryable']]
    except Exception as e:
        raise Exception(f"Failed to get Salesforce objects: {e}")

def get_sf_fields(sf, object_name):
    try:
        return [f["name"] for f in sf.__getattr__(object_name).describe()["fields"]]
    except Exception as e:
        raise Exception(f"Failed to get fields for {object_name}: {e}")

def get_sf_field_types(sf, object_name):
    """Get field types for a Salesforce object"""
    try:
        fields = sf.__getattr__(object_name).describe()["fields"]
        field_types = {}
        for field in fields:
            field_types[field["name"]] = field["type"]
        return field_types
    except Exception as e:
        raise Exception(f"Failed to get field types for {object_name}: {e}")

def insert_salesforce_records(sf, object_name, records, batch_size=200):
    """Insert records into Salesforce object"""
    try:
        # Get the Salesforce object
        sf_object = sf.__getattr__(object_name)
        
        # Process records in batches
        total_inserted = 0
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            
            # Insert batch
            result = sf_object.insert(batch)
            
            # Count successful insertions
            if isinstance(result, list):
                successful = sum(1 for r in result if r.get('success', False))
            else:
                successful = 1 if result.get('success', False) else 0
                
            total_inserted += successful
            
        return total_inserted
    except Exception as e:
        raise Exception(f"Failed to insert records into {object_name}: {e}")

def update_salesforce_records(sf, object_name, records, batch_size=200):
    """Update existing records in Salesforce object"""
    try:
        # Get the Salesforce object
        sf_object = sf.__getattr__(object_name)
        
        # Process records in batches
        total_updated = 0
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            
            # Update batch
            result = sf_object.update(batch)
            
            # Count successful updates
            if isinstance(result, list):
                successful = sum(1 for r in result if r.get('success', False))
            else:
                successful = 1 if result.get('success', False) else 0
                
            total_updated += successful
            
        return total_updated
    except Exception as e:
        raise Exception(f"Failed to update records in {object_name}: {e}")

def upsert_salesforce_records(sf, object_name, records, external_id_field, batch_size=200):
    """Upsert records in Salesforce object using external ID"""
    try:
        # Get the Salesforce object
        sf_object = sf.__getattr__(object_name)
        
        # Process records in batches
        total_upserted = 0
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            
            # Upsert batch
            result = sf_object.upsert(external_id_field, batch)
            
            # Count successful upserts
            if isinstance(result, list):
                successful = sum(1 for r in result if r.get('success', False))
            else:
                successful = 1 if result.get('success', False) else 0
                
            total_upserted += successful
            
        return total_upserted
    except Exception as e:
        raise Exception(f"Failed to upsert records in {object_name}: {e}")
