

# === FILE: services/arango_service.py ===
from arango.client import ArangoClient

def connect_arango(hosts, username, password):
    try:
        client = ArangoClient(hosts=hosts)
        return client
    except Exception as e:
        raise Exception(f"ArangoDB connection failed: {e}")

def ensure_db(arango_client, dbname, username, password):
    try:
        sys_db = arango_client.db("_system", username=username, password=password)
        
        # Check if database exists, create if not
        if not sys_db.has_database(dbname):
            sys_db.create_database(dbname)
            print(f"Created database: {dbname}")
        else:
            print(f"Database {dbname} already exists")
            
        # Connect to the target database
        target_db = arango_client.db(dbname, username=username, password=password)
        
        # Verify connection by attempting a simple operation
        target_db.properties()
        print(f"Successfully connected to database: {dbname}")
        
        return target_db
    except Exception as e:
        raise Exception(f"Failed to ensure database {dbname}: {e}")

def ensure_collection(db, collection_name, is_edge=False):
    try:
        if not db.has_collection(collection_name):
            if is_edge:
                collection = db.create_collection(collection_name, edge=True)
                print(f"Created edge collection: {collection_name}")
            else:
                collection = db.create_collection(collection_name, edge=False)
                print(f"Created document collection: {collection_name}")
        else:
            print(f"Collection {collection_name} already exists")
            collection = db.collection(collection_name)
        
        # Verify collection exists and return it
        if db.has_collection(collection_name):
            return db.collection(collection_name)
        else:
            raise Exception(f"Collection {collection_name} was not created properly")
            
    except Exception as e:
        raise Exception(f"Failed to ensure collection {collection_name}: {e}")

def get_collections(db):
    """Get all collections from ArangoDB database"""
    try:
        collections = db.collections()
        # Filter out system collections and return only user collections
        user_collections = [col['name'] for col in collections if not col['name'].startswith('_')]
        return user_collections
    except Exception as e:
        raise Exception(f"Failed to get ArangoDB collections: {e}")

def get_collection_fields(db, collection_name):
    """Get field names from a collection by sampling documents"""
    try:
        collection = db.collection(collection_name)
        
        # Get a sample document to determine fields
        cursor = collection.all(limit=1)
        sample_docs = list(cursor)
        
        if not sample_docs:
            return []
        
        # Get all fields from the sample document, excluding ArangoDB system fields
        sample_doc = sample_docs[0]
        fields = [key for key in sample_doc.keys() if not key.startswith('_')]
        
        return fields
    except Exception as e:
        raise Exception(f"Failed to get fields for collection {collection_name}: {e}")

def get_collection_data(db, collection_name, limit=100):
    """Get data from a collection with optional limit"""
    try:
        collection = db.collection(collection_name)
        if limit == 0:
            # No limit - get all documents
            cursor = collection.all()
        else:
            cursor = collection.all(limit=limit)
        return list(cursor)
    except Exception as e:
        raise Exception(f"Failed to get data from collection {collection_name}: {e}")
