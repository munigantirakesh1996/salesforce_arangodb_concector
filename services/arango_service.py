# === FILE: services/arango_service.py ===
from arango import ArangoClient

def connect_arango(hosts, username, password):
    try:
        client = ArangoClient(hosts=hosts)
        return client
    except Exception as e:
        raise Exception(f"ArangoDB connection failed: {e}")

def ensure_db(arango_client, dbname, username, password):
    try:
        sys_db = arango_client.db("_system", username=username, password=password)
        if not sys_db.has_database(dbname):
            sys_db.create_database(dbname)
        return arango_client.db(dbname, username=username, password=password)
    except Exception as e:
        raise Exception(f"Failed to ensure database {dbname}: {e}")

def ensure_collection(db, collection_name, is_edge=False):
    try:
        if not db.has_collection(collection_name):
            if is_edge:
                # Create edge collection
                collection = db.create_collection(collection_name, edge=True)
                print(f"Created edge collection: {collection_name}")
            else:
                # Create document collection
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