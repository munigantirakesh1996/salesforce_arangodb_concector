# === FILE: services/salesforce_service.py ===
from simple_salesforce import Salesforce

def connect_salesforce(username, password, token, instance_url):
    try:
        return Salesforce(
            username=username,
            password=password,
            security_token=token,
            instance_url=instance_url,
            version="60.0"
        )
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
