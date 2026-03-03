import json
from typing import List, Dict

def parse_schema_description(schema_json: str) -> List[Dict[str, str]]:
    """
    Custom pure Python tool for the AssetDiscovery agent.
    Parses a raw JSON schema description into a clean list of columns and types.
    """
    try:
        raw_schema = json.loads(schema_json)
        clean_schema = []
        for field in raw_schema.get("fields", []):
            clean_schema.append({
                "name": field.get("name", "unknown"),
                "type": field.get("type", "unknown"),
                "description": field.get("description", "")
            })
        return clean_schema
    except Exception as e:
        return [{"error": f"Failed to parse schema: {str(e)}"}]

def mock_get_table_schema(project: str, dataset: str, table: str) -> str:
    """Mock function representing an ADK GCP BigQuery Plugin capability."""
    mock_db = {
        "sales": json.dumps({
            "fields": [
                {"name": "transaction_id", "type": "STRING", "description": "Unique ID"},
                {"name": "amount", "type": "FLOAT", "description": "Sale amount"},
                {"name": "region", "type": "STRING", "description": "Sales region"}
            ]
        }),
        "users": json.dumps({
            "fields": [
                {"name": "user_id", "type": "STRING", "description": "Unique ID"},
                {"name": "signup_date", "type": "TIMESTAMP", "description": "Date of registration"},
                {"name": "is_active", "type": "BOOLEAN", "description": "Active status"}
            ]
        })
    }
    return mock_db.get(table, json.dumps({"fields": []}))
