from mcp.server.fastmcp import FastMCP
from typing import List

# Initialize the mock MCP server for the data catalog
mcp = FastMCP("MockCatalogServer")

# Mock catalog data — mirrors the shape returned by the real Dataplex MCP server
CATALOG = [
    {
        "id": "projects/mock-project/locations/us-central1/entryGroups/@bigquery/entries/project.dataset.sales",
        "display_name": "sales",
        "entry_type": "bigquery_table",
        "description": "Daily transactional sales data aggregated per region.",
        "fully_qualified_name": "bigquery:project.dataset.sales",
        "aspects": {
            "schema": {
                "fields": [
                    {"name": "transaction_id", "type": "STRING"},
                    {"name": "amount", "type": "FLOAT"},
                    {"name": "region", "type": "STRING"},
                ]
            }
        },
    },
    {
        "id": "projects/mock-project/locations/us-central1/entryGroups/@bigquery/entries/project.dataset.users",
        "display_name": "users",
        "entry_type": "bigquery_table",
        "description": "Core user dimension table with activity status.",
        "fully_qualified_name": "bigquery:project.dataset.users",
        "aspects": {
            "schema": {
                "fields": [
                    {"name": "user_id", "type": "STRING"},
                    {"name": "signup_date", "type": "TIMESTAMP"},
                    {"name": "is_active", "type": "BOOLEAN"},
                ]
            }
        },
    },
]


@mcp.tool()
def search_entries(query: str, max_results: int = 10, semantic: bool = True) -> List[dict]:
    """
    Search the mock enterprise catalog for data assets matching the query.
    Mirrors the interface of the real Dataplex MCP server (search_entries tool).

    Args:
        query: Free-text search query.
        max_results: Maximum number of results to return.
        semantic: Ignored in mock — included for interface compatibility.

    Returns:
        List of matching catalog entries.
    """
    query = query.lower()
    results = []
    for asset in CATALOG:
        if (
            query in asset["description"].lower()
            or query in asset["display_name"].lower()
            or query in asset["entry_type"].lower()
            or query in asset["id"].lower()
        ):
            results.append(asset)
    return results[:max_results]


@mcp.tool()
def get_entry_details(entry_name: str) -> dict:
    """
    Fetch full metadata for a specific mock catalog entry by resource name.
    Mirrors the interface of the real Dataplex MCP server (get_entry_details tool).

    Args:
        entry_name: The 'id' field from a search_entries result.

    Returns:
        Full entry dict with schema aspects.
    """
    for asset in CATALOG:
        if asset["id"] == entry_name or asset["fully_qualified_name"] == entry_name:
            return asset
    return {"error": f"Entry '{entry_name}' not found in mock catalog."}


if __name__ == "__main__":
    # Start the MCP server when run directly (stdio transport for ADK McpToolset)
    mcp.run(transport='stdio')
