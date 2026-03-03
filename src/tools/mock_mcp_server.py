from mcp.server.fastmcp import FastMCP
from typing import List

# Initialize the mock MCP server for the data catalog
mcp = FastMCP("MockCatalogServer")

# Mock catalog data
CATALOG = [
    {
        "id": "project.dataset.sales",
        "type": "table",
        "tags": ["PII", "Finance"],
        "description": "Daily transactional sales data aggregated per region."
    },
    {
        "id": "project.dataset.users",
        "type": "table",
        "tags": ["PII", "Core"],
        "description": "Core user dimension table with activity status."
    }
]

@mcp.tool()
def search_catalog(query: str) -> List[dict]:
    """
    Search the enterprise catalog for data assets matching the query.
    Used by the AssetDiscovery agent to find relevant tables.
    """
    query = query.lower()
    results = []
    for asset in CATALOG:
        if query in asset["description"].lower() or any(query in tag.lower() for tag in asset["tags"]) or query in asset["id"]:
            results.append(asset)
    return results

if __name__ == "__main__":
    # Start the MCP server when run directly
    mcp.run(transport='stdio')
