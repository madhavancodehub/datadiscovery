# Dataplex Universal Catalog MCP Server
# Exposes Dataplex CatalogServiceClient as MCP tools via FastMCP (stdio transport).
# ADK agents connect to this server via McpToolset + StdioConnectionParams.
# Run standalone: python dataplex_mcp_server.py
#
# KEY DESIGN NOTE:
#   The Dataplex client and google.cloud.dataplex_v1 are imported EAGERLY at
#   module level so all gRPC channel setup, credential loading, and protobuf
#   initialization happen during server startup — before any tool call arrives.
#   This prevents the MCP 30-second request timeout from being hit on first use.

import os
import logging
from dotenv import load_dotenv
from fastmcp import FastMCP

load_dotenv()

logging.basicConfig(level=logging.WARNING)   # WARNING only — avoid stdout pollution
logger = logging.getLogger(__name__)

# ── Eager initialisation ────────────────────────────────────────────────────
# Import and create the Dataplex client at startup, not inside tool functions.
# This trades a slightly slower server startup for fast, predictable tool calls.
try:
    from google.cloud import dataplex_v1 as _dataplex_v1
    _catalog_client = _dataplex_v1.CatalogServiceClient()
    logger.warning("Dataplex CatalogServiceClient initialised.")
except Exception as _init_err:
    _dataplex_v1 = None          # type: ignore[assignment]
    _catalog_client = None
    logger.warning("Dataplex client init failed: %s", _init_err)

_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "")

mcp = FastMCP("DataplexCatalogServer")


# ── Helpers ─────────────────────────────────────────────────────────────────

def _normalise_entry(entry) -> dict:
    """Convert a Dataplex Entry proto to a plain dict."""
    source = entry.entry_source if hasattr(entry, "entry_source") else None
    return {
        "id":                   entry.name,
        "display_name":         source.display_name if source else "",
        "entry_type":           entry.entry_type,
        "description":          source.description if source else "",
        "fully_qualified_name": getattr(entry, "fully_qualified_name", ""),
        "create_time":          entry.create_time.isoformat() if getattr(entry, "create_time", None) else None,
        "update_time":          entry.update_time.isoformat() if getattr(entry, "update_time", None) else None,
        "aspects":              _extract_aspects(entry),
    }


def _extract_aspects(entry) -> dict:
    aspects = {}
    if not hasattr(entry, "aspects"):
        return aspects
    for key, aspect in entry.aspects.items():
        try:
            data = type(aspect).to_dict(aspect) if hasattr(type(aspect), "to_dict") else {}
            aspects[key] = data.get("data", {})
        except Exception:
            aspects[key] = {}
    return aspects


# ── MCP Tools ────────────────────────────────────────────────────────────────

@mcp.tool()
async def search_entries(query: str, max_results: int = 10) -> list:
    """
    Search Google Cloud Dataplex Universal Catalog for data assets.

    Args:
        query: Free-text search query. Examples: "sales data", "user profiles",
               "type=bigquery_table", "dataset:marketing tag:PII"
        max_results: Maximum entries to return (1-50, default 10).

    Returns:
        List of matching catalog entries. Each entry has: id, display_name,
        entry_type, description, fully_qualified_name, create_time, aspects.
    """
    if not _PROJECT:
        return [{"error": "GOOGLE_CLOUD_PROJECT environment variable is not set."}]
    if _catalog_client is None:
        return [{"error": "Dataplex client failed to initialise. Check logs."}]

    try:
        request = _dataplex_v1.SearchEntriesRequest(
            name=f"projects/{_PROJECT}/locations/global",
            query=query,
            page_size=min(max(1, max_results), 50),
            order_by="relevance",
        )
        response = _catalog_client.search_entries(request=request)
        results = [
            _normalise_entry(r.dataplex_entry)
            for r in response.results
            if getattr(r, "dataplex_entry", None)
        ]
        return results

    except Exception as e:
        logger.error("search_entries failed: %s", e)
        return [{"error": str(e)}]


@mcp.tool()
async def get_entry_details(entry_name: str) -> dict:
    """
    Fetch complete metadata for a specific Dataplex catalog entry.

    Use this after search_entries() to get the full schema, aspects (data
    quality, ownership, business glossary), and lineage for an asset.

    Args:
        entry_name: The 'id' field from a search_entries() result.

    Returns:
        Full entry dict with all aspects populated.
    """
    if _catalog_client is None:
        return {"error": "Dataplex client failed to initialise. Check logs."}

    try:
        request = _dataplex_v1.GetEntryRequest(
            name=entry_name,
            view=_dataplex_v1.EntryView.FULL,
        )
        entry = _catalog_client.get_entry(request=request)
        return _normalise_entry(entry)

    except Exception as e:
        logger.error("get_entry_details failed for %s: %s", entry_name, e)
        return {"error": str(e)}


if __name__ == "__main__":
    # ADK spawns this script via StdioConnectionParams; FastMCP handles stdio.
    mcp.run(transport="stdio")
