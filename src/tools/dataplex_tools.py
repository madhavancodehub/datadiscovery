"""
Dataplex catalog tool functions for use as native ADK agent tools.

These functions expose the same interface as the Dataplex MCP server tools
(search_entries / get_entry_details) but are called directly by the
DataplexExpert ADK agent — no MCP protocol or subprocess lifecycle needed.

The Dataplex MCP server (catalog_servers/dataplex_mcp_server.py) remains
valid as a standalone server for external MCP clients. This module is the
ADK-native equivalent for use inside the agent framework.
"""

import os
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def _get_client():
    """Lazy-initialise the Dataplex CatalogServiceClient using ADC."""
    from google.cloud import dataplex_v1
    return dataplex_v1.CatalogServiceClient()


def _normalise_entry(entry) -> dict:
    """Convert a Dataplex Entry proto to a plain dict the LLM can reason over."""
    source = getattr(entry, "entry_source", None)
    return {
        "id":                  entry.name,
        "display_name":        source.display_name if source else "",
        "entry_type":          entry.entry_type,
        "description":         source.description if source else "",
        "fully_qualified_name": getattr(entry, "fully_qualified_name", ""),
        "create_time":         entry.create_time.isoformat() if getattr(entry, "create_time", None) else None,
        "update_time":         entry.update_time.isoformat() if getattr(entry, "update_time", None) else None,
        "aspects":             _extract_aspects(entry),
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


def search_entries(query: str, max_results: int = 10, semantic: bool = True) -> list:
    """
    Search Google Cloud Dataplex Universal Catalog for data assets.

    Uses CatalogServiceClient.search_entries() with optional semantic search.
    Requires GOOGLE_CLOUD_PROJECT env var and dataplex.projects.search IAM permission.

    Args:
        query: Free-text or structured query (see Dataplex search syntax).
               Examples: "sales data", "type=bigquery_table tag:PII", "dataset:marketing"
        max_results: Maximum number of results to return (1–50, default 10).
        semantic: If True, uses semantic (intent-aware) search. Default True.

    Returns:
        List of matching catalog entries with id, display_name, entry_type,
        description, fully_qualified_name, and aspects.
    """
    project = os.getenv("GOOGLE_CLOUD_PROJECT")
    if not project:
        return [{"error": "GOOGLE_CLOUD_PROJECT environment variable is not set."}]

    try:
        from google.cloud import dataplex_v1
        client = _get_client()

        request = dataplex_v1.SearchEntriesRequest(
            name=f"projects/{project}/locations/global",
            query=query,
            page_size=min(max(1, max_results), 50),
            order_by="relevance",
        )

        response = client.search_entries(request=request)
        results = []
        for result in response.results:
            if hasattr(result, "dataplex_entry") and result.dataplex_entry:
                results.append(_normalise_entry(result.dataplex_entry))
        return results

    except Exception as e:
        logger.error("Dataplex search_entries failed: %s", e)
        return [{"error": str(e)}]


def get_entry_details(entry_name: str) -> dict:
    """
    Fetch full metadata for a specific Dataplex catalog entry by resource name.

    Use this after search_entries() to enrich an asset with its full schema,
    data quality aspects, business metadata, and lineage information.

    Args:
        entry_name: Fully qualified resource name of the entry.
                    (Use the 'id' field returned by search_entries.)

    Returns:
        Full entry dict including all aspects (schema, data quality, etc.)
    """
    try:
        from google.cloud import dataplex_v1
        client = _get_client()

        request = dataplex_v1.GetEntryRequest(
            name=entry_name,
            view=dataplex_v1.EntryView.FULL,
        )
        entry = client.get_entry(request=request)
        return _normalise_entry(entry)

    except Exception as e:
        logger.error("Dataplex get_entry_details failed for %s: %s", entry_name, e)
        return {"error": str(e)}
