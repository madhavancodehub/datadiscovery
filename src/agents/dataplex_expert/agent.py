"""
DataplexExpert agent — connects to the Dataplex Universal Catalog MCP server
as a proper MCP client using ADK 1.x McpToolset.

Architecture:
  ┌─────────────────────┐      stdio (subprocess)      ┌──────────────────────────────┐
  │  DataplexExpert     │ ──────────────────────────►  │  dataplex_mcp_server.py      │
  │  (LlmAgent)         │      McpToolset              │  (FastMCP, stdio transport)  │
  │                     │ ◄──────────────────────────  │  search_entries              │
  └─────────────────────┘      MCPTool instances       │  get_entry_details           │
                                                        └──────────────────────────────┘
                                                               │
                                                               ▼
                                                   CatalogServiceClient (Dataplex API)

The MCP server subprocess is spawned automatically by McpToolset when the agent
first handles a request, and is managed by the ADK agent framework lifecycle.
No manual event loop management needed — ADK 1.x handles this internally.
"""

import os
import sys
import yaml

from google.adk.agents import LlmAgent
from google.adk.tools.mcp_tool.mcp_toolset import McpToolset
from google.adk.tools.mcp_tool.mcp_session_manager import StdioConnectionParams
from mcp import StdioServerParameters

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))

# Path to the Dataplex MCP server script
# Structure: src/agents/dataplex_expert/agent.py
#            src/catalog_servers/dataplex_mcp_server.py
_DATAPLEX_SERVER_SCRIPT = os.path.abspath(
    os.path.join(_THIS_DIR, "..", "..", "catalog_servers", "dataplex_mcp_server.py")
)

_CONFIG_PATH = os.path.abspath(
    os.path.join(_THIS_DIR, "..", "..", "config", "dataplex_expert.yaml")
)


def create_dataplex_expert_agent() -> LlmAgent:
    """
    Creates the DataplexExpert agent.

    McpToolset connects to the Dataplex MCP server via stdio transport.
    ADK 1.x manages the subprocess lifecycle automatically — the server is
    spawned when the toolset is first used and shut down with the agent.

    The MCP server exposes:
      - search_entries(query, max_results, semantic)
      - get_entry_details(entry_name)
    """
    with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
        raw_config = yaml.safe_load(f)

    model = os.getenv("MODEL_ID", "gemini-1.5-pro")
    if raw_config.get("model") not in (None, "${MODEL_ID}"):
        model = raw_config["model"]

    return LlmAgent(
        name=raw_config["name"],
        description=raw_config.get("description", ""),
        model=model,
        instruction=raw_config.get("instruction", ""),
        tools=[
            McpToolset(
                connection_params=StdioConnectionParams(
                    server_params=StdioServerParameters(
                        # sys.executable ensures the subprocess uses the same
                        # venv and inherits all env vars (GOOGLE_CLOUD_PROJECT etc.)
                        command=sys.executable,
                        args=[_DATAPLEX_SERVER_SCRIPT],
                    ),
                    timeout=60,
                ),
                # Only expose the two catalog tools to this agent
                tool_filter=["search_entries", "get_entry_details"],
            )
        ],
    )


# ADK web discovers this as standalone agent
root_agent = create_dataplex_expert_agent()
