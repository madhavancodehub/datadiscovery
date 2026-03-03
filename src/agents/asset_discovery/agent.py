import os
import yaml
from google.adk.agents import Agent
from google.adk.agents.llm_agent_config import LlmAgentConfig
from src.tools.mock_mcp_server import search_catalog

def create_asset_discovery_agent() -> Agent:
    """
    Creates the AssetDiscovery agent natively from the ADK YAML config,
    and binds the required python tools.
    """
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "config", "asset_discovery.yaml"))
    
    with open(config_path, "r", encoding="utf-8") as f:
        raw_config = yaml.safe_load(f)
        
    if raw_config.get("model") == "${MODEL_ID}":
        raw_config["model"] = os.getenv("MODEL_ID", "gemini-1.5-pro")
        
    config = LlmAgentConfig.parse_obj(raw_config)
    agent = Agent.from_config(config=config, config_abs_path=config_path)
    
    # Attach dependencies
    agent.tools = [search_catalog]
    
    return agent
