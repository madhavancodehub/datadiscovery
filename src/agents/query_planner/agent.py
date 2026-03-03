import os
import yaml
from google.adk.agents import Agent
from google.adk.agents.llm_agent_config import LlmAgentConfig

def create_query_planner_agent() -> Agent:
    """
    Creates the QueryPlanner agent natively from the ADK YAML config.
    Sub-agents are loaded automatically by ADK if defined in the YAML.
    """
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "config", "query_planner.yaml"))
    
    with open(config_path, "r", encoding="utf-8") as f:
        raw_config = yaml.safe_load(f)
        
    if raw_config.get("model") == "${MODEL_ID}":
        raw_config["model"] = os.getenv("MODEL_ID", "gemini-1.5-pro")
        
    config = LlmAgentConfig.parse_obj(raw_config)
    agent = Agent.from_config(config=config, config_abs_path=config_path)
    
    return agent
