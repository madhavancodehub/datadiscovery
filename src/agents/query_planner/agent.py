import os
import yaml
from google.adk.agents import Agent

def create_query_planner_agent() -> Agent:
    """
    Creates the QueryPlanner agent, loading its prompt from the YAML config.
    Sub-agents (e.g. AssetDiscovery for routing) are attached by the orchestrator.
    """
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "config", "query_planner.yaml"))

    with open(config_path, "r", encoding="utf-8") as f:
        raw_config = yaml.safe_load(f)

    model = os.getenv("MODEL_ID", "gemini-1.5-pro")
    if raw_config.get("model") not in (None, "${MODEL_ID}"):
        model = raw_config["model"]

    return Agent(
        name=raw_config["name"],
        description=raw_config.get("description", ""),
        model=model,
        instruction=raw_config.get("instruction", ""),
    )

# Required by ADK web when this agent is selected standalone
root_agent = create_query_planner_agent()
