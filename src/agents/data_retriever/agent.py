import os
import yaml
from google.adk.agents import Agent
from src.tools.custom_tools import mock_get_table_schema, run_query

def create_data_retriever_agent() -> Agent:
    """
    Creates the DataRetriever agent, loading its prompt from the YAML config
    and binding the required Python tools.
    """
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "config", "data_retriever.yaml"))

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
        tools=[mock_get_table_schema, run_query],
    )

# Required by ADK web when this agent is selected standalone
root_agent = create_data_retriever_agent()
