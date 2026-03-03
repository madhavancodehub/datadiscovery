import os
import json
from dotenv import load_dotenv

# Load environment variables early
load_dotenv()

from google.adk.agents.sequential_agent import SequentialAgent
from google.adk.agents.callback_context import CallbackContext
from google.adk.models.llm_response import LlmResponse

from src.agents.asset_discovery.agent import create_asset_discovery_agent
from src.agents.query_planner.agent import create_query_planner_agent
from src.agents.data_retriever.agent import create_data_retriever_agent
from src.agents.context_builder.agent import create_context_builder_agent

def build_data_discovery_workflow() -> SequentialAgent:
    """
    Builds the main routing workflow, manually composing agents that were 
    loaded natively from their individual ADK YAML configurations.
    """
    
    asset_discovery = create_asset_discovery_agent()
    query_planner = create_query_planner_agent()
    data_retriever = create_data_retriever_agent()
    context_builder = create_context_builder_agent()

    # Configure Agent2Agent capabilities explicitly:
    # We give the QueryPlanner the ability to route back to AssetDiscovery (Feature 12)
    query_planner.sub_agents = [asset_discovery]

    router = SequentialAgent(
        name="DataDiscoveryRouter",
        description="A multi-agent routing pipeline that discovers assets, plans queries, executes them, and builds a synthesized context narrative.",
        sub_agents=[
            asset_discovery,
            query_planner, 
            data_retriever,
            context_builder
        ],
    )
    return router

# ADK Agent Engine requires `root_agent` to be defined at module level
root_agent = build_data_discovery_workflow()
