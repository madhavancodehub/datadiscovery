import os
import yaml
from google.adk.agents import Agent
from google.adk.tools import google_search
from google.adk.agents.callback_context import CallbackContext
from google.adk.models.llm_response import LlmResponse

def logging_callback(callback_context: CallbackContext, llm_response: LlmResponse) -> None:
    """A sample callback to log model outputs and emit traces natively in ADK."""
    print(f"\n[ADK TRACE] Generated response length: {len(str(llm_response))}")
    return None

def create_context_builder_agent() -> Agent:
    """
    Creates the ContextBuilder agent, loading its prompt from the YAML config,
    and binding grounding and callbacks.
    """
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "config", "context_builder.yaml"))

    with open(config_path, "r", encoding="utf-8") as f:
        raw_config = yaml.safe_load(f)

    model = os.getenv("MODEL_ID", "gemini-1.5-pro")
    if raw_config.get("model") not in (None, "${MODEL_ID}"):
        model = raw_config["model"]

    agent = Agent(
        name=raw_config["name"],
        description=raw_config.get("description", ""),
        model=model,
        instruction=raw_config.get("instruction", ""),
        tools=[google_search],
        after_model_callback=logging_callback,
    )

    return agent

# Required by ADK web when this agent is selected standalone
root_agent = create_context_builder_agent()
