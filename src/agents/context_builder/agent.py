import os
import yaml
from google.adk.agents import Agent
from google.adk.agents.llm_agent_config import LlmAgentConfig
from google.genai import types
from google.adk.agents.callback_context import CallbackContext
from google.adk.models.llm_response import LlmResponse

def logging_callback(callback_context: CallbackContext, llm_response: LlmResponse) -> None:
    """A sample callback to log model outputs and emit traces natively in ADK."""
    print(f"\n[ADK TRACE] Generated response length: {len(str(llm_response))}")
    return None

def create_context_builder_agent() -> Agent:
    """
    Creates the ContextBuilder agent natively from the ADK YAML config,
    and binds grounding and callbacks.
    """
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "config", "context_builder.yaml"))
    
    with open(config_path, "r", encoding="utf-8") as f:
        raw_config = yaml.safe_load(f)
        
    if raw_config.get("model") == "${MODEL_ID}":
        raw_config["model"] = os.getenv("MODEL_ID", "gemini-1.5-pro")
        
    config = LlmAgentConfig.parse_obj(raw_config)
    agent = Agent.from_config(config=config, config_abs_path=config_path)
    
    # Attach Grounding natively (Feature 13)
    agent.generate_content_config = types.GenerateContentConfig(
        tools=[types.Tool(google_search=types.GoogleSearch())],
    )
    
    # Attach explicit callbacks (Feature 10)
    agent.after_model_callback = logging_callback
    
    return agent
