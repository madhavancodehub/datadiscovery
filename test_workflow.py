import asyncio
import uuid
import os
from dotenv import load_dotenv
from google.genai import types
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from src.agents.orchestrator.agent import root_agent

# Load environment variables
load_dotenv()

async def run_data_discovery(query: str):
    print("="*60)
    print("Multi-Agent Discovery Router (Google ADK Native)")
    print("="*60)
    
    # Verify Vertex AI is configured
    vertex_ai_flag = os.getenv("GOOGLE_GENAI_USE_VERTEXAI", "").lower()
    if vertex_ai_flag not in ("1", "true"):
         print("WARNING: GOOGLE_GENAI_USE_VERTEXAI is not set to 1 or true. The pipeline may attempt to use an API Key instead of Vertex AI.")
    if not os.getenv("GOOGLE_CLOUD_PROJECT"):
         print("ERROR: GOOGLE_CLOUD_PROJECT is required for Vertex AI mode. Please set it in your .env file.")
         return
         
    print(f"Executing against Project: {os.getenv('GOOGLE_CLOUD_PROJECT')} ({os.getenv('GOOGLE_CLOUD_LOCATION', 'us-central1')})")
    print(f"Query: '{query}'\n")

    # 1. Initialize ADK Session Service for memory retention
    session_service = InMemorySessionService()
    user_id = "analyst_beta"
    session_id = f"session_{uuid.uuid4().hex}"
    
    # Must explicitly call synchronous session creation
    session_service.create_session_sync(app_name="DataDiscovery", user_id=user_id, session_id=session_id)

    # 2. Initialize Runner to handle the workflow
    runner = Runner(agent=root_agent, app_name="DataDiscovery", session_service=session_service)

    # 3. Process exactly as systemdesigner does (streaming events)
    content = types.Content(role="user", parts=[types.Part(text=query)])
    
    print("--- Execution Log ---")
    async for event in runner.run_async(user_id=user_id, session_id=session_id, new_message=content):
        if event.content and event.content.parts:
            part = event.content.parts[0]
            if part.text:
                source = getattr(event, "author", "System")
                if source != "user":
                   print(f"[{source}]: {part.text.strip()[:150]}...") # Truncate for terminal 
            elif part.function_call:
                print(f"[System]: Tool Called -> {part.function_call.name}")
        
    print("\n--- Final Session History ---")
    
    # Since we are not in an async context here, we can try to fetch synchronously to print the recap
    session = session_service.get_session_sync(session_id=session_id, app_name="DataDiscovery", user_id=user_id)
    if hasattr(session, "history"):
        for item in session.history:
              role = getattr(item, "author", getattr(item, "role", "unknown"))
              if hasattr(item.content, "parts") and item.content.parts:
                   text = item.content.parts[0].text
                   if text:
                        print(f"[{role}] -> {text[:100]}...")
    elif hasattr(session, "events"):
         print(f"Session events length: {len(session.events)}")

    # Feature 11: Artifacts
    if session and hasattr(session, "state"):
        prompt_pkg = session.state.get("prompt_package")
        if prompt_pkg:
            artifact_dir = os.path.join(os.getcwd(), "docs")
            os.makedirs(artifact_dir, exist_ok=True)
            artifact_path = os.path.join(artifact_dir, "prompt_package_artifact.json")
            with open(artifact_path, "w", encoding="utf-8") as f:
                import json
                # Ensure the Pydantic model is serialized to dict
                if hasattr(prompt_pkg, "model_dump"):
                     json.dump(prompt_pkg.model_dump(), f, indent=2)
                else:
                     json.dump(prompt_pkg, f, indent=2)
            print(f"\n[Artifact Output] Saved PromptPackage securely to {artifact_path}")
        else:
            print("\n[Artifact Output] State did not contain 'prompt_package'.")

if __name__ == "__main__":
    test_query = "Show me the total sales per region combining user data to verify active statuses."
    asyncio.run(run_data_discovery(test_query))
