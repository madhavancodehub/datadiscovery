import sys
import os

# ADK web imports `orchestrator` as a top-level package from `src/agents/`,
# so `datadiscovery/` (the project root) is never on sys.path.
# We insert it here so that all `src.*` absolute imports in agent.py resolve.
_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from . import agent
