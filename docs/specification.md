# Multi-Agent Query App Specification (Enterprise Pattern)

## 1. Overview & Core User Journey
The system is a backend multi-agent routing architecture designed to translate natural language into query plans, execute data-retrieval scripts, and deliver a grounded answer. 

**User Journey:**
1. User inputs an analytic query (e.g., "what were the regional sales in the last 30 days and summarize active users").
2. The system queries catalog systems to discover relevant dataset assets.
3. It generates SQL and No-SQL query plans based on the available schemas.
4. It securely retrieves the necessary data.
5. It synthesizes a grounded LLM response using the retrieved data, with clear provenance.

**Key Constraints & Requirements:**
- **Performance:** Time to First Token (TTFT) is optimized through progress event streaming.
- **Accuracy:** Output must be strictly grounded using the retrieved schema, telemetry, and the organization's information architecture.
- **Responsible Execution:** Enforces strict access policies and output bounds (prevents fetching massive tables into the LLM context).
- **Composability:** Query intents can be mapped to any source, including BigQuery, Snowflake, PostgreSQL, or OpenSearch.

---

## 2. High-Level Architecture (Google ADK Native)

The architecture is built natively on the **Google Agent Development Kit (ADK)** to coordinate specialized agents, leveraging **Vertex AI** for model inference and enterprise security.

### Orchestrator
The main entry point is an ADK App leveraging a `SequentialAgent` Orchestrator. The orchestrator:
- Manages long-term conversational memory via `InMemorySessionService`.
- Triggers execution via the ADK `Runner`.
- Distributes work across specialized LLM agents in a defined order.
- Maintains the global context and authored state between agents.

### Specialized Agents
1. **AssetDiscovery Agent (`LlmAgent`)**: Parses the query and searches catalog metadata. Produces an `AssetBundle` containing schemas, lineage, glossary, and connection properties.
2. **QueryPlanner Agent (`LlmAgent`)**: Translates the `AssetBundle` into exact database constraints (e.g., BigQuery SQL). Produces a `QueryPlan`. Can use **Agent 2 Agent** protocol to request further clarification from the AssetDiscovery agent.
3. **DataRetriever Agent (`LlmAgent`)**: An action-heavy executor. Safely executes the `QueryPlan` via tool plugins and produces a heavily bounded `ResultSet`.
4. **ContextBuilder Agent (`LlmAgent`)**: The synthesizer. Consumes the retrieved data to build a strategy-driven prompt package containing the final narrative. Uses **Grounding** capabilities for citations.

### ADK Core Features Utilized
The system comprehensively utilizes the following 13 ADK primitives:
1. **Apps & Plugins:** Encapsulates the runtime logic and GCP connectors.
2. **Workflow Agents:** Uses `SequentialAgent` mapping.
3. **Agent Config:** Uses `LlmAgentConfig` and Python-based structure for instruction separation.
4. **Vertex AI Hosted Models:** Executes securely against models like `gemini-1.5-pro` via GCP Project auth.
5. **Tools and Integrations:** ADK-native mechanisms for GCP.
6. **Custom Tools & MCP:** Incorporates pure Python functions and **Model Context Protocol (MCP)** tool servers.
7. **Session Service:** Retains conversational memory across multiple invocations.
8. **Context Management:** Propagates typed Contracts across the stages.
9. **Resume Agents:** Ability to pause and yield back to the user (Human-in-the-loop) for ambiguities.
10. **Callbacks, Events & Streaming:** Real-time visibility into the pipeline.
11. **Artifacts:** Storage of outputs and intermediate specs as ADK Artifacts.
12. **Agent 2 Agent Protocol:** Direct inter-agent delegation.
13. **Grounding:** Vertex AI / Google Search grounding for the synthesized results.

---

## 3. Agent Responsibilities & Interfaces (Contracts)

Data is passed between agents via strict Pydantic schemas (Contracts).

### A) AssetDiscovery Agent
- **Objective:** From a user prompt, produce a governed candidate set of data assets and explain how to access them.
- **Inputs:** User context (roles, intents), Catalog APIs (via MCP or plugins).
- **Core Behaviors:** 
  - Resolves semantic intents (e.g., "sales" -> specific physical Tables).
  - Fetches lineage graphs and glossary tags.
- **Output:** `AssetBundle` JSON

### B) QueryPlanner Agent
- **Objective:** Turn an `AssetBundle` + Context into Executable Queries (SQL, vector search, API calls) without executing them.
- **Inputs:** `AssetBundle`, User Query.
- **Behaviors:** 
  - Writes optimal logical plans.
  - Matches tables against data sources.
  - Attaches explanations for why a specific query is selected.
- **Output:** `QueryPlan` JSON

### C) DataRetriever (Action) Agent
- **Objective:** Execute the `QueryPlan` with least privilege and return bounded results with provenance.
- **Inputs:** `QueryPlan`.
- **Behaviors:** 
  - Connects to execution engines via secure tools.
  - Enforces result caps, pagination, and sampling.
  - Validates row limits to protect LLM context windows.
- **Output:** `ResultSet` JSON

### D) ContextBuilder Agent
- **Objective:** Build an LLM response narrative that is compact, accurate, strategy-driven, and cites its sources.
- **Inputs:** `ResultSet`, original User Query, Data Policies.
- **Behaviors:** 
  - Schema-aware summarization and semantic chunking.
  - Maps citations back to the source assets.
  - Streams the final response to the user.
- **Output:** `PromptPackage` JSON (or streamed response)

---

## 4. Cross-Cutting Concerns

### Security & Governance
- **ABAC/RBAC Enforcement:** Identity payload propagation before data queries.
- **Least Privilege:** Queries run under explicit service account bounds per data source.

### Observability & Telemetry
- OpenTelemetry tracing for agent execution, tool usages, plan formulation, and context retrieval.
- Telemetry logs sent to native GCP logging platforms.

### Compliance
- Data classification propagation limits how raw data is shared.
- Redaction policies and masking rules applied before prompt construction.

---

## 5. Technology Stack Summary
- **Orchestration:** Google ADK (`SequentialAgent`, `LlmAgent`, `Runner`).
- **Models:** Vertex AI (`gemini-1.5-pro` / `gemini-2.5-flash`).
- **Memory & State:** `InMemorySessionService` / `Pydantic` Contracts.
- **Infrastructure:** Google Cloud Project (`GOOGLE_CLOUD_PROJECT` auth flow), Cloud Run.
- **Integrations:** MCP (Model Context Protocol) for Catalog access, BigQuery.
