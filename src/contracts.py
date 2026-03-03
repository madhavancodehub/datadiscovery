from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field

class DataAsset(BaseModel):
    """Represents a single data asset (e.g., table, view) discovered in the catalog."""
    asset_id: str = Field(description="Fully qualified ID of the asset (e.g., project.dataset.table)")
    asset_type: str = Field(description="Type of the asset (e.g., 'table', 'view', 'model')")
    description: Optional[str] = Field(None, description="Detailed description of the asset")
    schema_definition: List[Dict[str, str]] = Field(default_factory=list, description="List of columns with their types and descriptions")
    tags: List[str] = Field(default_factory=list, description="Governance or business tags associated with the asset")

class AssetBundle(BaseModel):
    """Output from AssetDiscovery: A curated collection of assets relevant to the user query."""
    assets: List[DataAsset] = Field(default_factory=list, description="The assets identified as relevant")
    rationale: str = Field(description="Explanation of why these assets were selected based on the user's intent")

class QueryPlan(BaseModel):
    """Output from QueryPlanner: The executable query and execution strategy."""
    sql_query: str = Field(description="The executable SQL query")
    target_assets: List[str] = Field(description="List of asset IDs this query touches")
    expected_result_type: str = Field(description="Description of what the result will look like (e.g., 'aggregated sales by region')")
    is_safe_to_execute: bool = Field(default=False, description="Flag indicating if the query is safe (read-only, bounded)")

class ResultSet(BaseModel):
    """Output from DataRetriever: The actual data returned from execution."""
    columns: List[str] = Field(description="Column names of the result set")
    rows: List[Dict[str, Any]] = Field(description="The rows of data")
    row_count: int = Field(description="Total number of rows returned")
    execution_time_ms: int = Field(description="Time taken to execute the query in milliseconds")
    error_message: Optional[str] = Field(None, description="Any error encountered during execution")

class PromptPackage(BaseModel):
    """Output from ContextBuilder: The final payload to synthesize the answer for the user."""
    original_query: str = Field(description="The analyst's original natural language query")
    synthesized_answer: str = Field(description="The final narrative answer derived from the data")
    data_citations: List[str] = Field(description="Citations or references to the specific assets or row data used")
    confidence_score: float = Field(description="Confidence score of the answer from 0.0 to 1.0")

class RoutingState(BaseModel):
    """The central state object passed through the ADK multi-agent pipeline."""
    user_query: str
    asset_bundle: Optional[AssetBundle] = None
    query_plan: Optional[QueryPlan] = None
    result_set: Optional[ResultSet] = None
    prompt_package: Optional[PromptPackage] = None
    current_agent: str = Field(default="Orchestrator")
    error_trace: List[str] = Field(default_factory=list)
