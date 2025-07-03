"""
MCP Data Ingestion Pipeline - Simple Local Version

A Model Context Protocol (MCP) driven intelligent data ingestion pipeline
that learns from previous executions and stores context locally.
"""

__version__ = "0.1.0"

from .pipeline import SimpleMCPPipeline
from .config import SourceConfig, TargetConfig, MCPConfig
from .models import IngestionResult, SchemaEvolution

__all__ = [
    "SimpleMCPPipeline",
    "SourceConfig",
    "TargetConfig", 
    "MCPConfig",
    "IngestionResult",
    "SchemaEvolution",
]


# src/mcp_ingestion/config.py
"""
Simple configuration for MCP pipeline
"""

from typing import List, Optional
from pydantic import BaseModel, Field


class SourceConfig(BaseModel):
    """Databricks source configuration"""
    source_id: str = Field(..., description="Unique identifier for this source")
    server_hostname: str = Field(..., description="Databricks hostname")
    http_path: str = Field(..., description="HTTP path for the warehouse")
    access_token: str = Field(..., description="Personal access token")
    catalog: str = Field(default="samples", description="Catalog name")
    schema: str = Field(default="bakehouse", description="Schema name")
    tables: List[str] = Field(default=["sales_customers"], description="Tables to ingest")


class TargetConfig(BaseModel):
    """S3 target configuration"""
    bucket_name: str = Field(..., description="S3 bucket name")
    prefix: str = Field(default="delta-tables", description="S3 prefix")
    region: str = Field(default="us-east-1", description="AWS region")


class MCPConfig(BaseModel):
    """MCP configuration - local storage only"""
    context_dir: str = Field(default="./mcp_context", description="Local context directory")
    openai_api_key: str = Field(..., description="OpenAI API key")
    openai_model: str = Field(default="gpt-4", description="OpenAI model")
    enable_llm: bool = Field(default=True, description="Enable LLM intelligence")
