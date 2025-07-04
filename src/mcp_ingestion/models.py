"""
Data models for MCP pipeline
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class SchemaEvolution:
    """Schema evolution analysis"""
    has_changes: bool
    new_columns: List[str]
    removed_columns: List[str]
    type_changes: Dict[str, Dict[str, str]]
    recommended_action: str
    backwards_compatible: bool


@dataclass
class IngestionResult:
    """Ingestion operation result"""
    source_id: str
    table_name: str
    rows_processed: int
    target_location: str
    execution_time_seconds: float
    schema_evolution: Optional[SchemaEvolution]
    performance_metrics: Dict[str, Any]
    success: bool
    error_message: Optional[str] = None
    context_updated: bool = False

