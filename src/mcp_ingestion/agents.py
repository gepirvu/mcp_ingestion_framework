"""
Simplified MCP agents with local context storage
"""

import json
import pandas as pd
import pyarrow as pa
from datetime import datetime
from typing import Any, Dict, List, Optional
from dataclasses import asdict

from databricks import sql
from deltalake import write_deltalake
from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_exponential
import structlog

from .config import SourceConfig, TargetConfig, MCPConfig
from .models import IngestionResult, SchemaEvolution
from .context import LocalMCPContext

logger = structlog.get_logger()


class SourceAgent:
    """Intelligent source connection agent"""
    
    def __init__(self, context: LocalMCPContext, llm_client: Optional[OpenAI] = None):
        self.context = context
        self.llm_client = llm_client
        
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def connect_with_context(self, source_config: SourceConfig):
        """Connect to Databricks with intelligent retry"""
        context = self.context.get_context("source", source_config.source_id)
        
        try:
            connection = sql.connect(
                server_hostname=source_config.server_hostname,
                http_path=source_config.http_path,
                access_token=source_config.access_token
            )
            
            # Update context with successful connection
            self.context.update_context("source", source_config.source_id, {
                "last_successful_connection": datetime.now().isoformat(),
                "consecutive_failures": 0,
                "connection_success_rate": context.get("connection_success_rate", 0) + 0.1
            })
            
            logger.info("Connected to Databricks successfully", source_id=source_config.source_id)
            return connection
            
        except Exception as e:
            # Update context with failure
            failures = context.get("consecutive_failures", 0) + 1
            self.context.update_context("source", source_config.source_id, {
                "last_failure": datetime.now().isoformat(),
                "last_error": str(e),
                "consecutive_failures": failures
            })
            
            logger.error("Failed to connect to Databricks", source_id=source_config.source_id, error=str(e))
            raise


class SchemaAgent:
    """Intelligent schema discovery agent"""
    
    def __init__(self, context: LocalMCPContext, llm_client: Optional[OpenAI] = None):
        self.context = context
        self.llm_client = llm_client
    
    def discover_schema(self, connection, source_config: SourceConfig, table_name: str) -> pa.Schema:
        """Discover and evolve schema"""
        context_key = f"{source_config.source_id}:{table_name}"
        context = self.context.get_context("schema", context_key)
        
        # Discover current schema
        current_schema = self._discover_current_schema(connection, source_config, table_name)
        
        # Analyze schema evolution
        evolution = self._analyze_schema_evolution(current_schema, context)
        
        # Generate PyArrow schema
        arrow_schema = self._generate_arrow_schema(current_schema)
        
        # Update context
        self.context.update_context("schema", context_key, {
            "previous_schema": current_schema,
            "current_schema": {name: str(field_type) for name, field_type in zip(arrow_schema.names, arrow_schema.types)},
            "evolution": asdict(evolution),
            "last_analyzed": datetime.now().isoformat()
        })
        
        logger.info("Schema discovered", table_name=table_name, columns=len(arrow_schema), has_changes=evolution.has_changes)
        return arrow_schema
    
    def _discover_current_schema(self, connection, source_config: SourceConfig, table_name: str) -> Dict[str, str]:
        """Discover current schema from source"""
        cursor = connection.cursor()
        
        try:
            query = f"DESCRIBE {source_config.catalog}.{source_config.schema}.{table_name}"
            cursor.execute(query)
            results = cursor.fetchall()
            
            schema = {}
            for row in results:
                col_name, data_type, nullable = row
                schema[col_name] = data_type
                
            return schema
            
        finally:
            cursor.close()
    
    def _analyze_schema_evolution(self, current_schema: Dict[str, str], context: Dict[str, Any]) -> SchemaEvolution:
        """Analyze schema changes"""
        previous_schema = context.get("previous_schema", {})
        
        if not previous_schema:
            return SchemaEvolution(
                has_changes=False,
                new_columns=[],
                removed_columns=[],
                type_changes={},
                recommended_action="CREATE_NEW",
                backwards_compatible=True
            )
        
        # Simple schema comparison
        new_columns = list(set(current_schema.keys()) - set(previous_schema.keys()))
        removed_columns = list(set(previous_schema.keys()) - set(current_schema.keys()))
        
        type_changes = {}
        for col in set(current_schema.keys()) & set(previous_schema.keys()):
            if current_schema[col] != previous_schema[col]:
                type_changes[col] = {
                    "old_type": previous_schema[col],
                    "new_type": current_schema[col]
                }
        
        has_changes = bool(new_columns or removed_columns or type_changes)
        
        return SchemaEvolution(
            has_changes=has_changes,
            new_columns=new_columns,
            removed_columns=removed_columns,
            type_changes=type_changes,
            recommended_action="COMPATIBLE_UPDATE" if not removed_columns else "BREAKING_CHANGE",
            backwards_compatible=not removed_columns
        )
    
    def _generate_arrow_schema(self, current_schema: Dict[str, str]) -> pa.Schema:
        """Generate PyArrow schema"""
        type_mapping = {
            "bigint": pa.int64(),
            "string": pa.string(),
            "int": pa.int32(),
            "double": pa.float64(),
            "boolean": pa.bool_(),
            "timestamp": pa.timestamp('us'),
            "date": pa.date32(),
        }
        
        fields = []
        for col_name, databricks_type in current_schema.items():
            clean_type = databricks_type.lower().split()[0]
            pa_type = type_mapping.get(clean_type, pa.string())
            field = pa.field(col_name, pa_type, nullable=True)
            fields.append(field)
        
        return pa.schema(fields)


class WriterAgent:
    """Intelligent Delta Lake writer"""
    
    def __init__(self, context: LocalMCPContext, llm_client: Optional[OpenAI] = None):
        self.context = context
        self.llm_client = llm_client
    
    def write_with_optimization(self, df: pd.DataFrame, schema: pa.Schema, target_config: TargetConfig, table_name: str) -> IngestionResult:
        """Write data with optimization"""
        start_time = datetime.now()
        
        context_key = f"{target_config.bucket_name}:{table_name}"
        context = self.context.get_context("writer", context_key)
        
        # Target location
        target_location = f"s3://{target_config.bucket_name}/{target_config.prefix}/{table_name}"
        
        try:
            # Write to Delta Lake
            write_deltalake(
                table_or_uri=target_location,
                data=df,
                mode="overwrite",
                storage_options={
                    "AWS_ALLOW_HTTP": "true",
                    "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
                }
            )
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            # Update context with performance
            self.context.update_context("writer", context_key, {
                "last_write_performance": {
                    "execution_time": execution_time,
                    "rows_written": len(df),
                    "data_size_mb": df.memory_usage(deep=True).sum() / (1024 * 1024)
                },
                "last_write_timestamp": datetime.now().isoformat(),
                "total_writes": context.get("total_writes", 0) + 1
            })
            
            logger.info("Data written successfully", 
                       table_name=table_name, 
                       rows=len(df), 
                       time_seconds=execution_time)
            
            return IngestionResult(
                source_id="databricks",
                table_name=table_name,
                rows_processed=len(df),
                target_location=target_location,
                execution_time_seconds=execution_time,
                schema_evolution=None,
                performance_metrics={"execution_time": execution_time},
                success=True,
                context_updated=True
            )
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            
            # Update context with failure
            self.context.update_context("writer", context_key, {
                "last_failure": datetime.now().isoformat(),
                "last_error": str(e),
                "failures": context.get("failures", 0) + 1
            })
            
            logger.error("Failed to write data", table_name=table_name, error=str(e))
            
            return IngestionResult(
                source_id="databricks",
                table_name=table_name,
                rows_processed=0,
                target_location=target_location,
                execution_time_seconds=execution_time,
                schema_evolution=None,
                performance_metrics={},
                success=False,
                error_message=str(e),
                context_updated=True
            )


class LoggerAgent:
    """Intelligent logging agent"""
    
    def __init__(self, context: LocalMCPContext):
        self.context = context
    
    def log_result(self, result: IngestionResult):
        """Log ingestion result with context"""
        context = self.context.get_context("logger", result.table_name)
        
        # Log structured event
        logger.info(
            "Ingestion completed",
            table_name=result.table_name,
            success=result.success,
            rows_processed=result.rows_processed,
            execution_time=result.execution_time_seconds,
            error=result.error_message
        )
        
        # Update execution history
        execution_history = context.get("execution_history", [])
        execution_history.append({
            "timestamp": datetime.now().isoformat(),
            "success": result.success,
            "rows_processed": result.rows_processed,
            "execution_time": result.execution_time_seconds,
            "error_message": result.error_message
        })
        
        # Keep only last 50 executions
        execution_history = execution_history[-50:]
        
        # Calculate success rate
        success_count = sum(1 for e in execution_history if e["success"])
        success_rate = success_count / len(execution_history) if execution_history else 0
        
        self.context.update_context("logger", result.table_name, {
            "execution_history": execution_history,
            "last_execution": execution_history[-1],
            "total_executions": len(execution_history),
            "success_rate": success_rate
        })