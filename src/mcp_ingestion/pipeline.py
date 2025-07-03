"""
Simple MCP Pipeline - Main orchestration
"""

import asyncio
import pandas as pd
from datetime import datetime
from typing import List, Optional

from databricks import sql
from openai import OpenAI
import structlog

from .config import SourceConfig, TargetConfig, MCPConfig
from .models import IngestionResult
from .context import LocalMCPContext
from .agents import SourceAgent, SchemaAgent, WriterAgent, LoggerAgent

logger = structlog.get_logger()


class SimpleMCPPipeline:
    """Simple MCP-driven ingestion pipeline"""
    
    def __init__(self, source_config: SourceConfig, target_config: TargetConfig, mcp_config: MCPConfig):
        self.source_config = source_config
        self.target_config = target_config
        self.mcp_config = mcp_config
        
        # Initialize context storage
        self.context = LocalMCPContext(mcp_config.context_dir)
        
        # Initialize LLM client (optional)
        self.llm_client = None
        if mcp_config.enable_llm and mcp_config.openai_api_key:
            try:
                self.llm_client = OpenAI(api_key=mcp_config.openai_api_key)
                logger.info("LLM client initialized")
            except Exception as e:
                logger.warning("Failed to initialize LLM client", error=str(e))
        
        # Initialize agents
        self.source_agent = SourceAgent(self.context, self.llm_client)
        self.schema_agent = SchemaAgent(self.context, self.llm_client)
        self.writer_agent = WriterAgent(self.context, self.llm_client)
        self.logger_agent = LoggerAgent(self.context)
    
    def run_ingestion(self) -> List[IngestionResult]:
        """Run the complete ingestion pipeline"""
        results = []
        
        logger.info("Starting MCP ingestion pipeline", source_id=self.source_config.source_id)
        
        try:
            # Phase 1: Connect to source
            connection = self.source_agent.connect_with_context(self.source_config)
            
            # Phase 2: Process each table
            for table_name in self.source_config.tables:
                logger.info("Processing table", table_name=table_name)
                
                try:
                    # Schema discovery
                    schema = self.schema_agent.discover_schema(connection, self.source_config, table_name)
                    
                    # Extract data
                    df = self._extract_data(connection, self.source_config, table_name)
                    
                    # Write data
                    result = self.writer_agent.write_with_optimization(df, schema, self.target_config, table_name)
                    
                    # Log result
                    self.logger_agent.log_result(result)
                    
                    results.append(result)
                    
                except Exception as e:
                    logger.error("Failed to process table", table_name=table_name, error=str(e))
                    
                    error_result = IngestionResult(
                        source_id=self.source_config.source_id,
                        table_name=table_name,
                        rows_processed=0,
                        target_location="",
                        execution_time_seconds=0,
                        schema_evolution=None,
                        performance_metrics={},
                        success=False,
                        error_message=str(e)
                    )
                    
                    self.logger_agent.log_result(error_result)
                    results.append(error_result)
            
            # Close connection
            connection.close()
            
        except Exception as e:
            logger.error("Pipeline failed", error=str(e))
            raise
        
        return results
    
    def _extract_data(self, connection, source_config: SourceConfig, table_name: str) -> pd.DataFrame:
        """Extract data from source"""
        cursor = connection.cursor()
        
        try:
            query = f"SELECT * FROM {source_config.catalog}.{source_config.schema}.{table_name}"
            cursor.execute(query)
            results = cursor.fetchall()
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            
            # Create DataFrame
            df = pd.DataFrame(results, columns=columns)
            
            logger.info("Data extracted", table_name=table_name, rows=len(df), columns=len(columns))
            return df
            
        finally:
            cursor.close()
    
    def get_context_summary(self) -> dict:
        """Get summary of all contexts"""
        return self.context.list_contexts()
    
    def get_performance_metrics(self) -> dict:
        """Get performance metrics from context"""
        metrics = {}
        
        for table_name in self.source_config.tables:
            logger_context = self.context.get_context("logger", table_name)
            
            if logger_context:
                metrics[table_name] = {
                    "total_executions": logger_context.get("total_executions", 0),
                    "success_rate": logger_context.get("success_rate", 0),
                    "last_execution": logger_context.get("last_execution", {})
                }
        
        return metrics