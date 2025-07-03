# src/mcp_ingestion/openai_orchestrator.py
"""
Simple LLM orchestrator using pure OpenAI SDK
User: "extract table sales_customers"
LLM: Decides which agents to call and executes them
"""

import os
import sys
import json
from datetime import datetime
from typing import Dict, List, Any
from pathlib import Path

from openai import OpenAI
import structlog

from .config import SourceConfig, TargetConfig, MCPConfig
from .context import LocalMCPContext
from .agents import SourceAgent, SchemaAgent, WriterAgent, LoggerAgent

logger = structlog.get_logger()


class OpenAIOrchestrator:
    """Simple orchestrator using OpenAI SDK for agent decision making"""
    
    def __init__(self, source_config: SourceConfig, target_config: TargetConfig, mcp_config: MCPConfig):
        self.source_config = source_config
        self.target_config = target_config
        self.mcp_config = mcp_config
        
        # Initialize OpenAI client
        self.llm = OpenAI(api_key=mcp_config.openai_api_key)
        
        # Initialize MCP context and agents
        self.mcp_context = LocalMCPContext(mcp_config.context_dir)
        self.agents = {
            "source": SourceAgent(self.mcp_context, self.llm),
            "schema": SchemaAgent(self.mcp_context, self.llm), 
            "writer": WriterAgent(self.mcp_context, self.llm),
            "logger": LoggerAgent(self.mcp_context)
        }
        
        # Pipeline state
        self.execution_state = {
            "connection": None,
            "schema": None,
            "data": None,
            "result": None
        }
    
    def process_user_request(self, user_input: str) -> str:
        """Process natural language request from user"""
        
        logger.info(f"🗣️  User request: {user_input}")
        
        # Step 1: Get current pipeline context
        pipeline_context = self._get_pipeline_context()
        
        # Step 2: Ask LLM to decide what to do
        action_plan = self._get_llm_action_plan(user_input, pipeline_context)
        
        # Step 3: Execute the plan
        execution_result = self._execute_action_plan(action_plan)
        
        return execution_result
    
    def _get_pipeline_context(self) -> Dict[str, Any]:
        """Get current pipeline context for LLM"""
        
        # Check agent contexts
        contexts = self.mcp_context.list_contexts()
        
        context_summary = {}
        for agent_name in ["source", "schema", "writer", "logger"]:
            if agent_name in contexts and contexts[agent_name]:
                # Get the first source's context as example
                source_key = contexts[agent_name][0]
                agent_context = self.mcp_context.get_context(agent_name, source_key)
                
                if agent_name == "source":
                    context_summary[agent_name] = {
                        "last_connection": agent_context.get("last_successful_connection"),
                        "failures": agent_context.get("consecutive_failures", 0)
                    }
                elif agent_name == "schema":
                    context_summary[agent_name] = {
                        "last_analyzed": agent_context.get("last_analyzed"),
                        "has_changes": agent_context.get("evolution", {}).get("has_changes", False)
                    }
                elif agent_name == "writer":
                    perf = agent_context.get("last_write_performance", {})
                    context_summary[agent_name] = {
                        "last_write": agent_context.get("last_write_timestamp"),
                        "rows_written": perf.get("rows_written", 0),
                        "execution_time": perf.get("execution_time", 0)
                    }
                elif agent_name == "logger":
                    context_summary[agent_name] = {
                        "total_executions": agent_context.get("total_executions", 0),
                        "success_rate": agent_context.get("success_rate", 0)
                    }
            else:
                context_summary[agent_name] = {"status": "no_previous_context"}
        
        return {
            "timestamp": datetime.now().isoformat(),
            "available_tables": self.source_config.tables,
            "target_bucket": self.target_config.bucket_name,
            "agent_contexts": context_summary,
            "current_execution_state": {
                "has_connection": self.execution_state["connection"] is not None,
                "has_schema": self.execution_state["schema"] is not None,
                "has_data": self.execution_state["data"] is not None
            }
        }
    
    def _get_llm_action_plan(self, user_input: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """Ask LLM to create action plan"""
        
        system_prompt = """You are a data pipeline orchestrator controlling 4 agents:

1. source: Connects to Databricks database
2. schema: Discovers table schemas  
3. writer: Extracts data and writes to S3
4. logger: Logs results

For table extraction, use: source → schema → writer → logger

ONLY return valid JSON in this exact format:
{
  "actions": [
    {
      "agent": "source",
      "action": "connect", 
      "table_name": "sales_customers",
      "reasoning": "Connect to database"
    },
    {
      "agent": "schema",
      "action": "discover", 
      "table_name": "sales_customers",
      "reasoning": "Discover schema"
    },
    {
      "agent": "writer",
      "action": "extract_write", 
      "table_name": "sales_customers",
      "reasoning": "Extract and write data"
    },
    {
      "agent": "logger",
      "action": "log", 
      "table_name": "sales_customers",
      "reasoning": "Log results"
    }
  ],
  "summary": "Extract sales_customers table"
}

No additional text, just the JSON."""

        # Extract table name from user input
        table_name = "unknown"
        user_lower = user_input.lower()
        if "sales_customers" in user_lower:
            table_name = "sales_customers"
        elif "sales_transactions" in user_lower:
            table_name = "sales_transactions"
        elif "table" in user_lower:
            # Try to extract table name after "table"
            words = user_input.split()
            for i, word in enumerate(words):
                if word.lower() in ["table", "table:"]:
                    if i + 1 < len(words):
                        table_name = words[i + 1].replace(":", "").strip()
                        break

        user_prompt = f"""User wants to: {user_input}
Table name: {table_name}
Current state: {context.get('current_execution_state', {})}

Return the JSON action plan:"""

        try:
            response = self.llm.chat.completions.create(
                model=self.mcp_config.openai_model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.1,
                max_tokens=500
            )
            
            response_text = response.choices[0].message.content.strip()
            logger.debug(f"🧠 Raw LLM response: {response_text}")
            
            # Try to parse JSON
            action_plan = json.loads(response_text)
            logger.info(f"🧠 LLM Action Plan: {action_plan.get('summary', '')}")
            
            return action_plan
            
        except json.JSONDecodeError as e:
            logger.error(f"❌ LLM returned invalid JSON: {e}")
            return self._create_fallback_plan(table_name, user_input)
        except Exception as e:
            logger.error(f"❌ LLM request failed: {e}")
            return self._create_fallback_plan(table_name, user_input)
    
    def _create_fallback_plan(self, table_name: str, user_input: str) -> Dict[str, Any]:
        """Create fallback plan when LLM fails"""
        if "status" in user_input.lower():
            return {
                "actions": [],
                "summary": "Show pipeline status"
            }
        
        # Default extraction plan
        return {
            "actions": [
                {"agent": "source", "action": "connect", "table_name": table_name, "reasoning": "Connect to database"},
                {"agent": "schema", "action": "discover", "table_name": table_name, "reasoning": "Discover schema"},
                {"agent": "writer", "action": "extract_write", "table_name": table_name, "reasoning": "Extract and write data"},
                {"agent": "logger", "action": "log", "table_name": table_name, "reasoning": "Log results"}
            ],
            "summary": f"Extract {table_name} table (fallback plan)"
        }
    
    def _execute_action_plan(self, action_plan: Dict[str, Any]) -> str:
        """Execute the action plan step by step"""
        
        results = []
        actions = action_plan.get("actions", [])
        
        logger.info(f"🚀 Executing {len(actions)} actions")
        
        for i, action in enumerate(actions):
            agent_name = action.get("agent")
            action_type = action.get("action") 
            table_name = action.get("table_name", "")
            reasoning = action.get("reasoning", "")
            
            logger.info(f"🔄 Step {i+1}: {agent_name} - {action_type} ({reasoning})")
            
            try:
                # Normalize agent names and actions
                normalized_agent = agent_name.lower().replace("_agent", "")
                normalized_action = action_type.lower()
                
                if normalized_agent == "source" and "connect" in normalized_action:
                    result = self._execute_source_agent(table_name)
                    
                elif normalized_agent == "schema" and ("discover" in normalized_action or "schema" in normalized_action):
                    result = self._execute_schema_agent(table_name)
                    
                elif normalized_agent == "writer" and ("extract" in normalized_action or "write" in normalized_action):
                    result = self._execute_writer_agent(table_name)
                    
                elif normalized_agent == "logger" and "log" in normalized_action:
                    result = self._execute_logger_agent(table_name)
                    
                else:
                    result = f"⚠️  Unknown action: {agent_name} {action_type}"
                
                results.append(f"✅ {agent_name}: {result}")
                
            except Exception as e:
                error_msg = f"❌ {agent_name} failed: {str(e)}"
                results.append(error_msg)
                logger.error(error_msg)
                # Decide whether to continue or stop
                if self._should_continue_after_error(agent_name, e):
                    continue
                else:
                    break
        
        # Compile final response
        summary = action_plan.get("summary", "")
        final_response = f"📋 {summary}\n\n" + "\n".join(results)
        
        return final_response
    
    def _execute_source_agent(self, table_name: str) -> str:
        """Execute source agent"""
        try:
            connection = self.agents["source"].connect_with_context(self.source_config)
            self.execution_state["connection"] = connection
            return f"Connected to Databricks for table: {table_name}"
        except Exception as e:
            raise Exception(f"Connection failed: {e}")
    
    def _execute_schema_agent(self, table_name: str) -> str:
        """Execute schema agent"""
        if not self.execution_state["connection"]:
            raise Exception("No connection available - connect first")
        
        try:
            schema = self.agents["schema"].discover_schema(
                self.execution_state["connection"],
                self.source_config, 
                table_name
            )
            self.execution_state["schema"] = schema
            return f"Schema discovered for {table_name}: {len(schema)} columns"
        except Exception as e:
            raise Exception(f"Schema discovery failed: {e}")
    
    def _execute_writer_agent(self, table_name: str) -> str:
        """Execute writer agent"""
        if not self.execution_state["connection"]:
            raise Exception("No connection available")
        if not self.execution_state["schema"]:
            raise Exception("No schema available")
        
        try:
            # Extract data first
            data = self._extract_data_from_source(table_name)
            self.execution_state["data"] = data
            
            # Write data
            result = self.agents["writer"].write_with_optimization(
                data,
                self.execution_state["schema"],
                self.target_config,
                table_name
            )
            self.execution_state["result"] = result
            
            if result.success:
                return f"Extracted and wrote {result.rows_processed:,} rows to {result.target_location}"
            else:
                raise Exception(f"Write failed: {result.error_message}")
                
        except Exception as e:
            raise Exception(f"Data extraction/writing failed: {e}")
    
    def _execute_logger_agent(self, table_name: str) -> str:
        """Execute logger agent"""
        if not self.execution_state["result"]:
            raise Exception("No results to log")
        
        try:
            self.agents["logger"].log_result(self.execution_state["result"])
            return f"Results logged for {table_name}"
        except Exception as e:
            raise Exception(f"Logging failed: {e}")
    
    def _extract_data_from_source(self, table_name: str):
        """Extract data from the source"""
        import pandas as pd
        
        cursor = self.execution_state["connection"].cursor()
        try:
            query = f"SELECT * FROM {self.source_config.catalog}.{self.source_config.schema}.{table_name}"
            cursor.execute(query)
            results = cursor.fetchall()
            
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(results, columns=columns)
            
            logger.info(f"📥 Extracted {len(df)} rows from {table_name}")
            return df
        finally:
            cursor.close()
    
    def _should_continue_after_error(self, agent_name: str, error: Exception) -> bool:
        """Simple logic to decide whether to continue after error"""
        # For source agent failures, stop immediately
        if agent_name == "source":
            return False
        # For other agents, continue but log the failure
        return True
    
    def get_status(self) -> str:
        """Get current pipeline status"""
        contexts = self.mcp_context.list_contexts()
        
        status_lines = [
            "📊 Current Pipeline Status:",
            f"  🔌 Connection: {'✅' if self.execution_state['connection'] else '❌'}",
            f"  📋 Schema: {'✅' if self.execution_state['schema'] else '❌'}",
            f"  📥 Data: {'✅' if self.execution_state['data'] else '❌'}",
            f"  📝 Results: {'✅' if self.execution_state['result'] else '❌'}",
            "",
            "🧠 Agent Contexts:"
        ]
        
        for agent, sources in contexts.items():
            status_lines.append(f"  {agent}: {len(sources)} active contexts")
        
        return "\n".join(status_lines)
    
    def chat(self):
        """Interactive chat interface"""
        print("🤖 OpenAI SDK Pipeline Orchestrator")
        print("=" * 50)
        print("Try these commands:")
        print("• 'extract table sales_customers'")
        print("• 'process sales_transactions'") 
        print("• 'show status'")
        print("• 'get pipeline status'")
        print("Type 'quit' to exit")
        print()
        
        while True:
            try:
                user_input = input("👤 You: ").strip()
                
                if user_input.lower() in ['quit', 'exit', 'bye']:
                    print("👋 Goodbye!")
                    break
                
                if not user_input:
                    continue
                
                # Special case for status
                if 'status' in user_input.lower():
                    response = self.get_status()
                else:
                    response = self.process_user_request(user_input)
                
                print(f"🤖 Assistant:\n{response}\n")
                
            except KeyboardInterrupt:
                print("\n👋 Goodbye!")
                break
            except Exception as e:
                print(f"❌ Error: {e}\n")


# Main runner script
def main():
    """Main function to start the chat interface"""
    from pathlib import Path
    from dotenv import load_dotenv
    
    # Load environment
    env_path = Path(__file__).parent.parent.parent / ".env"
    load_dotenv(env_path)
    
    # Check OpenAI API key
    if not os.getenv("OPENAI_API_KEY"):
        print("❌ OPENAI_API_KEY not found in .env file")
        print("Add your OpenAI API key to enable LLM orchestration")
        return
    
    # Configure
    source_config = SourceConfig(
        source_id="databricks_samples",
        server_hostname=os.getenv("DATABRICKS_HOSTNAME"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_ACCESS_TOKEN"),
        catalog="samples",
        schema="bakehouse",
        tables=["sales_customers", "sales_transactions"]
    )
    
    target_config = TargetConfig(
        bucket_name="sdsdataset",
        prefix="delta-tables"
    )
    
    mcp_config = MCPConfig(
        context_dir="./mcp_context",
        openai_api_key=os.getenv("OPENAI_API_KEY"),
        openai_model="gpt-4",
        enable_llm=True
    )
    
    # Create and start orchestrator
    try:
        orchestrator = OpenAIOrchestrator(source_config, target_config, mcp_config)
        orchestrator.chat()
    except Exception as e:
        print(f"❌ Failed to start orchestrator: {e}")

if __name__ == "__main__":
    main()