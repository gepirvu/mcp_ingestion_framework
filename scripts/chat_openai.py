# scripts/chat_openai.py
#!/usr/bin/env python3
"""
Chat with OpenAI to orchestrate your MCP pipeline
Pure OpenAI SDK - no extra dependencies needed!
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

def main():
    """Start the OpenAI chat orchestrator"""
    print("🚀 Starting OpenAI Pipeline Orchestrator")
    print("=" * 50)
    
    # Load environment
    env_path = Path(__file__).parent.parent / ".env"
    if not env_path.exists():
        print("❌ .env file not found")
        print("Make sure you have a .env file in the project root")
        sys.exit(1)
    
    load_dotenv(env_path)
    
    # Check required environment variables
    required_vars = [
        "DATABRICKS_HOSTNAME",
        "DATABRICKS_HTTP_PATH", 
        "DATABRICKS_ACCESS_TOKEN",
        "OPENAI_API_KEY",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY"
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        print(f"❌ Missing environment variables: {missing_vars}")
        print("Check your .env file")
        sys.exit(1)
    
    print("✅ Environment variables loaded")
    
    # Import and start orchestrator
    try:
        from mcp_ingestion.openai_orchestrator import OpenAIOrchestrator
        from mcp_ingestion.config import SourceConfig, TargetConfig, MCPConfig
        
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
            prefix="delta-tables",
            region=os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        )
        
        mcp_config = MCPConfig(
            context_dir="./mcp_context",
            openai_api_key=os.getenv("OPENAI_API_KEY"),
            openai_model="gpt-4",
            enable_llm=True
        )
        
        print("✅ Configuration loaded")
        print("🤖 Starting chat interface...")
        print()
        
        # Start the orchestrator
        orchestrator = OpenAIOrchestrator(source_config, target_config, mcp_config)
        orchestrator.chat()
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Make sure all required modules are available")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error starting orchestrator: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

# Example usage script
# scripts/example_commands.py
#!/usr/bin/env python3
"""
Example commands you can use with the OpenAI orchestrator
"""

examples = [
    "extract table sales_customers",
    "process the sales_transactions table", 
    "go extract data from test_table",
    "get the current pipeline status",
    "show me what's happening",
    "extract sales_customers and then sales_transactions",
    "check if the pipeline is working",
    "run ingestion for all tables",
    "what's the status of my pipeline?",
    "extract table: sales_customers please"
]

print("🤖 Example Commands for OpenAI Pipeline Orchestrator")
print("=" * 60)
print()
print("You can try any of these natural language commands:")
print()

for i, example in enumerate(examples, 1):
    print(f"{i:2d}. {example}")

print()
print("🚀 Start the orchestrator with:")
print("   uv run python scripts/chat_openai.py")
print()
print("💡 The LLM will:")
print("   • Understand your intent")
print("   • Decide which agents to call") 
print("   • Execute them in the right order")
print("   • Give you detailed feedback")


# Test script to verify it works
# scripts/test_openai_orchestrator.py
#!/usr/bin/env python3
"""
Test the OpenAI orchestrator programmatically
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

def test_orchestrator():
    """Test the orchestrator with a sample command"""
    # Load environment
    env_path = Path(__file__).parent.parent / ".env"
    load_dotenv(env_path)
    
    if not os.getenv("OPENAI_API_KEY"):
        print("❌ OPENAI_API_KEY required for testing")
        return
    
    try:
        from mcp_ingestion.openai_orchestrator import OpenAIOrchestrator
        from mcp_ingestion.config import SourceConfig, TargetConfig, MCPConfig
        
        # Configure
        source_config = SourceConfig(
            source_id="test_source",
            server_hostname=os.getenv("DATABRICKS_HOSTNAME"),
            http_path=os.getenv("DATABRICKS_HTTP_PATH"),
            access_token=os.getenv("DATABRICKS_ACCESS_TOKEN"),
            catalog="samples",
            schema="bakehouse"
        )
        
        target_config = TargetConfig(bucket_name="sdsdataset")
        mcp_config = MCPConfig(
            context_dir="./test_mcp_context",
            openai_api_key=os.getenv("OPENAI_API_KEY"),
            enable_llm=True
        )
        
        # Create orchestrator
        orchestrator = OpenAIOrchestrator(source_config, target_config, mcp_config)
        
        # Test commands
        test_commands = [
            "show pipeline status",
            "extract table sales_customers"
        ]
        
        print("🧪 Testing OpenAI Orchestrator")
        print("=" * 40)
        
        for command in test_commands:
            print(f"\n👤 Test command: {command}")
            print("🤖 Response:")
            
            try:
                if 'status' in command:
                    response = orchestrator.get_status()
                else:
                    response = orchestrator.process_user_request(command)
                print(response)
            except Exception as e:
                print(f"❌ Error: {e}")
            
            print("-" * 40)
        
        print("\n✅ Test completed!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_orchestrator()