#!/usr/bin/env python3
"""
Main MCP Pipeline Runner - This script performs the actual data ingestion
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

def main():
    """Run the MCP data ingestion pipeline"""
    print("🚀 Starting MCP Data Ingestion Pipeline")
    print("=" * 50)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load environment variables
    env_path = Path(__file__).parent.parent / ".env"
    if not env_path.exists():
        print("❌ .env file not found in project root")
        print("Run: python scripts/configure.py to create it")
        sys.exit(1)
    
    load_dotenv(env_path)
    
    # Validate required environment variables
    required_vars = [
        "DATABRICKS_HOSTNAME",
        "DATABRICKS_HTTP_PATH",
        "DATABRICKS_ACCESS_TOKEN",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY"
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        print(f"❌ Missing required environment variables: {missing_vars}")
        print("Run: python scripts/configure.py to set them up")
        sys.exit(1)
    
    print("✅ Environment variables loaded")
    
    # Import pipeline components
    try:
        from mcp_ingestion.config import SourceConfig, TargetConfig, MCPConfig
        from mcp_ingestion.pipeline import SimpleMCPPipeline
        print("✅ Pipeline components imported")
    except ImportError as e:
        print(f"❌ Failed to import pipeline components: {e}")
        print("Make sure you're running from the project root")
        sys.exit(1)
    
    # Create configuration
    print("\n⚙️  Creating Configuration")
    print("-" * 30)
    
    source_config = SourceConfig(
        source_id="databricks_samples",
        server_hostname=os.getenv("DATABRICKS_HOSTNAME"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_ACCESS_TOKEN"),
        catalog="samples",
        schema="bakehouse",
        tables=["sales_customers", "sales_transactions"]
    )
    print(f"📊 Source: {source_config.catalog}.{source_config.schema}")
    print(f"📋 Tables: {source_config.tables}")
    
    target_config = TargetConfig(
        bucket_name="sdsdataset",
        prefix="delta-tables",
        region=os.getenv("AWS_DEFAULT_REGION", "eu-central-1")
    )
    print(f"🪣 Target: s3://{target_config.bucket_name}/{target_config.prefix}")
    
    mcp_config = MCPConfig(
        context_dir=os.getenv("MCP_CONTEXT_DIR", "./mcp_context"),
        openai_api_key=os.getenv("OPENAI_API_KEY", ""),
        openai_model="gpt-4",
        enable_llm=os.getenv("MCP_ENABLE_LLM", "false").lower() == "true"
    )
    print(f"🧠 Context: {mcp_config.context_dir}")
    print(f"🤖 LLM: {'Enabled' if mcp_config.enable_llm else 'Disabled'}")
    
    # Initialize pipeline
    print("\n🔧 Initializing Pipeline")
    print("-" * 30)
    
    try:
        pipeline = SimpleMCPPipeline(source_config, target_config, mcp_config)
        print("✅ Pipeline initialized successfully")
        
        # Show context summary before execution
        context_summary = pipeline.get_context_summary()
        if context_summary:
            print(f"📚 Existing context: {len(context_summary)} agents have prior context")
        else:
            print("📚 No existing context - first run")
            
    except Exception as e:
        print(f"❌ Failed to initialize pipeline: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    # Run ingestion
    print("\n🏃 Running Ingestion")
    print("-" * 30)
    
    try:
        start_time = datetime.now()
        results = pipeline.run_ingestion()
        end_time = datetime.now()
        total_time = (end_time - start_time).total_seconds()
        
        print(f"\n📊 Ingestion Results")
        print("=" * 50)
        
        total_rows = 0
        successful_tables = 0
        
        for result in results:
            status = "✅ SUCCESS" if result.success else "❌ FAILED"
            print(f"\n{status} - {result.table_name}")
            print(f"  📈 Rows processed: {result.rows_processed:,}")
            print(f"  ⏱️  Execution time: {result.execution_time_seconds:.2f}s")
            print(f"  📍 Target location: {result.target_location}")
            print(f"  🧠 Context updated: {result.context_updated}")
            
            if result.success:
                total_rows += result.rows_processed
                successful_tables += 1
                
                # Show performance metrics
                perf_metrics = result.performance_metrics
                if perf_metrics:
                    print(f"  📊 Performance: {perf_metrics}")
                    
            else:
                print(f"  ❌ Error: {result.error_message}")
                
                # Show troubleshooting tips
                if "connection" in result.error_message.lower():
                    print("  💡 Check Databricks credentials and warehouse status")
                elif "s3" in result.error_message.lower():
                    print("  💡 Check AWS credentials and S3 bucket permissions")
        
        print(f"\n📈 Overall Summary")
        print("-" * 30)
        print(f"  Tables processed: {successful_tables}/{len(results)}")
        print(f"  Total rows: {total_rows:,}")
        print(f"  Total time: {total_time:.2f}s")
        print(f"  Average speed: {total_rows/total_time:.0f} rows/second" if total_time > 0 else "  Average speed: N/A")
        
        # Show context summary after execution
        print(f"\n🧠 Context Summary")
        print("-" * 30)
        contexts = pipeline.get_context_summary()
        for agent, sources in contexts.items():
            print(f"  {agent}: {len(sources)} sources")
        
        # Show performance metrics
        print(f"\n📊 Performance Metrics")
        print("-" * 30)
        metrics = pipeline.get_performance_metrics()
        for table, stats in metrics.items():
            print(f"  {table}:")
            print(f"    Total executions: {stats.get('total_executions', 0)}")
            print(f"    Success rate: {stats.get('success_rate', 0):.1%}")
            
            last_exec = stats.get('last_execution', {})
            if last_exec:
                print(f"    Last execution: {last_exec.get('timestamp', 'Unknown')}")
        
        # Final status
        if successful_tables == len(results):
            print("\n🎉 All tables processed successfully!")
            print("✅ Your data has been ingested to Delta Lake format")
            print(f"📁 Check your S3 bucket: s3://{target_config.bucket_name}/{target_config.prefix}")
        else:
            print(f"\n⚠️  {len(results) - successful_tables} table(s) failed")
            print("Check the error messages above for troubleshooting")
            
    except Exception as e:
        print(f"❌ Pipeline execution failed: {e}")
        import traceback
        traceback.print_exc()
        
        # Show troubleshooting tips
        print("\n🔧 Troubleshooting Tips:")
        print("1. Check your .env file has all required variables")
        print("2. Verify Databricks warehouse is running")
        print("3. Test S3 access with: aws s3 ls s3://sdsdataset/")
        print("4. Check MCP context directory permissions")
        
        sys.exit(1)
    
    print(f"\n✅ Pipeline completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("🔍 Run 'python scripts/monitor.py' to inspect context and performance")

if __name__ == "__main__":
    main()