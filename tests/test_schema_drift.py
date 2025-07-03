#!/usr/bin/env python3
"""
Schema Drift Testing Script
Test how MCP agents detect and handle schema evolution
"""

import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

def test_schema_drift():
    """Test schema drift detection capabilities"""
    print("🔬 Testing Schema Drift Detection")
    print("=" * 50)
    
    # Load environment
    env_path = Path(__file__).parent.parent / ".env"
    if not env_path.exists():
        env_path = Path("../.env") 
        load_dotenv("../.env")
    else:
        load_dotenv()
    
    from mcp_ingestion.config import SourceConfig
    from mcp_ingestion.context import LocalMCPContext
    from mcp_ingestion.agents import SchemaAgent
    from databricks import sql
    
    # Setup
    source_config = SourceConfig(
        source_id="schema_test",
        server_hostname=os.getenv("DATABRICKS_HOSTNAME"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_ACCESS_TOKEN"),
        catalog="samples",
        schema="bakehouse",
        tables=["sales_customers"]
    )
    
    context = LocalMCPContext("./test_schema_context")
    schema_agent = SchemaAgent(context)
    
    # Connect to Databricks
    connection = sql.connect(
        server_hostname=source_config.server_hostname,
        http_path=source_config.http_path,
        access_token=source_config.access_token
    )
    
    print("✅ Connected to Databricks")
    
    # Test 1: First discovery (baseline)
    print("\n📊 Test 1: Initial Schema Discovery")
    print("-" * 40)
    
    schema1 = schema_agent.discover_schema(connection, source_config, "sales_customers")
    print(f"✅ Baseline schema discovered: {len(schema1)} columns")
    print(f"Columns: {schema1.names}")
    
    # Show stored context
    context_data = context.get_context("schema", "schema_test:sales_customers")
    print(f"📚 Context stored: {len(context_data)} entries")
    
    # Test 2: Simulate schema evolution by manually modifying context
    print("\n🔄 Test 2: Simulating Schema Evolution")
    print("-" * 40)
    
    # Manually create "previous" schema that differs from current
    simulated_old_schema = {
        "customerID": "bigint",
        "first_name": "string", 
        "last_name": "string",
        "email_address": "string",
        "phone_number": "string",
        "address": "string",
        "city": "string",
        "state": "string",
        "country": "string",
        "continent": "string"
        # Missing: postal_zip_code, gender (simulate new columns)
    }
    
    # Update context to simulate previous run with different schema
    context.update_context("schema", "schema_test:sales_customers", {
        "previous_schema": simulated_old_schema,
        "last_analyzed": "2024-01-01T00:00:00"
    })
    
    print("✅ Simulated previous schema with missing columns")
    
    # Test 3: Re-discover schema to trigger evolution detection
    print("\n🧬 Test 3: Schema Evolution Detection")
    print("-" * 40)
    
    schema2 = schema_agent.discover_schema(connection, source_config, "sales_customers")
    
    # Check context for evolution analysis
    evolved_context = context.get_context("schema", "schema_test:sales_customers")
    evolution = evolved_context.get("evolution", {})
    
    print(f"🔍 Schema evolution detected:")
    print(f"  Has changes: {evolution.get('has_changes', False)}")
    print(f"  New columns: {evolution.get('new_columns', [])}")
    print(f"  Removed columns: {evolution.get('removed_columns', [])}")
    print(f"  Type changes: {evolution.get('type_changes', {})}")
    print(f"  Recommended action: {evolution.get('recommended_action', 'UNKNOWN')}")
    print(f"  Backwards compatible: {evolution.get('backwards_compatible', True)}")
    
    # Test 4: Test with completely different table
    print("\n📋 Test 4: Different Table Schema")
    print("-" * 40)
    
    try:
        # Test with sales_transactions if available
        transactions_schema = schema_agent.discover_schema(connection, source_config, "sales_transactions")
        print(f"✅ Transactions schema: {len(transactions_schema)} columns")
        print(f"Columns: {transactions_schema.names}")
        
        # Compare schemas
        customer_cols = set(schema1.names)
        transaction_cols = set(transactions_schema.names)
        
        common_cols = customer_cols & transaction_cols
        customer_only = customer_cols - transaction_cols
        transaction_only = transaction_cols - customer_cols
        
        print(f"\n📊 Schema Comparison:")
        print(f"  Common columns: {list(common_cols)}")
        print(f"  Customer-only columns: {list(customer_only)}")
        print(f"  Transaction-only columns: {list(transaction_only)}")
        
    except Exception as e:
        print(f"⚠️  Sales transactions table not accessible: {e}")
    
    # Test 5: Manual type change simulation
    print("\n🔄 Test 5: Type Change Detection")
    print("-" * 40)
    
    # Simulate type changes
    simulated_type_change_schema = {
        **{col: "string" for col in schema1.names},  # All current columns
        "customerID": "string",  # Changed from bigint to string
        "postal_zip_code": "string"  # Changed from bigint to string
    }
    
    context.update_context("schema", "type_test:sales_customers", {
        "previous_schema": simulated_type_change_schema,
        "last_analyzed": "2024-01-01T00:00:00"
    })
    
    # Create new source config for type test
    type_test_config = SourceConfig(
        source_id="type_test",
        server_hostname=source_config.server_hostname,
        http_path=source_config.http_path,
        access_token=source_config.access_token,
        catalog="samples",
        schema="bakehouse"
    )
    
    schema_agent.discover_schema(connection, type_test_config, "sales_customers")
    
    type_context = context.get_context("schema", "type_test:sales_customers")
    type_evolution = type_context.get("evolution", {})
    
    print(f"🔍 Type change detection:")
    print(f"  Type changes detected: {type_evolution.get('type_changes', {})}")
    
    # Cleanup
    connection.close()
    
    print(f"\n🧠 Context Summary")
    print("-" * 40)
    contexts = context.list_contexts()
    for agent, sources in contexts.items():
        print(f"  {agent}: {sources}")
    
    print(f"\n✅ Schema drift testing completed!")
    print("📁 Test context stored in: ./test_schema_context/")
    print("🔍 Check the context files to see evolution tracking")

def inspect_context():
    """Inspect schema evolution context"""
    context_dir = Path("./test_schema_context")
    
    if not context_dir.exists():
        print("❌ No test context found. Run test_schema_drift() first.")
        return
    
    print("🔍 Schema Context Inspection")
    print("=" * 50)
    
    schema_dir = context_dir / "schema"
    if schema_dir.exists():
        for context_file in schema_dir.glob("*.json"):
            print(f"\n📄 {context_file.name}")
            print("-" * 30)
            
            with open(context_file, 'r') as f:
                data = json.load(f)
            
            # Show evolution info
            evolution = data.get("evolution", {})
            if evolution:
                print(f"Has changes: {evolution.get('has_changes')}")
                print(f"New columns: {evolution.get('new_columns')}")
                print(f"Removed columns: {evolution.get('removed_columns')}")
                print(f"Type changes: {evolution.get('type_changes')}")
                print(f"Action: {evolution.get('recommended_action')}")
                print(f"Compatible: {evolution.get('backwards_compatible')}")
            
            # Show schemas
            prev_schema = data.get("previous_schema", {})
            curr_schema = data.get("current_schema", {})
            
            if prev_schema:
                print(f"Previous schema columns: {len(prev_schema)}")
            if curr_schema:
                print(f"Current schema columns: {len(curr_schema)}")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "inspect":
        inspect_context()
    else:
        test_schema_drift()