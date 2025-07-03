# test_databricks_connection.py
"""
Simple test to verify Databricks SQL serverless connection from .env file
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

def test_databricks_connection():
    """Test Databricks connection using .env file"""
    print("🔍 Testing Databricks SQL Serverless Connection")
    print("=" * 50)
    
    # Load environment variables
    if not Path("../.env").exists():
        print("❌ .env file not found")
        print("Create .env file with your Databricks credentials")
        return False
    
    load_dotenv("../.env")
    
    # Check required environment variables
    required_vars = [
        "DATABRICKS_HOSTNAME",
        "DATABRICKS_HTTP_PATH",
        "DATABRICKS_ACCESS_TOKEN"
    ]
    
    missing_vars = []
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            missing_vars.append(var)
        else:
            print(f"✅ {var}: {value[:10]}...")  # Show first 10 chars for security
    
    if missing_vars:
        print(f"❌ Missing environment variables: {missing_vars}")
        return False
    
    # Test connection
    try:
        print("\n🔗 Testing connection...")
        from databricks import sql
        
        connection = sql.connect(
            server_hostname=os.getenv("DATABRICKS_HOSTNAME"),
            http_path=os.getenv("DATABRICKS_HTTP_PATH"),
            access_token=os.getenv("DATABRICKS_ACCESS_TOKEN")
        )
        
        print("✅ Connection established successfully!")
        
        # Test simple query
        print("\n📊 Testing simple query...")
        cursor = connection.cursor()
        cursor.execute("SELECT 1 as test_column")
        result = cursor.fetchone()
        
        if result and result[0] == 1:
            print("✅ Simple query test passed!")
        else:
            print("❌ Simple query test failed!")
            return False
        
        # Test access to your specific table
        print("\n📋 Testing access to samples.bakehouse.sales_customers...")
        try:
            cursor.execute("SELECT COUNT(*) FROM samples.bakehouse.sales_customers")
            count_result = cursor.fetchone()
            row_count = count_result[0] if count_result else 0
            
            print(f"✅ Table access successful! Row count: {row_count:,}")
            
            # Show sample data
            print("\n📄 Sample data (first 3 rows):")
            cursor.execute("SELECT * FROM samples.bakehouse.sales_customers LIMIT 3")
            sample_rows = cursor.fetchall()
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            print(f"Columns: {columns}")
            
            for i, row in enumerate(sample_rows, 1):
                print(f"Row {i}: {row}")
            
        except Exception as e:
            print(f"❌ Table access failed: {e}")
            print("Make sure you have access to samples.bakehouse.sales_customers")
            return False
        
        finally:
            cursor.close()
            connection.close()
        
        print("\n🎉 All Databricks tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("\nTroubleshooting tips:")
        print("1. Check your DATABRICKS_HOSTNAME (should not include https://)")
        print("2. Verify your HTTP_PATH starts with /sql/1.0/warehouses/")
        print("3. Ensure your ACCESS_TOKEN is valid and not expired")
        print("4. Make sure your SQL warehouse is running")
        return False

if __name__ == "__main__":
    success = test_databricks_connection()
    sys.exit(0 if success else 1)