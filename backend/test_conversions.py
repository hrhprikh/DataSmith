"""
🔄 Test CSV ↔ JSON Conversion Functionality
Demonstrates all conversion features of DataSmith AI
"""

import requests
import json
import pandas as pd
import os

API_BASE_URL = "http://localhost:8000"

def create_test_files():
    """Create sample CSV and JSON files for testing conversion"""
    
    # Ensure directories exist
    os.makedirs('../data/raw', exist_ok=True)
    os.makedirs('../data/converted', exist_ok=True)
    
    print("📁 Creating test files...")
    
    # 1. Simple CSV file
    csv_data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice Johnson', 'Bob Smith', 'Carol Brown', 'David Wilson', 'Eva Davis'],
        'age': [28, 35, 42, 29, 33],
        'city': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'],
        'salary': [75000, 82000, 95000, 68000, 71000],
        'department': ['Engineering', 'Sales', 'Marketing', 'Engineering', 'HR']
    }
    
    df = pd.DataFrame(csv_data)
    df.to_csv('../data/raw/employees.csv', index=False)
    print("✅ Created employees.csv")
    
    # 2. Complex CSV with nested-like data (will be interesting for JSON conversion)
    product_data = {
        'product_id': ['P001', 'P002', 'P003'],
        'name': ['Laptop Pro', 'Wireless Mouse', 'Mechanical Keyboard'],
        'price': [1299.99, 79.99, 159.99],
        'category': ['Electronics/Computers', 'Electronics/Accessories', 'Electronics/Accessories'],
        'specs': [
            'CPU: Intel i7, RAM: 16GB, Storage: 512GB SSD',
            'Wireless: Bluetooth 5.0, Battery: 18 months',
            'Switches: Cherry MX Blue, Backlight: RGB'
        ],
        'tags': ['premium,business,portable', 'wireless,ergonomic', 'gaming,mechanical,rgb']
    }
    
    df_products = pd.DataFrame(product_data)
    df_products.to_csv('../data/raw/products.csv', index=False)
    print("✅ Created products.csv")
    
    # 3. JSON file (list of records format)
    json_records = [
        {
            "id": 1,
            "customer_name": "John Doe",
            "email": "john@example.com",
            "orders": [
                {"order_id": "ORD001", "amount": 299.99, "date": "2024-01-15"},
                {"order_id": "ORD002", "amount": 149.50, "date": "2024-02-20"}
            ],
            "preferences": {
                "newsletter": True,
                "sms_alerts": False,
                "preferred_category": "Electronics"
            }
        },
        {
            "id": 2,
            "customer_name": "Jane Smith", 
            "email": "jane@example.com",
            "orders": [
                {"order_id": "ORD003", "amount": 89.99, "date": "2024-01-28"}
            ],
            "preferences": {
                "newsletter": False,
                "sms_alerts": True,
                "preferred_category": "Books"
            }
        }
    ]
    
    with open('../data/raw/customers.json', 'w') as f:
        json.dump(json_records, f, indent=2)
    print("✅ Created customers.json")
    
    # 4. JSON file (dictionary format)
    json_dict = {
        "metadata": {
            "version": "1.0",
            "created_date": "2024-01-01",
            "source": "sales_system"
        },
        "sales_data": {
            "Q1": {"revenue": 125000, "units": 450},
            "Q2": {"revenue": 138000, "units": 520},
            "Q3": {"revenue": 142000, "units": 580},
            "Q4": {"revenue": 156000, "units": 650}
        }
    }
    
    with open('../data/raw/sales_summary.json', 'w') as f:
        json.dump(json_dict, f, indent=2)
    print("✅ Created sales_summary.json")

def test_csv_to_json_conversion():
    """Test CSV to JSON conversion with different formats"""
    
    print("\n🔄 Testing CSV to JSON Conversion")
    print("=" * 50)
    
    test_files = ['employees.csv', 'products.csv']
    formats = ['records', 'index', 'values', 'split']
    
    for filename in test_files:
        file_path = f'../data/raw/{filename}'
        if not os.path.exists(file_path):
            print(f"⚠️ Skipping {filename} - file not found")
            continue
            
        print(f"\n📊 Converting {filename}:")
        
        for format_type in formats:
            try:
                with open(file_path, 'rb') as f:
                    files = {'file': (filename, f, 'text/csv')}
                    params = {'format_type': format_type, 'pretty': True}
                    
                    response = requests.post(
                        f"{API_BASE_URL}/convert/csv-to-json/",
                        files=files,
                        params=params
                    )
                    
                    if response.status_code == 200:
                        result = response.json()
                        print(f"✅ {format_type}: {result['conversion_info']['output_file']}")
                    else:
                        print(f"❌ {format_type}: {response.status_code}")
                        
            except Exception as e:
                print(f"❌ {format_type}: {e}")

def test_json_to_csv_conversion():
    """Test JSON to CSV conversion"""
    
    print("\n🔄 Testing JSON to CSV Conversion") 
    print("=" * 50)
    
    test_files = ['customers.json', 'sales_summary.json']
    
    for filename in test_files:
        file_path = f'../data/raw/{filename}'
        if not os.path.exists(file_path):
            print(f"⚠️ Skipping {filename} - file not found")
            continue
            
        print(f"\n📊 Converting {filename}:")
        
        try:
            with open(file_path, 'rb') as f:
                files = {'file': (filename, f, 'application/json')}
                params = {'auto_flatten': True}
                
                response = requests.post(
                    f"{API_BASE_URL}/convert/json-to-csv/",
                    files=files,
                    params=params
                )
                
                if response.status_code == 200:
                    result = response.json()
                    print(f"✅ Success: {result['conversion_info']['output_file']}")
                    print(f"   Columns: {', '.join(result['data_preview']['columns'][:5])}...")
                    print(f"   Records: {result['conversion_info']['records_count']}")
                else:
                    print(f"❌ Failed: {response.status_code}")
                    print(f"   Error: {response.text}")
                    
        except Exception as e:
            print(f"❌ Error: {e}")

def test_batch_conversion():
    """Test batch conversion functionality"""
    
    print("\n🔄 Testing Batch Conversion")
    print("=" * 50)
    
    # Batch convert CSVs to JSON
    csv_files = ['../data/raw/employees.csv', '../data/raw/products.csv']
    existing_files = [f for f in csv_files if os.path.exists(f)]
    
    if existing_files:
        print("📦 Batch converting CSV files to JSON:")
        
        try:
            files = []
            for file_path in existing_files:
                with open(file_path, 'rb') as f:
                    files.append(('files', (os.path.basename(file_path), f.read(), 'text/csv')))
            
            response = requests.post(
                f"{API_BASE_URL}/convert/batch/",
                files=files,
                params={'target_format': 'json'}
            )
            
            if response.status_code == 200:
                result = response.json()
                print(f"✅ Batch conversion completed")
                print(f"   Successful: {result['successful_conversions']}")
                print(f"   Failed: {result['failed_conversions']}")
                
                if result['results']:
                    print("   Files converted:")
                    for r in result['results']:
                        print(f"     • {r['file']} → {r['conversion']}")
                        
            else:
                print(f"❌ Batch conversion failed: {response.status_code}")
                
        except Exception as e:
            print(f"❌ Batch conversion error: {e}")

def demonstrate_advanced_features():
    """Demonstrate advanced conversion features"""
    
    print("\n🎯 Advanced Features Demo")
    print("=" * 50)
    
    # Create a complex nested JSON for testing
    complex_json = {
        "users": [
            {
                "id": 1,
                "profile": {
                    "name": "Alice Johnson",
                    "contact": {
                        "email": "alice@example.com",
                        "phone": "+1-555-0123"
                    },
                    "address": {
                        "street": "123 Main St",
                        "city": "New York",
                        "zip": "10001"
                    }
                },
                "activity": {
                    "last_login": "2024-01-15T10:30:00Z",
                    "login_count": 45,
                    "preferences": ["email", "notifications"]
                }
            }
        ]
    }
    
    # Save complex JSON
    complex_file = '../data/raw/complex_nested.json'
    with open(complex_file, 'w') as f:
        json.dump(complex_json, f, indent=2)
    
    print("📊 Testing complex nested JSON flattening:")
    
    try:
        with open(complex_file, 'rb') as f:
            files = {'file': ('complex_nested.json', f, 'application/json')}
            
            response = requests.post(
                f"{API_BASE_URL}/convert/json-to-csv/",
                files=files,
                params={'auto_flatten': True}
            )
            
            if response.status_code == 200:
                result = response.json()
                print("✅ Complex JSON flattened successfully")
                print("   Flattened columns:")
                for col in result['data_preview']['columns']:
                    print(f"     • {col}")
            else:
                print(f"❌ Flattening failed: {response.status_code}")
                
    except Exception as e:
        print(f"❌ Error: {e}")

def show_conversion_results():
    """Show the results of conversions"""
    
    print("\n📁 Conversion Results")
    print("=" * 50)
    
    converted_dir = '../data/converted'
    if os.path.exists(converted_dir):
        files = os.listdir(converted_dir)
        if files:
            print("Generated files:")
            for file in sorted(files):
                file_path = os.path.join(converted_dir, file)
                size = os.path.getsize(file_path)
                print(f"  📄 {file} ({size} bytes)")
        else:
            print("No converted files found")
    else:
        print("Converted directory doesn't exist")

def check_api_status():
    """Check if the API is running"""
    try:
        response = requests.get(f"{API_BASE_URL}/")
        if response.status_code == 200:
            api_info = response.json()
            print("✅ DataSmith AI API is running")
            print(f"   Available endpoints: {len(api_info.get('endpoints', {}))}")
            return True
        else:
            print(f"⚠️ API responded with status: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ API not accessible: {e}")
        print("💡 Start the server with: uvicorn app:app --reload")
        return False

if __name__ == "__main__":
    print("🔄 DataSmith AI - CSV ↔ JSON Conversion Testing")
    print("=" * 60)
    
    # Check API status
    if not check_api_status():
        exit(1)
    
    # Create test files
    create_test_files()
    
    # Test conversions
    test_csv_to_json_conversion()
    test_json_to_csv_conversion()
    test_batch_conversion()
    demonstrate_advanced_features()
    
    # Show results
    show_conversion_results()
    
    print("\n🎉 All conversion tests completed!")
    print("\n💡 Available conversion endpoints:")
    print("   • POST /convert/csv-to-json/ - Convert CSV to JSON")
    print("   • POST /convert/json-to-csv/ - Convert JSON to CSV") 
    print("   • POST /convert/batch/ - Batch convert multiple files")
    print("   • GET /convert/download/{type}/{filename} - Download converted files")
    
    print("\n🎯 Try the conversions at: http://localhost:8000/docs")