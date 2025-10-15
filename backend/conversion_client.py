"""
🌐 DataSmith AI - CSV ↔ JSON Conversion Client
Easy-to-use examples for converting between CSV and JSON formats
"""

import requests
import os

def convert_csv_to_json(csv_file_path, format_type="records", pretty=True):
    """
    Convert a CSV file to JSON
    
    Args:
        csv_file_path: Path to CSV file
        format_type: "records", "index", "values", "split", "columns"
        pretty: Whether to format JSON nicely
    """
    
    if not os.path.exists(csv_file_path):
        print(f"❌ File not found: {csv_file_path}")
        return None
    
    url = "http://localhost:8000/convert/csv-to-json/"
    
    with open(csv_file_path, 'rb') as f:
        files = {'file': (os.path.basename(csv_file_path), f, 'text/csv')}
        params = {'format_type': format_type, 'pretty': pretty}
        
        try:
            print(f"🔄 Converting {os.path.basename(csv_file_path)} to JSON...")
            response = requests.post(url, files=files, params=params)
            
            if response.status_code == 200:
                result = response.json()
                print(f"✅ Conversion successful!")
                print(f"   📁 Output: {result['conversion_info']['output_file']}")
                print(f"   📊 Records: {result['conversion_info']['records_count']}")
                print(f"   🏷️  Format: {result['conversion_info']['format_type']}")
                return result
            else:
                print(f"❌ Conversion failed: {response.status_code}")
                print(f"   Error: {response.text}")
                return None
                
        except Exception as e:
            print(f"❌ Error: {e}")
            return None

def convert_json_to_csv(json_file_path, auto_flatten=True):
    """
    Convert a JSON file to CSV
    
    Args:
        json_file_path: Path to JSON file  
        auto_flatten: Whether to flatten nested structures
    """
    
    if not os.path.exists(json_file_path):
        print(f"❌ File not found: {json_file_path}")
        return None
    
    url = "http://localhost:8000/convert/json-to-csv/"
    
    with open(json_file_path, 'rb') as f:
        files = {'file': (os.path.basename(json_file_path), f, 'application/json')}
        params = {'auto_flatten': auto_flatten}
        
        try:
            print(f"🔄 Converting {os.path.basename(json_file_path)} to CSV...")
            response = requests.post(url, files=files, params=params)
            
            if response.status_code == 200:
                result = response.json()
                print(f"✅ Conversion successful!")
                print(f"   📁 Output: {result['conversion_info']['output_file']}")
                print(f"   📊 Records: {result['conversion_info']['records_count']}")
                print(f"   🏷️  Columns: {result['conversion_info']['columns_count']}")
                return result
            else:
                print(f"❌ Conversion failed: {response.status_code}")
                print(f"   Error: {response.text}")
                return None
                
        except Exception as e:
            print(f"❌ Error: {e}")
            return None

def download_converted_file(filename, file_type):
    """
    Download a converted file
    
    Args:
        filename: Name of the converted file
        file_type: 'csv' or 'json'
    """
    
    url = f"http://localhost:8000/convert/download/{file_type}/{filename}"
    
    try:
        print(f"📥 Downloading {filename}...")
        response = requests.get(url)
        
        if response.status_code == 200:
            # Save file locally
            with open(filename, 'wb') as f:
                f.write(response.content)
            print(f"✅ Downloaded: {filename}")
            return True
        else:
            print(f"❌ Download failed: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Download error: {e}")
        return False

def batch_convert_files(file_paths, target_format):
    """
    Batch convert multiple files
    
    Args:
        file_paths: List of file paths to convert
        target_format: 'csv' or 'json'
    """
    
    url = "http://localhost:8000/convert/batch/"
    
    files = []
    for file_path in file_paths:
        if os.path.exists(file_path):
            with open(file_path, 'rb') as f:
                content_type = 'text/csv' if file_path.endswith('.csv') else 'application/json'
                files.append(('files', (os.path.basename(file_path), f.read(), content_type)))
    
    if not files:
        print("❌ No valid files found for batch conversion")
        return None
    
    try:
        print(f"📦 Batch converting {len(files)} files to {target_format}...")
        response = requests.post(url, files=files, params={'target_format': target_format})
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Batch conversion completed!")
            print(f"   ✅ Successful: {result['successful_conversions']}")
            print(f"   ❌ Failed: {result['failed_conversions']}")
            
            if result['results']:
                print("   📁 Converted files:")
                for r in result['results']:
                    print(f"      • {r['file']}")
            
            if result['errors']:
                print("   ⚠️ Errors:")
                for e in result['errors']:
                    print(f"      • {e['file']}: {e['error']}")
                    
            return result
        else:
            print(f"❌ Batch conversion failed: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"❌ Batch conversion error: {e}")
        return None

# Example usage functions
def example_csv_to_json():
    """Example: Convert CSV to different JSON formats"""
    
    print("\n🎯 Example: CSV to JSON Conversion")
    print("=" * 40)
    
    # Create a sample CSV file
    sample_csv_content = """name,age,city,job,salary
Alice Johnson,28,New York,Engineer,75000
Bob Smith,35,Los Angeles,Designer,68000
Carol Brown,42,Chicago,Manager,95000"""
    
    os.makedirs('../data/raw', exist_ok=True)
    sample_file = '../data/raw/sample_employees.csv'
    
    with open(sample_file, 'w') as f:
        f.write(sample_csv_content)
    
    print(f"📝 Created sample file: {sample_file}")
    
    # Convert to different JSON formats
    formats = ['records', 'index', 'values', 'split']
    
    for format_type in formats:
        print(f"\n🔄 Converting to {format_type} format...")
        result = convert_csv_to_json(sample_file, format_type=format_type)

def example_json_to_csv():
    """Example: Convert JSON to CSV with flattening"""
    
    print("\n🎯 Example: JSON to CSV Conversion")
    print("=" * 40)
    
    # Create a sample JSON file with nested structure
    import json
    
    sample_json = [
        {
            "id": 1,
            "user": {
                "name": "Alice Johnson",
                "email": "alice@example.com",
                "profile": {
                    "age": 28,
                    "location": "New York"
                }
            },
            "orders": [
                {"id": "ORD001", "amount": 299.99},
                {"id": "ORD002", "amount": 149.50}
            ]
        },
        {
            "id": 2,
            "user": {
                "name": "Bob Smith",
                "email": "bob@example.com", 
                "profile": {
                    "age": 35,
                    "location": "Los Angeles"
                }
            },
            "orders": [
                {"id": "ORD003", "amount": 89.99}
            ]
        }
    ]
    
    sample_file = '../data/raw/sample_users.json'
    with open(sample_file, 'w') as f:
        json.dump(sample_json, f, indent=2)
    
    print(f"📝 Created sample JSON: {sample_file}")
    
    # Convert to CSV with flattening
    result = convert_json_to_csv(sample_file, auto_flatten=True)

def example_batch_conversion():
    """Example: Batch convert multiple files"""
    
    print("\n🎯 Example: Batch Conversion")
    print("=" * 40)
    
    # Check for existing files
    csv_files = []
    json_files = []
    
    test_dir = '../data/raw'
    if os.path.exists(test_dir):
        for file in os.listdir(test_dir):
            file_path = os.path.join(test_dir, file)
            if file.endswith('.csv'):
                csv_files.append(file_path)
            elif file.endswith('.json'):
                json_files.append(file_path)
    
    if csv_files:
        print(f"📦 Batch converting {len(csv_files)} CSV files to JSON...")
        batch_convert_files(csv_files, 'json')
    
    if json_files:
        print(f"📦 Batch converting {len(json_files)} JSON files to CSV...")
        batch_convert_files(json_files, 'csv')

if __name__ == "__main__":
    print("🌐 DataSmith AI - CSV ↔ JSON Conversion Client")
    print("=" * 50)
    
    # Check if API is running
    try:
        response = requests.get("http://localhost:8000/")
        if response.status_code == 200:
            print("✅ DataSmith AI API is running")
        else:
            print("⚠️ API connection issue")
    except:
        print("❌ API not accessible - make sure to start: uvicorn app:app --reload")
        exit(1)
    
    # Run examples
    example_csv_to_json()
    example_json_to_csv() 
    example_batch_conversion()
    
    print("\n🎉 All examples completed!")
    print("\n💡 Converted files are saved in: ../data/converted/")
    print("📚 Use the functions above in your own scripts for conversions")