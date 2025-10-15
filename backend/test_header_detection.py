"""
🧪 Test Enhanced Header Detection for CSV ↔ JSON Conversion
This tests the improved logic for detecting headerless CSV files
"""

import os
import pandas as pd
import requests
import json

def create_test_csvs():
    """Create test CSV files - some with headers, some without"""
    
    os.makedirs('../data/raw', exist_ok=True)
    
    print("📁 Creating test CSV files...")
    
    # 1. CSV WITH proper headers
    good_csv = """name,age,gender,city,year,feedback
Alice Johnson,28,Female,New York,2020,Good service
Bob Smith,35,Male,Los Angeles,2019,Excellent experience
Carol Brown,42,Female,Chicago,2021,Average quality"""
    
    with open('../data/raw/with_headers.csv', 'w') as f:
        f.write(good_csv)
    print("✅ Created with_headers.csv (proper headers)")
    
    # 2. CSV WITHOUT headers (data starts immediately)
    no_headers_csv = """Alice,24,Female,New York,2005,good service
Bob,30,Male,,2006,BAD experience
Charlie,,Male,Los Angeles,1994,Good service
Alice,24,Female,New York,1998,good service
David,29,,Chicago,1999,Average
Eve,22,Female,Boston,2010,bad response
,28,Male,Boston,2020,Excellent"""
    
    with open('../data/raw/no_headers.csv', 'w') as f:
        f.write(no_headers_csv)
    print("✅ Created no_headers.csv (no proper headers)")
    
    # 3. CSV with POOR headers (looks like data)
    poor_headers_csv = """Alice, 24, Female, New York,2005, good service 
Bob, 30, Male, ,2006, BAD experience
Charlie, , Male, Los Angeles,1994, Good service
Alice, 24, Female, New York,1998, good service 
David, 29, , Chicago,1999, Average 
Eve, 22, Female, Boston,2010, bad response 
, 28, Male, Boston,2020, Excellent"""
    
    with open('../data/raw/poor_headers.csv', 'w') as f:
        f.write(poor_headers_csv)
    print("✅ Created poor_headers.csv (first row looks like data)")

def test_header_detection_api():
    """Test the API's header detection capabilities"""
    
    print("\n🧪 Testing Header Detection via API")
    print("=" * 50)
    
    test_files = [
        ('with_headers.csv', 'Should detect proper headers'),
        ('no_headers.csv', 'Should detect missing headers and use AI'),
        ('poor_headers.csv', 'Should detect poor headers and use AI')
    ]
    
    for filename, description in test_files:
        file_path = f'../data/raw/{filename}'
        
        if not os.path.exists(file_path):
            print(f"⚠️ Skipping {filename} - file not found")
            continue
        
        print(f"\n📊 Testing {filename}: {description}")
        
        try:
            with open(file_path, 'rb') as f:
                files = {'file': (filename, f, 'text/csv')}
                params = {'format_type': 'records', 'pretty': True}
                
                response = requests.post(
                    "http://localhost:8000/convert/csv-to-json/",
                    files=files,
                    params=params
                )
                
                if response.status_code == 200:
                    result = response.json()
                    header_info = result['conversion_info']['header_detection']
                    
                    print(f"✅ Conversion successful!")
                    print(f"   Had proper headers: {header_info['had_proper_headers']}")
                    print(f"   AI generated headers: {header_info['ai_generated_headers']}")
                    print(f"   Final columns: {header_info['final_columns']}")
                    
                    # Show sample data
                    sample = result['data_preview']['sample_records'][0]
                    print(f"   Sample record: {sample}")
                    
                else:
                    print(f"❌ Failed: {response.status_code}")
                    print(f"   Error: {response.text}")
                    
        except Exception as e:
            print(f"❌ Error testing {filename}: {e}")

def test_local_header_detection():
    """Test header detection logic locally"""
    
    print("\n🔍 Testing Header Detection Logic Locally")
    print("=" * 50)
    
    # Import the functions we created
    import sys
    sys.path.append('.')
    
    test_cases = [
        # Good headers
        (['name', 'age', 'city', 'feedback'], 'Should be detected as proper headers'),
        
        # Poor headers (look like data)
        (['Alice', ' 24', ' Female', ' New York'], 'Should be detected as data, not headers'),
        
        # Mixed (some good, some bad)
        (['name', 'Alice', 'city', '2005'], 'Mixed - should be detected as poor headers'),
        
        # Numeric headers
        (['0', '1', '2', '3'], 'Numeric headers - should be detected as poor'),
        
        # Generic headers
        (['Column1', 'Unnamed: 0', 'col_1'], 'Generic headers - should be detected as poor')
    ]
    
    for headers, description in test_cases:
        print(f"\n🧪 Testing: {headers}")
        print(f"   Expected: {description}")
        
        # Create a dummy DataFrame to test with
        dummy_data = [['Alice', 25, 'New York', 'Good'], ['Bob', 30, 'LA', 'Bad']]
        df = pd.DataFrame(dummy_data, columns=headers)
        
        # Test our detection function
        try:
            from app import detect_proper_headers
            is_proper = detect_proper_headers(headers, df)
            print(f"   Result: {'✅ Proper headers' if is_proper else '❌ Poor headers (will use AI)'}")
        except ImportError:
            print("   ⚠️ Could not import detection function")

def demonstrate_before_after():
    """Show before/after examples of the conversion"""
    
    print("\n📋 Before/After Examples")
    print("=" * 50)
    
    # Show what happens with the problematic file
    problematic_file = '../data/raw/no_headers.csv'
    
    if os.path.exists(problematic_file):
        print("📄 Original CSV content:")
        with open(problematic_file, 'r') as f:
            lines = f.readlines()[:3]  # Show first 3 lines
            for i, line in enumerate(lines, 1):
                print(f"   Line {i}: {line.strip()}")
        
        print("\n🔄 After conversion with improved header detection:")
        try:
            with open(problematic_file, 'rb') as f:
                files = {'file': ('no_headers.csv', f, 'text/csv')}
                
                response = requests.post(
                    "http://localhost:8000/convert/csv-to-json/",
                    files=files,
                    params={'format_type': 'records', 'pretty': True}
                )
                
                if response.status_code == 200:
                    result = response.json()
                    
                    # Show the improved JSON structure
                    sample_record = result['data_preview']['sample_records'][0]
                    print("   Improved JSON structure:")
                    print(f"   {json.dumps(sample_record, indent=6)}")
                    
                    # Show that headers were improved
                    columns = result['conversion_info']['header_detection']['final_columns']
                    print(f"\n   🤖 AI-generated column names: {columns}")
                    
                else:
                    print(f"   ❌ Conversion failed: {response.status_code}")
                    
        except Exception as e:
            print(f"   ❌ Error: {e}")

def check_api_status():
    """Check if API is running"""
    try:
        response = requests.get("http://localhost:8000/")
        if response.status_code == 200:
            print("✅ DataSmith AI API is running")
            return True
        else:
            print(f"⚠️ API responded with status: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ API not accessible: {e}")
        print("💡 Start the server with: cd backend && uvicorn app:app --reload")
        return False

if __name__ == "__main__":
    print("🧪 Testing Enhanced Header Detection")
    print("=" * 60)
    
    # Check if API is running
    if not check_api_status():
        print("⚠️ API not running - testing local logic only")
        create_test_csvs()
        test_local_header_detection()
    else:
        # Full testing with API
        create_test_csvs()
        test_local_header_detection()
        test_header_detection_api()
        demonstrate_before_after()
    
    print("\n🎉 Header detection testing completed!")
    print("\n💡 Key improvements:")
    print("   • Detects when CSV has no proper headers")
    print("   • Uses AI to generate meaningful column names")
    print("   • Handles data that looks like headers")
    print("   • Provides feedback about header detection in API response")