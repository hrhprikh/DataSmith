"""
🌐 Example client for DataSmith AI Enhanced API
Shows how to interact with the intelligent CSV processing endpoints
"""

import requests
import json
import os

API_BASE_URL = "http://localhost:8000"

def upload_and_process_csv(file_path, use_ai=True):
    """Upload and process a CSV file using the API"""
    
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return None
    
    endpoint = "/process/" if use_ai else "/process/legacy"
    url = f"{API_BASE_URL}{endpoint}"
    
    with open(file_path, 'rb') as f:
        files = {'file': (os.path.basename(file_path), f, 'text/csv')}
        
        print(f"📤 Uploading {file_path} to {endpoint}")
        
        try:
            response = requests.post(url, files=files)
            response.raise_for_status()
            
            result = response.json()
            return result
            
        except requests.exceptions.RequestException as e:
            print(f"❌ API Error: {e}")
            return None

def quick_analyze_csv(file_path):
    """Quickly analyze a CSV without full processing"""
    
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return None
    
    url = f"{API_BASE_URL}/analyze/"
    
    with open(file_path, 'rb') as f:
        files = {'file': (os.path.basename(file_path), f, 'text/csv')}
        
        print(f"🔍 Analyzing {file_path}")
        
        try:
            response = requests.post(url, files=files)
            response.raise_for_status()
            
            result = response.json()
            return result
            
        except requests.exceptions.RequestException as e:
            print(f"❌ API Error: {e}")
            return None

def display_results(result, analysis_type="Processing"):
    """Display API results in a formatted way"""
    
    if not result:
        return
        
    print(f"\n📊 {analysis_type} Results:")
    print("=" * 50)
    
    if 'file_info' in result:
        info = result['file_info']
        print(f"📁 File: {info.get('original_name')} → {info.get('processed_name', 'N/A')}")
        print(f"📏 Dimensions: {info.get('rows_processed', 'N/A')} rows, {len(info.get('columns', []))} columns")
        print(f"🏷️  Columns: {', '.join(info.get('columns', []))}")
    
    if 'ai_insights' in result:
        insights = result['ai_insights']
        print(f"\n🤖 AI Insights:")
        print(f"   Data Type: {insights.get('data_type', 'N/A')}")
        print(f"   Labels: {insights.get('label_explanation', 'N/A')}")
        if 'labels_added' in insights:
            print(f"   Added Labels: {', '.join(insights['labels_added'])}")
    
    if 'sample_data' in result:
        print(f"\n📋 Sample Data:")
        for i, row in enumerate(result['sample_data'][:2]):
            print(f"   Row {i+1}: {row}")
    
    if 'data_quality' in result:
        quality = result['data_quality']
        print(f"\n📈 Data Quality:")
        print(f"   Completeness: {quality.get('completeness', 'N/A')}")
        print(f"   Missing Values: {sum(quality.get('missing_values', {}).values())} total")
        print(f"   Duplicates: {quality.get('duplicate_rows', 0)}")
    
    if 'recommendations' in result:
        print(f"\n💡 Recommendations:")
        for rec in result['recommendations']:
            print(f"   • {rec}")

def test_api_endpoints():
    """Test all API endpoints with sample data"""
    
    # Test files (create these first using the test script)
    test_files = [
        '../data/raw/test_customer_feedback.csv',
        '../data/raw/test_sales_data.csv',
        '../data/raw/test_survey_no_headers.csv'
    ]
    
    # Check API status
    try:
        response = requests.get(f"{API_BASE_URL}/")
        response.raise_for_status()
        
        print("🚀 DataSmith AI API Status:")
        api_info = response.json()
        print(f"   Message: {api_info.get('message', 'N/A')}")
        print(f"   Features: {len(api_info.get('features', []))} available")
        
    except Exception as e:
        print(f"❌ API not accessible: {e}")
        print("💡 Make sure to start the server with: uvicorn app:app --reload")
        return
    
    # Test each file
    for file_path in test_files:
        if os.path.exists(file_path):
            print(f"\n" + "="*60)
            print(f"🧪 Testing with: {os.path.basename(file_path)}")
            
            # 1. Quick analysis
            analysis_result = quick_analyze_csv(file_path)
            display_results(analysis_result, "Quick Analysis")
            
            # 2. AI Processing
            ai_result = upload_and_process_csv(file_path, use_ai=True)
            display_results(ai_result, "AI Processing")
            
        else:
            print(f"⚠️ Skipping {file_path} - file not found")
            print("💡 Run test_ai_enhancements.py first to create test data")

def demo_custom_csv():
    """Demo with a custom CSV file"""
    print("\n" + "="*60)
    print("🎯 Custom CSV Demo")
    
    # Create a custom CSV on the fly
    custom_data = """John,25,Engineer,New York,I really love this new software tool
Jane,30,Designer,Los Angeles,The interface could be more intuitive
Bob,35,Manager,Chicago,Excellent product quality and support
Alice,28,Developer,Boston,Good value for money overall
Mike,42,Analyst,Miami,Not satisfied with the performance"""
    
    custom_file = '../data/raw/demo_custom.csv'
    with open(custom_file, 'w') as f:
        f.write(custom_data)
    
    print(f"📝 Created custom CSV: {custom_file}")
    
    # Process with AI
    result = upload_and_process_csv(custom_file, use_ai=True)
    display_results(result, "Custom CSV AI Processing")

if __name__ == "__main__":
    print("🌐 DataSmith AI API Client Demo")
    print("Make sure the API server is running on http://localhost:8000")
    print("Start with: uvicorn app:app --reload")
    
    # Ensure data directory exists
    os.makedirs('../data/raw', exist_ok=True)
    
    # Test API endpoints
    test_api_endpoints()
    
    # Demo with custom CSV
    demo_custom_csv()
    
    print(f"\n✅ Demo completed!")
    print(f"🎯 Check the '../data/labeled/' folder for processed files")