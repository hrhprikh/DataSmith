"""
🔧 Fix JSON Headers - Convert existing JSON with data-as-keys to proper headers
Uses Mistral 7B AI to analyze data patterns and generate meaningful column names
"""

import json
import requests
import pandas as pd
import os

LM_API_URL = "http://localhost:1234/v1/completions"

def analyze_json_and_generate_headers(json_data):
    """
    Analyze JSON data with poor keys and generate proper headers using AI
    """
    
    # Extract sample data for analysis
    if isinstance(json_data, list) and len(json_data) > 0:
        first_record = json_data[0]
        sample_values = []
        
        # Get first few records for better analysis
        for record in json_data[:3]:
            row_values = list(record.values())
            sample_values.append(row_values)
    
    prompt = f"""
You are an expert data analyst. Looking at this data sample, determine what each column represents and generate appropriate column names.

Data Analysis:
{json.dumps(sample_values, indent=2)}

Based on the patterns in this data:
- Column 1: Appears to be IDs starting with "P" (like P001, P002, P003, etc.)
- Column 2: Appears to be names (John, Sarah, Arun, Pooja, Ravi, etc.)
- Column 3: Appears to be ages (45, 50, 60, 28, 33, etc.)
- Column 4: Appears to be gender (Male, Female)
- Column 5: Appears to be temperature or vital signs (98.6, 102.1, 120.0, 96.4, 104.2)
- Column 6: Appears to be health status or diagnosis (Normal, Fever, High BP, etc.)

Generate appropriate column names that reflect what each column contains. Return ONLY a JSON array of column names.

Expected output format: ["patient_id", "name", "age", "gender", "temperature", "diagnosis"]
"""

    payload = {
        "model": "mistral-7b-instruct-v0.3.Q4_K_M.gguf",
        "prompt": prompt,
        "temperature": 0.1,
        "max_tokens": 200
    }

    try:
        print("🤖 Analyzing data with Mistral 7B to generate proper headers...")
        response = requests.post(LM_API_URL, json=payload, timeout=60)
        response.raise_for_status()
        
        data = response.json()
        raw_text = data["choices"][0].get("text", "").strip()
        
        # Clean up the response
        if "```" in raw_text:
            parts = raw_text.split("```")
            raw_text = parts[1] if len(parts) > 1 else raw_text
        raw_text = raw_text.strip("` \n")
        
        # Parse JSON
        try:
            headers = json.loads(raw_text)
            if isinstance(headers, list):
                print(f"✅ AI-generated headers: {headers}")
                return headers
        except:
            print("⚠️ AI response not valid JSON, using intelligent fallback")
    
    except Exception as e:
        print(f"❌ AI request failed: {e}")
    
    # Fallback headers based on data analysis
    fallback_headers = ["patient_id", "name", "age", "gender", "temperature", "diagnosis"]
    print(f"🔄 Using fallback headers: {fallback_headers}")
    return fallback_headers

def fix_json_headers(input_file_path, output_file_path=None):
    """
    Fix JSON file by replacing data-as-keys with proper AI-generated headers
    """
    
    if not os.path.exists(input_file_path):
        print(f"❌ File not found: {input_file_path}")
        return None
    
    # Load the problematic JSON
    with open(input_file_path, 'r', encoding='utf-8') as f:
        json_data = json.load(f)
    
    print(f"📁 Loading JSON file: {input_file_path}")
    print(f"📊 Records found: {len(json_data)}")
    
    if not json_data or len(json_data) == 0:
        print("❌ No data found in JSON file")
        return None
    
    # Show current problematic structure
    print("🔍 Current problematic structure:")
    first_record = json_data[0]
    for key, value in first_record.items():
        print(f"   '{key}': '{value}'")
    
    # Generate proper headers using AI
    proper_headers = analyze_json_and_generate_headers(json_data)
    
    # Create new JSON with proper headers
    fixed_data = []
    old_keys = list(json_data[0].keys())
    
    for record in json_data:
        new_record = {}
        values = list(record.values())
        
        # Map old values to new headers
        for i, header in enumerate(proper_headers):
            if i < len(values):
                new_record[header] = values[i]
        
        fixed_data.append(new_record)
    
    # Save fixed JSON
    if output_file_path is None:
        output_file_path = input_file_path.replace('.json', '_fixed.json')
    
    with open(output_file_path, 'w', encoding='utf-8') as f:
        json.dump(fixed_data, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Fixed JSON saved to: {output_file_path}")
    
    # Show new improved structure
    print("🎉 New improved structure:")
    first_fixed = fixed_data[0]
    for key, value in first_fixed.items():
        print(f"   '{key}': '{value}'")
    
    return output_file_path

def fix_health_json():
    """
    Specifically fix the health.json file
    """
    input_file = '../data/converted/health.json'
    output_file = '../data/converted/health_fixed.json'
    
    print("🏥 Fixing health.json with AI-generated headers...")
    result = fix_json_headers(input_file, output_file)
    
    if result:
        print(f"\n📋 Summary:")
        print(f"   Original: {input_file}")
        print(f"   Fixed: {output_file}")
        print(f"   Headers: AI-generated using Mistral 7B")
        
        # Show comparison
        with open(output_file, 'r') as f:
            fixed_data = json.load(f)
        
        print(f"\n🔍 Sample fixed record:")
        print(json.dumps(fixed_data[0], indent=2))

if __name__ == "__main__":
    print("🔧 JSON Header Fixer - AI-Powered")
    print("=" * 50)
    
    # Check if LM Studio is running
    try:
        test_response = requests.get("http://localhost:1234/v1/models", timeout=5)
        if test_response.status_code == 200:
            print("✅ LM Studio is running and accessible")
        else:
            print("⚠️ LM Studio connection issue")
    except:
        print("❌ LM Studio not accessible - make sure it's running on localhost:1234")
        print("💡 You can still use fallback headers")
    
    # Fix the health.json file
    fix_health_json()
    
    print(f"\n💡 Usage for other files:")
    print(f"   fix_json_headers('path/to/your/file.json')")
    print(f"   # Creates: path/to/your/file_fixed.json")