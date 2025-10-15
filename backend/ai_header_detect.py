import requests
import json

LM_API_URL = "http://localhost:1234/v1/completions"  # Local LM Studio API endpoint

def ai_detect_headers(sample_rows: list):
    """
    Uses Mistral 7B to intelligently infer column names from data samples
    """

    prompt = f"""
You are an expert data analyst. Analyze the following data samples and determine the most appropriate column names.

Instructions:
1. Look at the data patterns, types, and values to understand what each column represents
2. Generate descriptive, professional column names in snake_case format
3. Consider common data patterns (names, ages, dates, IDs, categories, etc.)
4. Be specific - if you see email patterns, use "email", not "contact"
5. Return ONLY a JSON array of column names, nothing else

Data analysis guidelines:
- Text that looks like names → "name" or "full_name" 
- Numbers 0-120 → likely "age"
- Text with @ symbols → "email"
- Long text passages → "review", "comment", or "description"
- Dates/timestamps → "date", "created_at", etc.
- Short codes/IDs → "id", "user_id", etc.
- Categories → "category", "type", "status"
- Yes/No values → "is_active", "approved", etc.

Sample data rows:
{json.dumps(sample_rows, indent=2)}

Expected output format: ["column1", "column2", "column3"]
"""

    payload = {
        "model": "mistral-7b-instruct-v0.3.Q4_K_M.gguf",
        "prompt": prompt,
        "temperature": 0.1,  # Lower temperature for more consistent header naming
        "max_tokens": 500
    }

    try:
        response = requests.post(LM_API_URL, json=payload, timeout=60)
        response.raise_for_status()
        data = response.json()
        raw_text = data["choices"][0].get("text", "").strip()

        # Remove markdown if present
        if "```" in raw_text:
            parts = raw_text.split("```")
            raw_text = parts[1] if len(parts) > 1 else raw_text
        raw_text = raw_text.strip("` \n")

        # Parse JSON safely
        try:
            headers = json.loads(raw_text)
            if isinstance(headers, list) and len(headers) == len(sample_rows[0]):
                print("✅ AI-detected headers:", headers)
                return headers
            else:
                print("⚠️ Header count mismatch, using intelligent fallback")
                return intelligent_header_fallback(sample_rows)
        except Exception as e:
            print(f"⚠️ JSON parsing failed: {e}")
            print("🔍 Raw AI output:", raw_text[:200])
            return intelligent_header_fallback(sample_rows)

    except Exception as e:
        print(f"❌ LM Studio connection error: {e}")
        return intelligent_header_fallback(sample_rows)

def intelligent_header_fallback(sample_rows: list):
    """
    Intelligent fallback header detection based on data patterns
    """
    headers = []
    
    for col_idx in range(len(sample_rows[0])):
        # Get sample values for this column
        sample_values = [str(row[col_idx]) if row[col_idx] is not None else "" for row in sample_rows[:3]]
        
        header = detect_column_type(sample_values, col_idx)
        headers.append(header)
    
    print("🎯 Intelligent fallback headers:", headers)
    return headers

def detect_column_type(sample_values, col_idx):
    """
    Detect column type based on sample values
    """
    # Join samples for pattern analysis
    combined = " ".join(sample_values).lower()
    
    # Check for specific patterns
    if any('@' in val for val in sample_values):
        return "email"
    
    # Try to detect if numeric
    numeric_count = 0
    for val in sample_values:
        try:
            float(val)
            numeric_count += 1
        except:
            pass
    
    if numeric_count >= len(sample_values) * 0.8:  # 80% numeric
        # Check if it could be age, year, price, etc.
        try:
            nums = [float(val) for val in sample_values if val.replace('.', '').isdigit()]
            if nums:
                avg_num = sum(nums) / len(nums)
                if 0 <= avg_num <= 120:
                    return "age"
                elif 1900 <= avg_num <= 2030:
                    return "year"
                elif avg_num > 1000:
                    return "amount"
                else:
                    return "value"
        except:
            pass
        return "number"
    
    # Check for common text patterns
    if any(len(val) > 50 for val in sample_values):
        return "description"
    elif any(val in ['yes', 'no', 'true', 'false', 'y', 'n'] for val in combined.split()):
        return "status"
    elif col_idx == 0:  # First column often contains names or IDs
        return "name"
    else:
        return f"column_{col_idx + 1}"
