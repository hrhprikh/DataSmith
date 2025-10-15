import requests
import json
import pandas as pd
from ai_header_detect import ai_detect_headers

LM_API_URL = "http://localhost:1234/v1/completions"

def ai_analyze_and_process(df: pd.DataFrame):
    """
    Intelligently analyze any CSV file using Mistral 7B to:
    1. Detect and improve column headers if needed
    2. Understand the data structure and content
    3. Generate appropriate labels and insights
    4. Clean and standardize the data
    """
    
    # First, check if headers need improvement
    df = improve_headers_if_needed(df)
    
    # Get sample data for AI analysis
    sample = df.head(10).to_dict(orient="records")
    columns_info = {col: str(df[col].dtype) for col in df.columns}
    
    prompt = f"""
You are an intelligent data analyst AI. Analyze this dataset and process it intelligently.

Dataset Info:
- Columns: {list(df.columns)}
- Data types: {columns_info}
- Sample data: {json.dumps(sample[:5], indent=2)}

Your tasks:
1. **Understand the data**: What type of dataset is this? (e.g., customer reviews, sales data, survey responses, etc.)
2. **Clean the data**: Fix inconsistencies, handle missing values appropriately
3. **Generate intelligent labels**: Based on the data content, create meaningful categorical labels
4. **Preserve all columns**: Keep original data structure but enhance it

Output requirements:
- Return ONLY a JSON object with this structure:
{{
  "data_type": "brief description of what this dataset contains",
  "processed_data": [list of processed records with all original columns plus new insights],
  "label_explanation": "explanation of what the new labels represent"
}}

Rules:
- Fill missing values intelligently based on context
- For text data, create sentiment/category labels
- For numeric data, create range-based or statistical labels
- Preserve ALL original columns
- Add meaningful derived columns that provide insights

Dataset sample:
{json.dumps(sample, indent=2)}
"""

    payload = {
        "model": "mistral-7b-instruct-v0.3.Q4_K_M.gguf",
        "prompt": prompt,
        "temperature": 0.3,
        "max_tokens": 2000,
    }

    try:
        print("🤖 Analyzing dataset with Mistral 7B...")
        res = requests.post(LM_API_URL, json=payload, timeout=180)
        res.raise_for_status()
        data = res.json()
        text = data["choices"][0].get("text", "").strip()

        # Extract JSON section
        json_start = text.find('{')
        json_end = text.rfind('}') + 1
        json_part = text[json_start:json_end] if json_start >= 0 else text

        try:
            result = json.loads(json_part)
            if "processed_data" in result and isinstance(result["processed_data"], list):
                print("✅ AI Analysis Complete:")
                print(f"📊 Data Type: {result.get('data_type', 'Unknown')}")
                print(f"🏷️  Labels: {result.get('label_explanation', 'N/A')}")
                
                processed_df = pd.DataFrame(result["processed_data"])
                
                # Add metadata as attributes
                processed_df.attrs['data_type'] = result.get('data_type', 'Unknown')
                processed_df.attrs['label_explanation'] = result.get('label_explanation', 'N/A')
                
                return processed_df
        except Exception as e:
            print(f"⚠️ Failed to parse AI response: {e}")
    except Exception as e:
        print(f"❌ LM Studio error: {e}")

    # Enhanced fallback processing

    print("🔄 Using enhanced fallback processing...")
    return intelligent_fallback_processing(df)

def improve_headers_if_needed(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detect and improve column headers using AI if they appear to be generic or poor quality
    """
    headers = list(df.columns)
    
    # Check if headers need improvement
    needs_improvement = any([
        # Generic column names
        col.startswith(('Unnamed:', 'Column', 'col_', 'column_')) or
        # Numeric only headers
        col.isdigit() or
        # Very short or single character headers
        len(col.strip()) <= 1 or
        # Headers that are just numbers or positions
        col in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
        for col in headers
    ])
    
    if needs_improvement or len([col for col in headers if col.startswith('Unnamed')]) > 0:
        print("🔍 Detecting poor quality headers, attempting AI improvement...")
        
        # Get sample rows for header detection
        sample_rows = df.head(3).values.tolist()
        improved_headers = ai_detect_headers(sample_rows)
        
        if len(improved_headers) == len(df.columns):
            df.columns = improved_headers
            print(f"✅ Headers improved: {improved_headers}")
        else:
            print("⚠️ Header count mismatch, keeping original headers")
    
    return df

def intelligent_fallback_processing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enhanced fallback processing that intelligently analyzes the data structure
    """
    # Clean column names
    df.columns = [str(col).strip().lower().replace(' ', '_') for col in df.columns]
    
    # Detect data types and patterns
    text_columns = []
    numeric_columns = []
    date_columns = []
    
    for col in df.columns:
        if df[col].dtype == 'object':
            # Check if it might be numeric stored as text
            try:
                pd.to_numeric(df[col].dropna().iloc[:5])
                numeric_columns.append(col)
                df[col] = pd.to_numeric(df[col], errors='coerce')
            except:
                # Check if it might be dates
                try:
                    pd.to_datetime(df[col].dropna().iloc[:5])
                    date_columns.append(col)
                except:
                    text_columns.append(col)
        else:
            numeric_columns.append(col)
    
    print(f"📊 Data analysis: {len(text_columns)} text, {len(numeric_columns)} numeric, {len(date_columns)} date columns")
    
    # Intelligent labeling based on detected patterns
    labels_added = []
    
    # For text columns, add sentiment/category analysis
    for col in text_columns:
        if any(keyword in col for keyword in ['review', 'comment', 'feedback', 'description', 'text']):
            df[f'{col}_sentiment'] = df[col].apply(get_text_sentiment)
            labels_added.append(f'{col}_sentiment')
    
    # For numeric columns, add statistical categories
    for col in numeric_columns:
        if df[col].notna().sum() > 0:  # Only if column has data
            if 'age' in col or 'year' in col:
                df[f'{col}_category'] = df[col].apply(get_age_category)
                labels_added.append(f'{col}_category')
            elif 'price' in col or 'cost' in col or 'amount' in col:
                df[f'{col}_range'] = df[col].apply(lambda x: get_price_range(x, df[col]))
                labels_added.append(f'{col}_range')
    
    # General data quality label
    df['data_quality'] = 'Good'
    for idx, row in df.iterrows():
        missing_count = row.isna().sum()
        if missing_count > len(df.columns) * 0.5:
            df.loc[idx, 'data_quality'] = 'Poor'
        elif missing_count > len(df.columns) * 0.2:
            df.loc[idx, 'data_quality'] = 'Fair'
    
    labels_added.append('data_quality')
    
    # Fill missing values intelligently
    for col in df.columns:
        if df[col].isna().any():
            if col in text_columns:
                df[col] = df[col].fillna('Unknown')
            elif col in numeric_columns:
                df[col] = df[col].fillna(df[col].median())
    
    print(f"✅ Enhanced fallback processing complete. Added labels: {labels_added}")
    df.attrs['processing_type'] = 'Enhanced Fallback'
    df.attrs['labels_added'] = labels_added
    
    return df

def get_text_sentiment(text):
    """Simple sentiment analysis"""
    if pd.isna(text):
        return 'Neutral'
    
    text = str(text).lower()
    positive_words = ['good', 'great', 'excellent', 'amazing', 'love', 'best', 'awesome', 'fantastic']
    negative_words = ['bad', 'terrible', 'awful', 'hate', 'worst', 'poor', 'horrible', 'disappointing']
    
    pos_count = sum(1 for word in positive_words if word in text)
    neg_count = sum(1 for word in negative_words if word in text)
    
    if pos_count > neg_count:
        return 'Positive'
    elif neg_count > pos_count:
        return 'Negative'
    else:
        return 'Neutral'

def get_age_category(age):
    """Categorize age values"""
    if pd.isna(age):
        return 'Unknown'
    
    age = float(age)
    if age < 18:
        return 'Minor'
    elif age < 30:
        return 'Young Adult'
    elif age < 50:
        return 'Adult'
    elif age < 65:
        return 'Middle Age'
    else:
        return 'Senior'

def get_price_range(value, series):
    """Categorize numeric values into ranges based on distribution"""
    if pd.isna(value):
        return 'Unknown'
    
    q25, q75 = series.quantile([0.25, 0.75])
    
    if value <= q25:
        return 'Low'
    elif value <= q75:
        return 'Medium'
    else:
        return 'High'

# Legacy function for backward compatibility
def ai_clean_and_label(df: pd.DataFrame):
    """Legacy function - redirects to new intelligent processing"""
    return ai_analyze_and_process(df)
