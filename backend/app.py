from fastapi import FastAPI, UploadFile, HTTPException
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import os
import json
import io
import re
import numpy as np
import requests
from datetime import datetime
from cleaning import clean_data
from labeling import simple_sentiment_label
from ai_cleaner import ai_analyze_and_process, ai_clean_and_label
from ai_data_analyzer import AIDataAnalyzer
from ai_log_processor import AILogProcessor
from pdf_to_dataset_processor import PDFToDatasetProcessor

app = FastAPI(
    title="DataSmith AI - Intelligent CSV Processor",
    description="Automatically analyze, clean, and label any CSV file using Mistral 7B AI",
    version="2.0.0"
)

def clean_for_json(data):
    """Clean data structure for JSON serialization by replacing NaN values and converting pandas types"""
    if isinstance(data, dict):
        return {str(k): clean_for_json(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [clean_for_json(item) for item in data]
    elif isinstance(data, (np.integer, np.int64, np.int32)):
        return int(data)
    elif isinstance(data, (np.floating, np.float64, np.float32)):
        if np.isnan(data) or np.isinf(data):
            return None
        return float(data)
    elif isinstance(data, np.bool_):
        return bool(data)
    elif isinstance(data, np.ndarray):
        return clean_for_json(data.tolist())
    elif isinstance(data, np.dtype):
        return str(data)
    elif hasattr(data, 'dtype') and hasattr(data, '__iter__'):
        # Handle pandas Series and other iterables with dtype
        try:
            if hasattr(data, 'to_dict'):
                return clean_for_json(data.to_dict())
            elif hasattr(data, 'tolist'):
                return clean_for_json(data.tolist())
            else:
                return [clean_for_json(item) for item in data]
        except:
            return str(data)
    elif pd.isna(data):
        return None
    elif data is None:
        return None
    else:
        # Convert any remaining non-serializable types to string
        try:
            # Test if it's JSON serializable
            import json
            json.dumps(data)
            return data
        except (TypeError, ValueError):
            return str(data)

# Add CORS middleware to allow frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],  # Frontend origins
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# ===== LM STUDIO PROXY ENDPOINTS =====

@app.get("/lm-studio/models")
async def get_lm_studio_models(lm_studio_url: str = "http://localhost:1234"):
    """
    Proxy endpoint to get available models from LM Studio
    This avoids CORS issues when connecting from the frontend
    """
    import requests
    
    try:
        response = requests.get(f"{lm_studio_url}/v1/models", timeout=5)
        
        if response.status_code == 200:
            data = response.json()
            return JSONResponse(content={
                "success": True,
                "models": data.get("data", []),
                "total_models": len(data.get("data", [])),
                "lm_studio_url": lm_studio_url
            })
        else:
            raise HTTPException(
                status_code=response.status_code,
                detail=f"LM Studio returned status code: {response.status_code}"
            )
            
    except requests.exceptions.ConnectionError:
        raise HTTPException(
            status_code=503,
            detail="Could not connect to LM Studio. Make sure LM Studio is running and the local server is started."
        )
    except requests.exceptions.Timeout:
        raise HTTPException(
            status_code=504,
            detail="Connection to LM Studio timed out. Check if the server is responding."
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to connect to LM Studio: {str(e)}"
        )

@app.post("/lm-studio/test-connection")
async def test_lm_studio_connection(request_data: dict):
    """
    Test connection to LM Studio and return connection status
    """
    import requests
    
    lm_studio_url = request_data.get("lm_studio_url", "http://localhost:1234")
    
    try:
        # Test connection by getting models
        response = requests.get(f"{lm_studio_url}/v1/models", timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            models = data.get("data", [])
            
            return JSONResponse(content={
                "success": True,
                "connected": True,
                "message": f"Successfully connected to LM Studio! Found {len(models)} available models.",
                "models": models,
                "lm_studio_url": lm_studio_url,
                "server_info": {
                    "total_models": len(models),
                    "response_time_ms": response.elapsed.total_seconds() * 1000
                }
            })
        else:
            return JSONResponse(content={
                "success": False,
                "connected": False,
                "message": f"LM Studio server responded with status {response.status_code}",
                "lm_studio_url": lm_studio_url
            })
            
    except requests.exceptions.ConnectionError:
        return JSONResponse(content={
            "success": False,
            "connected": False,
            "message": "Could not connect to LM Studio. Please check:\n1. LM Studio is installed and running\n2. Local server is started in LM Studio\n3. Server URL is correct (default: http://localhost:1234)",
            "lm_studio_url": lm_studio_url,
            "troubleshooting": [
                "Open LM Studio application",
                "Go to 'Local Server' tab",
                "Click 'Start Server'",
                "Ensure the port matches your URL (default: 1234)"
            ]
        })
    except requests.exceptions.Timeout:
        return JSONResponse(content={
            "success": False,
            "connected": False,
            "message": "Connection to LM Studio timed out. The server may be overloaded or not responding.",
            "lm_studio_url": lm_studio_url
        })
    except Exception as e:
        return JSONResponse(content={
            "success": False,
            "connected": False,
            "message": f"Unexpected error connecting to LM Studio: {str(e)}",
            "lm_studio_url": lm_studio_url
        })

def load_csv_with_header_detection(content):
    """
    Load CSV and detect if it has proper headers
    Returns (DataFrame, has_proper_headers)
    """
    # Try to load CSV normally first
    try:
        df = pd.read_csv(io.BytesIO(content))
        
        # Check if headers look like actual headers vs data
        has_proper_headers = detect_proper_headers(df.columns.tolist(), df)
        
        if not has_proper_headers:
            # Try loading without headers
            df_no_header = pd.read_csv(io.BytesIO(content), header=None)
            return df_no_header, False
        
        return df, True
        
    except Exception:
        # Try different encodings and separators
        for encoding in ['utf-8', 'latin1', 'cp1252']:
            for sep in [',', ';', '\t']:
                try:
                    df = pd.read_csv(io.BytesIO(content), encoding=encoding, sep=sep)
                    has_proper_headers = detect_proper_headers(df.columns.tolist(), df)
                    
                    if not has_proper_headers:
                        df_no_header = pd.read_csv(io.BytesIO(content), encoding=encoding, sep=sep, header=None)
                        return df_no_header, False
                    
                    print(f"✅ Loaded with encoding={encoding}, sep='{sep}'")
                    return df, True
                except:
                    continue
        
        raise HTTPException(status_code=400, detail="Could not parse CSV file")

def detect_proper_headers(column_names, df):
    """
    Detect if column names are proper headers or just data from first row
    """
    if not column_names:
        return False
    
    # Check for obvious signs of poor headers
    poor_header_indicators = [
        # Generic names
        lambda col: col.startswith(('Unnamed:', 'Column', 'col_', 'column_')),
        # Numeric only
        lambda col: str(col).strip().isdigit(),
        # Very short or empty
        lambda col: len(str(col).strip()) <= 1,
        # Looks like actual data values (names, ages, etc.)
        lambda col: is_likely_data_value(str(col), df),
        # Contains only spaces or special chars
        lambda col: re.match(r'^[\s\-_\.]+$', str(col)),
        # Specific data patterns that indicate it's not a header
        lambda col: str(col).strip() in ['P001', 'P002', 'P003', 'P004', 'P005'],  # ID patterns
        lambda col: str(col).strip() in ['John', 'Sarah', 'Alice', 'Bob', 'Charlie', 'David', 'Eve'],  # Common names
        lambda col: str(col).strip() in ['Male', 'Female', 'M', 'F'],  # Gender values
        lambda col: str(col).strip() in ['Normal', 'Fever', 'High BP', 'Good', 'Bad', 'Average'],  # Status values
        lambda col: re.match(r'^\d{1,3}(\.\d+)?$', str(col).strip()),  # Looks like age/temperature (45, 98.6)
    ]
    
    poor_headers_count = 0
    for col in column_names:
        for indicator in poor_header_indicators:
            if indicator(col):
                poor_headers_count += 1
                break
    
    # If more than 40% of headers look like data, probably no proper headers (more aggressive)
    return poor_headers_count < len(column_names) * 0.4

def is_likely_data_value(value, df):
    """
    Check if a value looks like actual data rather than a column header
    """
    value = str(value).strip()
    
    # Common patterns that suggest it's data, not a header
    data_patterns = [
        # Looks like a person's name (starts with capital, has spaces)
        lambda v: len(v.split()) == 2 and all(word.istitle() for word in v.split()),
        # Single capitalized word (likely a name)
        lambda v: len(v.split()) == 1 and v.istitle() and len(v) >= 3,
        # Looks like a number with trailing spaces (age, year, etc.)
        lambda v: v.strip().isdigit() and (v.startswith(' ') or v.endswith(' ')),
        # Looks like a year or age
        lambda v: v.strip().isdigit() and (1900 <= int(v.strip()) <= 2030 or 0 <= int(v.strip()) <= 150),
        # Looks like temperature or decimal number
        lambda v: re.match(r'^\d{1,3}\.\d{1}$', v.strip()),
        # Contains typical data indicators
        lambda v: any(word in v.lower() for word in ['service', 'experience', 'good', 'bad', 'average', 'excellent', 'normal', 'fever', 'high bp']),
        # Looks like a location with specific format
        lambda v: any(city in v for city in ['New York', 'Los Angeles', 'Chicago', 'Boston', 'Miami']),
        # Looks like gender
        lambda v: v.strip().lower() in ['male', 'female', 'm', 'f'],
        # Has leading/trailing spaces (common in messy data)
        lambda v: v != v.strip() and len(v.strip()) > 2,
        # Looks like ID pattern
        lambda v: re.match(r'^[A-Z]\d{3}$', v.strip()),  # P001, P002, etc.
        # Common first names
        lambda v: v.strip() in ['John', 'Sarah', 'Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Mary', 'James', 'Patricia', 'Michael', 'Linda', 'William', 'Elizabeth', 'Richard', 'Barbara', 'Joseph', 'Susan', 'Thomas', 'Jessica', 'Christopher', 'Karen', 'Charles', 'Nancy', 'Daniel', 'Betty', 'Matthew', 'Helen', 'Anthony', 'Sandra', 'Mark', 'Donna', 'Donald', 'Carol', 'Steven', 'Ruth', 'Paul', 'Sharon', 'Andrew', 'Michelle', 'Joshua', 'Laura', 'Kenneth', 'Sarah', 'Kevin', 'Kimberly', 'Brian', 'Deborah', 'George', 'Dorothy', 'Timothy', 'Lisa', 'Ronald', 'Nancy', 'Edward', 'Karen', 'Jason', 'Betty', 'Jeffrey', 'Helen', 'Ryan', 'Sandra', 'Jacob', 'Donna', 'Gary', 'Carol', 'Nicholas', 'Ruth', 'Eric', 'Sharon', 'Jonathan', 'Michelle', 'Stephen', 'Laura', 'Larry', 'Sarah', 'Justin', 'Kimberly', 'Scott', 'Deborah', 'Brandon', 'Dorothy', 'Benjamin', 'Amy', 'Samuel', 'Angela', 'Gregory', 'Ashley', 'Alexander', 'Brenda', 'Frank', 'Emma', 'Patrick', 'Olivia', 'Raymond', 'Cynthia', 'Jack', 'Marie', 'Dennis', 'Janet', 'Jerry', 'Catherine', 'Tyler', 'Frances', 'Aaron', 'Christine', 'Jose', 'Samantha', 'Henry', 'Debra', 'Adam', 'Rachel', 'Douglas', 'Carolyn', 'Nathan', 'Janet', 'Peter', 'Virginia', 'Zachary', 'Maria', 'Kyle', 'Heather', 'Noah', 'Diane', 'Alan', 'Julie', 'Ethan', 'Joyce', 'Jeremy', 'Victoria', 'Lionel', 'Kelly', 'Carl', 'Christina', 'Wayne', 'Joan', 'Ralph', 'Evelyn', 'Roy', 'Lauren', 'Eugene', 'Judith', 'Louis', 'Megan', 'Philip', 'Cheryl', 'Bobby', 'Andrea', 'Johnny', 'Hannah', 'Mason', 'Jacqueline', 'Arun', 'Pooja', 'Ravi', 'Priya', 'Amit', 'Neha', 'Raj', 'Anjali', 'Vikash', 'Sneha']
    ]
    
    return any(pattern(value) for pattern in data_patterns)

@app.get("/")
def root():
    return {
        "message": "🚀 Welcome to DataSmith AI - Intelligent CSV Processing",
        "features": [
            "🤖 AI-powered header detection",
            "📊 Automatic data type recognition", 
            "🏷️ Smart labeling and categorization",
            "🧹 Intelligent data cleaning",
            "📈 Statistical insights generation"
        ],
        "endpoints": {
            "/process/": "Process CSV with AI analysis (recommended)",
            "/process/legacy": "Traditional processing mode",
            "/analyze/": "Quick data analysis without processing",
            "/convert/csv-to-json/": "Convert CSV files to JSON format",
            "/convert/csv-to-json-smart/": "Smart CSV to JSON with AI headers (recommended)",
            "/convert/json-to-csv/": "Convert JSON files to CSV format"
        }
    }

@app.post("/process/")
async def intelligent_process(file: UploadFile, force_ai: bool = True):
    """
    🤖 Intelligent CSV processing using Mistral 7B AI
    Automatically detects headers, data types, and generates meaningful labels
    """
    try:
        # Validate file
        if not file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail="Only CSV files are supported")
        
        # Ensure directories exist
        os.makedirs("../data/raw", exist_ok=True)
        os.makedirs("../data/cleaned", exist_ok=True) 
        os.makedirs("../data/labeled", exist_ok=True)

        # Save uploaded file
        raw_path = f"../data/raw/{file.filename}"
        with open(raw_path, "wb") as f:
            content = await file.read()
            f.write(content)

        print(f"📁 Processing file: {file.filename}")
        
        # Load dataset with flexible header detection
        try:
            # First, try to load normally
            df = pd.read_csv(raw_path)
            print(f"📊 Loaded dataset: {df.shape[0]} rows, {df.shape[1]} columns")
        except Exception as e:
            # Try different encodings and separators
            for encoding in ['utf-8', 'latin1', 'cp1252']:
                for sep in [',', ';', '\t']:
                    try:
                        df = pd.read_csv(raw_path, encoding=encoding, sep=sep)
                        print(f"✅ Loaded with encoding={encoding}, sep='{sep}'")
                        break
                    except:
                        continue
            else:
                raise HTTPException(status_code=400, detail="Could not parse CSV file")

        # 🤖 AI Processing
        if force_ai:
            print("🤖 Starting AI analysis with Mistral 7B...")
            df_processed = ai_analyze_and_process(df)
            processing_mode = "AI-Enhanced"
        else:
            # Legacy processing
            df_cleaned = clean_data(df)
            df_processed = simple_sentiment_label(df_cleaned, "review")
            processing_mode = "Traditional"

        # Save processed data
        output_filename = f"ai_processed_{file.filename}"
        labeled_path = f"../data/labeled/{output_filename}"
        df_processed.to_csv(labeled_path, index=False)

        # Prepare response with insights
        response_data = {
            "success": True,
            "message": f"✅ {processing_mode} processing completed",
            "file_info": {
                "original_name": file.filename,
                "processed_name": output_filename,
                "rows_processed": len(df_processed),
                "columns": list(df_processed.columns),
                "data_types": {col: str(dtype) for col, dtype in df_processed.dtypes.items()}
            },
            "processing_details": {
                "mode": processing_mode,
                "ai_enabled": force_ai,
                "output_path": labeled_path
            }
        }

        # Add AI insights if available
        if hasattr(df_processed, 'attrs'):
            if 'data_type' in df_processed.attrs:
                response_data["ai_insights"] = {
                    "data_type": df_processed.attrs.get('data_type'),
                    "label_explanation": df_processed.attrs.get('label_explanation'),
                    "processing_type": df_processed.attrs.get('processing_type')
                }
            if 'labels_added' in df_processed.attrs:
                response_data["ai_insights"]["labels_added"] = df_processed.attrs.get('labels_added')

        # Add sample of processed data
        response_data["sample_data"] = df_processed.head(3).to_dict('records')

        return JSONResponse(content=response_data)

    except Exception as e:
        print(f"❌ Processing error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")

@app.post("/process/legacy")
async def legacy_process(file: UploadFile):
    """
    📊 Traditional processing mode (without AI)
    """
    return await intelligent_process(file, force_ai=False)

@app.post("/analyze/")
async def quick_analyze(file: UploadFile):
    """
    🔍 Quick data analysis without full processing
    Returns insights about the dataset structure and content
    """
    try:
        if not file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail="Only CSV files are supported")

        # Read file content
        content = await file.read()
        
        # Try to load the CSV
        try:
            df = pd.read_csv(pd.io.common.BytesIO(content))
        except:
            # Try different encodings
            for encoding in ['latin1', 'cp1252']:
                try:
                    df = pd.read_csv(pd.io.common.BytesIO(content), encoding=encoding)
                    break
                except:
                    continue
            else:
                raise HTTPException(status_code=400, detail="Could not parse CSV file")

        # Generate analysis
        analysis = {
            "file_name": file.filename,
            "dimensions": {"rows": len(df), "columns": len(df.columns)},
            "columns": {
                "names": list(df.columns),
                "types": {col: str(dtype) for col, dtype in df.dtypes.items()}
            },
            "data_quality": {
                "missing_values": df.isnull().sum().to_dict(),
                "duplicate_rows": df.duplicated().sum(),
                "completeness": f"{((df.size - df.isnull().sum().sum()) / df.size * 100):.1f}%"
            },
            "sample_data": df.head(5).to_dict('records'),
            "recommendations": []
        }

        # Add recommendations
        if df.isnull().sum().sum() > 0:
            analysis["recommendations"].append("🧹 Dataset has missing values - AI processing can handle these intelligently")
        
        if any(col.startswith('Unnamed') for col in df.columns):
            analysis["recommendations"].append("🏷️ Poor column headers detected - AI can improve these")
            
        if len(df.select_dtypes(include=['object']).columns) > 0:
            analysis["recommendations"].append("📝 Text data found - AI can generate sentiment and category labels")

        analysis["recommendations"].append("🤖 Use /process/ endpoint for full AI analysis and enhancement")

        return JSONResponse(content=analysis)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")

@app.post("/convert/csv-to-json/")
async def csv_to_json(file: UploadFile, format_type: str = "records", pretty: bool = True):
    """
    🔄 Convert CSV file to JSON format
    
    Format options:
    - 'records': [{"col1": "val1", "col2": "val2"}, {...}] (default)
    - 'index': {"0": {"col1": "val1"}, "1": {"col2": "val2"}}
    - 'values': [["val1", "val2"], ["val3", "val4"]]
    - 'split': {"columns": ["col1", "col2"], "data": [["val1", "val2"]]}
    """
    try:
        if not file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail="Only CSV files are supported")

        # Ensure directories exist
        os.makedirs("../data/converted", exist_ok=True)
        
        print(f"🔄 Converting CSV to JSON: {file.filename}")
        
        # Read CSV file
        content = await file.read()
        
        # First, detect if CSV has proper headers
        df, has_proper_headers = load_csv_with_header_detection(content)
        
        print(f"🔍 Header detection result: {'Proper headers found' if has_proper_headers else 'Poor/No headers detected'}")
        print(f"� Current columns: {list(df.columns)}")
        
        if not has_proper_headers:
            print("🤖 Using AI to generate intelligent column names...")
            # Use AI to detect proper headers
            from ai_header_detect import ai_detect_headers
            sample_rows = df.head(3).values.tolist()
            print(f"📊 Sample data for AI analysis: {sample_rows}")
            
            improved_headers = ai_detect_headers(sample_rows)
            
            if len(improved_headers) == len(df.columns):
                df.columns = improved_headers
                print(f"✅ Applied AI-generated headers: {improved_headers}")
            else:
                print("⚠️ Header count mismatch, using intelligent fallback")
                # Create intelligent fallback based on data patterns
                fallback_headers = []
                for i, col in enumerate(df.columns):
                    sample_val = str(df.iloc[0, i]) if len(df) > 0 else ""
                    if re.match(r'^[A-Z]\d{3,}', sample_val):
                        fallback_headers.append("id")
                    elif sample_val.replace('.', '').replace(' ', '').isalpha() and len(sample_val.split()) <= 2:
                        fallback_headers.append("name")
                    elif sample_val.strip().isdigit() and 0 <= int(sample_val.strip()) <= 150:
                        fallback_headers.append("age")
                    elif sample_val.strip().lower() in ['male', 'female', 'm', 'f']:
                        fallback_headers.append("gender")
                    elif re.match(r'^\d{2,3}\.\d$', sample_val.strip()):
                        fallback_headers.append("temperature")
                    elif any(word in sample_val.lower() for word in ['normal', 'fever', 'high', 'good', 'bad', 'excellent']):
                        fallback_headers.append("status")
                    else:
                        fallback_headers.append(f"column_{i+1}")
                
                df.columns = fallback_headers
                print(f"🎯 Applied intelligent fallback headers: {fallback_headers}")
        else:
            print("✅ Using existing proper headers")
            
        print(f"🏁 Final column names: {list(df.columns)}")

        # Convert to specified JSON format
        if format_type == "records":
            json_data = df.to_dict('records')
        elif format_type == "index":
            json_data = df.to_dict('index')
        elif format_type == "values":
            json_data = df.values.tolist()
        elif format_type == "split":
            json_data = df.to_dict('split')
        elif format_type == "columns":
            json_data = df.to_dict('dict')
        else:
            json_data = df.to_dict('records')  # default

        # Save JSON file
        json_filename = file.filename.replace('.csv', '.json')
        json_path = f"../data/converted/{json_filename}"
        
        with open(json_path, 'w', encoding='utf-8') as f:
            if pretty:
                json.dump(json_data, f, indent=2, ensure_ascii=False, default=str)
            else:
                json.dump(json_data, f, ensure_ascii=False, default=str)

        # Prepare response
        response_data = {
            "success": True,
            "message": "✅ CSV successfully converted to JSON",
            "conversion_info": {
                "input_file": file.filename,
                "output_file": json_filename,
                "format_type": format_type,
                "pretty_formatted": pretty,
                "records_count": len(df),
                "columns_count": len(df.columns),
                "output_path": json_path,
                "header_detection": {
                    "had_proper_headers": has_proper_headers,
                    "ai_generated_headers": not has_proper_headers,
                    "final_columns": list(df.columns)
                }
            },
            "data_preview": {
                "columns": list(df.columns),
                "sample_records": df.head(3).to_dict('records'),
                "json_structure": type(json_data).__name__
            }
        }

        return JSONResponse(content=response_data)

    except Exception as e:
        print(f"❌ CSV to JSON conversion error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Conversion failed: {str(e)}")

@app.post("/convert/csv-to-json-smart/")
async def csv_to_json_smart(file: UploadFile, format_type: str = "records", pretty: bool = True):
    """
    🧠 Smart CSV to JSON conversion - Always uses AI to generate perfect headers
    Forces AI header detection regardless of current header quality
    """
    try:
        if not file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail="Only CSV files are supported")

        # Ensure directories exist
        os.makedirs("../data/converted", exist_ok=True)
        
        print(f"🧠 Smart converting CSV to JSON: {file.filename}")
        
        # Read CSV file without headers (force treating first row as data)
        content = await file.read()
        
        try:
            df = pd.read_csv(io.BytesIO(content), header=None)
        except Exception:
            # Try different encodings and separators
            for encoding in ['utf-8', 'latin1', 'cp1252']:
                for sep in [',', ';', '\t']:
                    try:
                        df = pd.read_csv(io.BytesIO(content), encoding=encoding, sep=sep, header=None)
                        print(f"✅ Loaded with encoding={encoding}, sep='{sep}'")
                        break
                    except:
                        continue
                if 'df' in locals():
                    break
            else:
                raise HTTPException(status_code=400, detail="Could not parse CSV file")

        print(f"📊 Loaded as headerless CSV: {df.shape}")
        
        # Always use AI to generate smart headers
        print("🤖 Generating intelligent headers with AI...")
        from ai_header_detect import ai_detect_headers
        
        sample_rows = df.head(3).values.tolist()
        print(f"📋 Sample data for AI: {sample_rows}")
        
        smart_headers = ai_detect_headers(sample_rows)
        
        if len(smart_headers) == len(df.columns):
            df.columns = smart_headers
            print(f"✅ Applied AI-generated headers: {smart_headers}")
        else:
            # Enhanced fallback with data pattern analysis
            print("🎯 Using enhanced pattern-based headers...")
            enhanced_headers = []
            
            for i in range(len(df.columns)):
                col_data = df.iloc[:, i].astype(str).str.strip()
                sample_vals = col_data.head(3).tolist()
                
                # Analyze patterns in the column data
                if any(re.match(r'^[A-Z]\d{3,}', val) for val in sample_vals):
                    enhanced_headers.append("patient_id")
                elif any(val.replace(' ', '').isalpha() and len(val.split()) <= 2 for val in sample_vals):
                    enhanced_headers.append("name")
                elif any(val.isdigit() and 0 <= int(val) <= 150 for val in sample_vals if val.isdigit()):
                    enhanced_headers.append("age")
                elif any(val.lower() in ['male', 'female', 'm', 'f'] for val in sample_vals):
                    enhanced_headers.append("gender")
                elif any(re.match(r'^\d{2,3}\.\d{1,2}$', val) for val in sample_vals):
                    enhanced_headers.append("temperature")
                elif any(word in ' '.join(sample_vals).lower() for word in ['normal', 'fever', 'high bp', 'good', 'bad', 'excellent', 'average']):
                    enhanced_headers.append("diagnosis")
                else:
                    enhanced_headers.append(f"field_{i+1}")
            
            df.columns = enhanced_headers
            print(f"🎯 Applied pattern-based headers: {enhanced_headers}")

        # Convert to JSON format
        if format_type == "records":
            json_data = df.to_dict('records')
        elif format_type == "index":
            json_data = df.to_dict('index')
        elif format_type == "values":
            json_data = df.values.tolist()
        elif format_type == "split":
            json_data = df.to_dict('split')
        elif format_type == "columns":
            json_data = df.to_dict('dict')
        else:
            json_data = df.to_dict('records')

        # Save JSON file
        json_filename = file.filename.replace('.csv', '_smart.json')
        json_path = f"../data/converted/{json_filename}"
        
        with open(json_path, 'w', encoding='utf-8') as f:
            if pretty:
                json.dump(json_data, f, indent=2, ensure_ascii=False, default=str)
            else:
                json.dump(json_data, f, ensure_ascii=False, default=str)

        # Prepare response
        response_data = {
            "success": True,
            "message": "🧠 CSV successfully converted to JSON with smart headers",
            "conversion_info": {
                "input_file": file.filename,
                "output_file": json_filename,
                "format_type": format_type,
                "pretty_formatted": pretty,
                "records_count": len(df),
                "columns_count": len(df.columns),
                "output_path": json_path,
                "smart_processing": True,
                "ai_generated_headers": True,
                "final_columns": list(df.columns)
            },
            "data_preview": {
                "columns": list(df.columns),
                "sample_records": df.head(3).to_dict('records'),
                "json_structure": type(json_data).__name__
            }
        }

        return JSONResponse(content=response_data)

    except Exception as e:
        print(f"❌ Smart CSV to JSON conversion error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Smart conversion failed: {str(e)}")

@app.post("/convert/json-to-csv/")
async def json_to_csv(file: UploadFile, auto_flatten: bool = True):
    """
    🔄 Convert JSON file to CSV format
    
    Options:
    - auto_flatten: Automatically flatten nested JSON structures
    """
    try:
        if not file.filename.endswith('.json'):
            raise HTTPException(status_code=400, detail="Only JSON files are supported")

        # Ensure directories exist
        os.makedirs("../data/converted", exist_ok=True)
        
        print(f"🔄 Converting JSON to CSV: {file.filename}")
        
        # Read JSON file
        content = await file.read()
        
        try:
            json_data = json.loads(content.decode('utf-8'))
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=400, detail=f"Invalid JSON format: {str(e)}")
        except UnicodeDecodeError:
            # Try different encodings
            for encoding in ['latin1', 'cp1252']:
                try:
                    json_data = json.loads(content.decode(encoding))
                    break
                except:
                    continue
            else:
                raise HTTPException(status_code=400, detail="Could not decode JSON file")

        # Convert JSON to DataFrame
        try:
            if isinstance(json_data, list):
                # List of records format
                df = pd.json_normalize(json_data) if auto_flatten else pd.DataFrame(json_data)
            elif isinstance(json_data, dict):
                # Dictionary format - try different approaches
                if all(isinstance(v, dict) for v in json_data.values()):
                    # Index-based format: {"0": {"col1": "val1"}, "1": {"col2": "val2"}}
                    df = pd.DataFrame.from_dict(json_data, orient='index')
                elif all(isinstance(v, list) for v in json_data.values()):
                    # Column-based format: {"col1": ["val1", "val2"], "col2": ["val3", "val4"]}
                    df = pd.DataFrame(json_data)
                else:
                    # Single record or mixed format
                    df = pd.json_normalize([json_data]) if auto_flatten else pd.DataFrame([json_data])
            else:
                raise ValueError("Unsupported JSON structure")
                
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Could not convert JSON to tabular format: {str(e)}")

        # Clean column names (remove dots from normalization)
        df.columns = [col.replace('.', '_') for col in df.columns]
        
        # Save CSV file
        csv_filename = file.filename.replace('.json', '.csv')
        csv_path = f"../data/converted/{csv_filename}"
        
        df.to_csv(csv_path, index=False, encoding='utf-8')

        # Prepare response
        response_data = {
            "success": True,
            "message": "✅ JSON successfully converted to CSV",
            "conversion_info": {
                "input_file": file.filename,
                "output_file": csv_filename,
                "auto_flatten": auto_flatten,
                "records_count": len(df),
                "columns_count": len(df.columns),
                "output_path": csv_path
            },
            "data_preview": {
                "columns": list(df.columns),
                "sample_records": df.head(3).to_dict('records'),
                "original_json_type": type(json_data).__name__
            }
        }

        return JSONResponse(content=response_data)

    except Exception as e:
        print(f"❌ JSON to CSV conversion error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Conversion failed: {str(e)}")

@app.get("/convert/download/{file_type}/{filename}")
async def download_converted_file(file_type: str, filename: str):
    """
    📥 Download converted files (CSV or JSON)
    """
    try:
        if file_type not in ['csv', 'json']:
            raise HTTPException(status_code=400, detail="File type must be 'csv' or 'json'")
        
        file_path = f"../data/converted/{filename}"
        
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="File not found")
        
        # Determine media type
        media_type = "text/csv" if file_type == "csv" else "application/json"
        
        return FileResponse(
            path=file_path,
            filename=filename,
            media_type=media_type
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Download failed: {str(e)}")

@app.post("/convert/batch/")
async def batch_convert(files: list[UploadFile], target_format: str = "json"):
    """
    🔄 Batch convert multiple files between CSV and JSON
    """
    try:
        if target_format not in ['csv', 'json']:
            raise HTTPException(status_code=400, detail="Target format must be 'csv' or 'json'")
        
        results = []
        errors = []
        
        for file in files:
            try:
                if target_format == "json" and file.filename.endswith('.csv'):
                    # Convert CSV to JSON
                    result = await csv_to_json(file, format_type="records", pretty=True)
                    results.append({
                        "file": file.filename,
                        "status": "success",
                        "conversion": "csv_to_json"
                    })
                elif target_format == "csv" and file.filename.endswith('.json'):
                    # Convert JSON to CSV
                    result = await json_to_csv(file, auto_flatten=True)
                    results.append({
                        "file": file.filename,
                        "status": "success", 
                        "conversion": "json_to_csv"
                    })
                else:
                    errors.append({
                        "file": file.filename,
                        "error": f"Cannot convert {file.filename} to {target_format}"
                    })
            except Exception as e:
                errors.append({
                    "file": file.filename,
                    "error": str(e)
                })
        
        return JSONResponse(content={
            "message": f"Batch conversion completed",
            "successful_conversions": len(results),
            "failed_conversions": len(errors),
            "results": results,
            "errors": errors
        })
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Batch conversion failed: {str(e)}")

@app.post("/analyze/auto-report")
async def generate_auto_report(file: UploadFile):
    """
    Generate comprehensive AI-powered data understanding report
    """
    try:
        # Validate file type
        if not file.filename.lower().endswith('.csv'):
            raise HTTPException(status_code=400, detail=f"Only CSV files are supported for AI analysis. Received: {file.filename}")
        
        # Read and process the CSV file
        content = await file.read()
        df, detected_headers = load_csv_with_header_detection(content)
        
        if df.empty:
            raise HTTPException(status_code=400, detail="The uploaded file is empty or invalid")
        
        # Initialize AI Data Analyzer
        analyzer = AIDataAnalyzer()
        
        # Generate comprehensive report
        report = analyzer.generate_comprehensive_report(df, file.filename)
        
        # Add header detection info
        report["header_detection"] = {
            "detected_headers": detected_headers,
            "header_quality": "Good" if detected_headers else "Auto-generated"
        }
        
        return JSONResponse(content={
            "message": "AI Data Understanding Report generated successfully",
            "report": report,
            "filename": file.filename
        })
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Report generation failed: {str(e)}")

@app.post("/analyze/auto-report-html")
async def generate_auto_report_html(file: UploadFile):
    """
    Generate comprehensive AI-powered data understanding report as HTML
    """
    try:
        # Validate file type
        if not file.filename.lower().endswith('.csv'):
            raise HTTPException(status_code=400, detail=f"Only CSV files are supported for AI analysis. Received: {file.filename}")
        
        # Read and process the CSV file
        content = await file.read()
        df, detected_headers = load_csv_with_header_detection(content)
        
        if df.empty:
            raise HTTPException(status_code=400, detail="The uploaded file is empty or invalid")
        
        # Initialize AI Data Analyzer
        analyzer = AIDataAnalyzer()
        
        # Generate comprehensive report
        report = analyzer.generate_comprehensive_report(df, file.filename)
        
        # Add header detection info
        report["header_detection"] = {
            "detected_headers": detected_headers,
            "header_quality": "Good" if detected_headers else "Auto-generated"
        }
        
        # Format as HTML
        html_report = analyzer.format_report_as_html(report)
        
        # Save HTML report
        report_filename = f"ai_report_{file.filename.replace('.csv', '')}.html"
        report_path = os.path.join("../data/reports", report_filename)
        
        # Create reports directory if it doesn't exist
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(html_report)
        
        return JSONResponse(content={
            "message": "AI Data Understanding Report (HTML) generated successfully",
            "report_path": report_path,
            "filename": report_filename,
            "html_content": html_report
        })
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"HTML report generation failed: {str(e)}")



@app.post("/convert/json-to-csv-for-analysis")
async def convert_json_to_csv_for_analysis(file: UploadFile):
    """
    Convert JSON file to CSV format for AI analysis
    """
    try:
        # Validate file type
        if not file.filename.lower().endswith('.json'):
            raise HTTPException(status_code=400, detail=f"Only JSON files are supported for this conversion. Received: {file.filename}")
        
        # Read JSON content
        content = await file.read()
        
        try:
            json_data = json.loads(content.decode('utf-8'))
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=400, detail=f"Invalid JSON format: {str(e)}")
        
        # Convert JSON to DataFrame
        if isinstance(json_data, list):
            df = pd.DataFrame(json_data)
        elif isinstance(json_data, dict):
            # If it's a single object, wrap in a list
            df = pd.DataFrame([json_data])
        else:
            raise HTTPException(status_code=400, detail="JSON must be an array of objects or a single object")
        
        if df.empty:
            raise HTTPException(status_code=400, detail="No data found in JSON file")
        
        # Convert to CSV
        csv_content = df.to_csv(index=False)
        
        # Save converted CSV
        csv_filename = file.filename.replace('.json', '.csv')
        csv_path = os.path.join("../data/converted", csv_filename)
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        
        with open(csv_path, 'w', encoding='utf-8') as f:
            f.write(csv_content)
        
        return JSONResponse(content={
            "message": "JSON successfully converted to CSV for AI analysis",
            "original_file": file.filename,
            "csv_file": csv_filename,
            "csv_path": csv_path,
            "rows": len(df),
            "columns": len(df.columns),
            "note": "You can now upload the CSV file for AI data analysis"
        })
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"JSON to CSV conversion failed: {str(e)}")

@app.get("/logs/test")
async def test_logs_endpoint():
    """Test endpoint to verify logs API is accessible"""
    return {"status": "ok", "message": "Logs API is working"}



@app.post("/ai-report/csv")
async def generate_ai_csv_report(file: UploadFile):
    """
    Generate AI-powered data report for CSV files
    Analyzes structure, patterns, quality, and provides insights
    """
    try:
        print(f"🚀 Generating AI report for CSV: {file.filename}")
        
        # Validate file type
        if not file.filename.lower().endswith('.csv'):
            raise HTTPException(status_code=400, detail="Please upload a CSV file")
        
        # Read CSV content
        content = await file.read()
        
        # Create AI Data Report Generator
        from ai_data_report_generator import AIDataReportGenerator
        report_generator = AIDataReportGenerator()
        
        # Generate comprehensive report
        report = report_generator.generate_csv_report(content, file.filename)
        
        # Clean report for JSON serialization
        clean_report = clean_for_json(report)
        
        return JSONResponse(content={
            "message": "AI CSV report generated successfully",
            "file_name": file.filename,
            "report_type": "csv_analysis",
            "report": clean_report
        })
        
    except Exception as e:
        import traceback
        print(f"❌ AI CSV report error: {str(e)}")
        print(f"📍 Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"AI report generation failed: {str(e)}")

@app.post("/ai-report/json")
async def generate_ai_json_report(file: UploadFile):
    """
    Generate AI-powered data report for JSON files
    Analyzes structure, schema, patterns, and provides insights
    """
    try:
        print(f"🚀 Generating AI report for JSON: {file.filename}")
        
        # Validate file type
        if not file.filename.lower().endswith('.json'):
            raise HTTPException(status_code=400, detail="Please upload a JSON file")
        
        # Read JSON content
        content = await file.read()
        
        # Create AI Data Report Generator
        from ai_data_report_generator import AIDataReportGenerator
        report_generator = AIDataReportGenerator()
        
        # Generate comprehensive report
        report = report_generator.generate_json_report(content, file.filename)
        
        # Clean report for JSON serialization
        clean_report = clean_for_json(report)
        
        return JSONResponse(content={
            "message": "AI JSON report generated successfully",
            "file_name": file.filename,
            "report_type": "json_analysis", 
            "report": clean_report
        })
        
    except Exception as e:
        import traceback
        print(f"❌ AI JSON report error: {str(e)}")
        print(f"📍 Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"AI report generation failed: {str(e)}")



@app.post("/logs/process-raw")
async def process_raw_logs(file: UploadFile):
    """
    Process raw log text through complete AI pipeline:
    Raw Text → Parse → AI Label → Anomaly Detection → Insights → CSV Export
    """
    print(f"🚀 Starting log processing for: {file.filename}")
    try:
        # Validate file (accept .log, .txt files)
        allowed_extensions = ['.log', '.txt']
        file_ext = '.' + file.filename.lower().split('.')[-1] if '.' in file.filename else ''
        
        if file_ext not in allowed_extensions:
            raise HTTPException(
                status_code=400, 
                detail=f"Unsupported file type. Please upload .log or .txt files. Received: {file.filename}"
            )
        
        # Read log content
        content = await file.read()
        raw_log_text = content.decode('utf-8')
        
        if not raw_log_text.strip():
            raise HTTPException(status_code=400, detail="Log file is empty")
        
        print(f"📊 Log file size: {len(raw_log_text)} characters")
        print(f"📝 First 200 chars: {raw_log_text[:200]}")
        
        # Initialize AI Log Processor
        print("🤖 Initializing AI Log Processor...")
        processor = AILogProcessor()
        
        # Process logs through complete pipeline
        print("🔄 Starting log processing pipeline...")
        processed_df, summary, csv_path = processor.process_logs_to_csv(
            raw_log_text, 
            f"processed_{file.filename.replace(file_ext, '')}.csv"
        )
        
        print(f"✅ Processing complete. Processed {len(processed_df)} log entries")
        
        # Clean DataFrame for JSON serialization (handle NaN values)
        processed_df_clean = processed_df.copy()
        
        # Replace NaN values with None for JSON compatibility
        processed_df_clean = processed_df_clean.where(pd.notna(processed_df_clean), None)
        
        # Convert DataFrame to JSON for response (first 50 rows)
        processed_data = processed_df_clean.head(50).to_dict('records')
        
        # Clean summary data for JSON serialization
        summary_clean = clean_for_json(summary)
        
        response_data = {
            "message": "Log processing completed successfully",
            "original_file": file.filename,
            "processed_csv": csv_path,
            "summary": summary_clean,
            "processed_data": processed_data,
            "total_logs": len(processed_df),
            "anomalies_detected": summary_clean.get('anomaly_summary', {}).get('total_anomalies', 0),
            "parsing_success_rate": summary_clean.get('parsing_summary', {}).get('parsing_success_rate', 0),
            "ai_insights": summary_clean.get('ai_insights', ''),
            "recommendations": summary_clean.get('recommendations', [])
        }
        
        return JSONResponse(content=response_data)
        
    except Exception as e:
        import traceback
        print(f"❌ Log processing error: {str(e)}")
        print(f"📍 Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Log processing failed: {str(e)}")

@app.post("/logs/process-text")
async def process_log_text(request_data: dict):
    """
    Process raw log text (from textarea/direct input) through AI pipeline
    """
    try:
        raw_log_text = request_data.get('log_text', '')
        
        if not raw_log_text.strip():
            raise HTTPException(status_code=400, detail="No log text provided")
        
        # Initialize AI Log Processor
        processor = AILogProcessor()
        
        # Process logs through complete pipeline
        processed_df, summary, csv_path = processor.process_logs_to_csv(raw_log_text)
        
        # Convert DataFrame to JSON for response
        processed_data = processed_df.to_dict('records')
        
        return JSONResponse(content={
            "message": "Log text processing completed successfully",
            "processed_csv": csv_path,
            "summary": summary,
            "processed_data": processed_data[:50],  # Return first 50 rows in response
            "total_logs": len(processed_df),
            "anomalies_detected": summary.get('anomaly_summary', {}).get('total_anomalies', 0),
            "ai_insights": summary.get('ai_insights', ''),
            "recommendations": summary.get('recommendations', [])
        })
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Log text processing failed: {str(e)}")

@app.get("/logs/sample-processing")
async def sample_log_processing():
    """
    Demonstrate log processing with sample log data
    """
    try:
        # Sample log data with various formats
        sample_logs = '''
2024-01-15 10:30:15,123 INFO  com.example.UserService - User john.doe logged in successfully
2024-01-15 10:30:16,456 WARN  com.example.SecurityService - Failed login attempt from IP 192.168.1.100
2024-01-15 10:30:17,789 ERROR com.example.DatabaseService - Connection timeout to database server
192.168.1.50 - - [15/Jan/2024:10:30:18 +0000] "GET /api/users HTTP/1.1" 200 1234
2024-01-15 10:30:19,012 INFO  com.example.OrderService - Order #ORD12345 created for user john.doe
2024-01-15 10:30:20,345 ERROR com.example.PaymentService - Payment processing failed for order #ORD12345
2024-01-15 10:30:21,678 FATAL com.example.SystemService - Critical system failure detected
192.168.1.75 - - [15/Jan/2024:10:30:22 +0000] "POST /api/login HTTP/1.1" 401 0
2024-01-15 10:30:23,901 INFO  com.example.NotificationService - Email notification sent to user@example.com
2024-01-15 10:30:24,234 DEBUG com.example.CacheService - Cache cleared for key: user_sessions
Jan 15 10:30:25 server01 kernel: Out of memory: Kill process 1234 (java) score 900 or sacrifice child
2024-01-15 10:30:26,567 ERROR com.example.FileService - Unable to access file /var/log/application.log
        '''
        
        # Process sample logs
        processor = AILogProcessor()
        processed_df, summary, csv_path = processor.process_logs_to_csv(sample_logs, "sample_log_processing.csv")
        
        # Convert DataFrame to JSON for response (first 50 rows)
        processed_data = processed_df.to_dict('records')[:50]
        
        return JSONResponse(content={
            "message": "Sample log processing demonstration",
            "processed_csv": csv_path,
            "summary": summary,
            "processed_data": processed_data,
            "total_logs": len(processed_df),
            "anomalies_detected": summary.get('anomaly_summary', {}).get('total_anomalies', 0),
            "ai_insights": summary.get('ai_insights', ''),
            "recommendations": summary.get('recommendations', []),
            "note": "This demonstrates the complete log processing pipeline with sample data"
        })
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Sample log processing failed: {str(e)}")

@app.get("/logs/download-csv/{filename}")
async def download_processed_csv(filename: str):
    """
    Download processed log CSV file
    """
    try:
        csv_path = os.path.join("../data/processed", filename)
        
        if not os.path.exists(csv_path):
            raise HTTPException(status_code=404, detail="CSV file not found")
        
        return FileResponse(
            path=csv_path,
            filename=filename,
            media_type='text/csv'
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"File download failed: {str(e)}")

# ===== LOG COLUMN SELECTION ENDPOINTS =====

@app.post("/logs/get-columns")
async def get_log_columns(file: UploadFile = None, log_text: str = None):
    """
    🔍 Get available columns from processed logs for user selection
    """
    try:
        if not file and not log_text:
            raise HTTPException(status_code=400, detail="Either file or log_text must be provided")
        
        # Get log content
        if file:
            content = await file.read()
            raw_log_text = content.decode('utf-8')
        else:
            raw_log_text = log_text
        
        if not raw_log_text.strip():
            raise HTTPException(status_code=400, detail="No log content provided")
        
        # Process logs to get structure
        processor = AILogProcessor()
        df, insights, csv_filename = processor.process_logs_to_csv(raw_log_text)
        
        if df.empty:
            raise HTTPException(status_code=400, detail="No structured data could be extracted from logs")
        
        # Get available columns
        column_info = processor.get_available_columns(df)
        
        result = {
            "success": True,
            "columns": column_info,
            "sample_csv_filename": csv_filename,
            "processing_insights": insights
        }
        
        return JSONResponse(content=clean_for_json(result))
        
    except Exception as e:
        print(f"❌ Error getting log columns: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to get log columns: {str(e)}")

@app.post("/logs/create-filtered-csv")
async def create_filtered_log_csv(request_data: dict):
    """
    📊 Create filtered CSV with selected columns only
    """
    try:
        # Extract request data
        selected_columns = request_data.get('selected_columns', [])
        log_text = request_data.get('log_text', '')
        original_filename = request_data.get('original_filename', '')
        
        if not selected_columns:
            raise HTTPException(status_code=400, detail="No columns selected")
        
        if not log_text:
            raise HTTPException(status_code=400, detail="No log text provided")
        
        # Process logs again (in a real app, this would be cached)
        processor = AILogProcessor()
        df, insights, _ = processor.process_logs_to_csv(log_text)
        
        if df.empty:
            raise HTTPException(status_code=400, detail="No data to filter")
        
        # Create filtered CSV
        base_name = original_filename.replace('.log', '').replace('.txt', '') if original_filename else 'logs'
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filtered_filename = f"{base_name}_filtered_{timestamp}.csv"
        
        output_filename = processor.create_filtered_csv(df, selected_columns, filtered_filename)
        
        # Get summary of filtered data
        filtered_df = df[selected_columns]
        
        result = {
            "success": True,
            "filename": output_filename,
            "total_rows": len(filtered_df),
            "selected_columns": selected_columns,
            "filtered_data_preview": filtered_df.head(5).to_dict('records') if len(filtered_df) > 0 else [],
            "file_size_estimate": f"{len(filtered_df) * len(selected_columns) * 20} bytes"
        }
        
        return JSONResponse(content=clean_for_json(result))
        
    except Exception as e:
        print(f"❌ Error creating filtered CSV: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to create filtered CSV: {str(e)}")

@app.get("/logs/download-filtered/{filename}")
async def download_filtered_log_csv(filename: str):
    """
    💾 Download filtered log CSV file
    """
    try:
        csv_path = os.path.join("../data/cleaned", filename)
        
        if not os.path.exists(csv_path):
            raise HTTPException(status_code=404, detail="Filtered CSV file not found")
        
        return FileResponse(
            path=csv_path,
            filename=filename,
            media_type='text/csv'
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Filtered file download failed: {str(e)}")

# ===== PDF TO DATASET PROCESSING ENDPOINTS =====

@app.post("/pdf/convert-to-csv")
async def convert_pdf_to_csv(file: UploadFile):
    """
    🧠 Intelligent PDF to CSV conversion using AI analysis
    Enhanced workflow: PDF → AI Document Analysis → Smart Data Extraction → Structured Dataset
    """
    
    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Only PDF files are supported")
    
    try:
        print(f"🔄 Starting intelligent PDF to CSV conversion: {file.filename}")
        
        # Read PDF content
        pdf_content = await file.read()
        
        # Initialize enhanced PDF processor with LM Studio connection
        processor = PDFToDatasetProcessor(lm_studio_url="http://localhost:1234")
        
        # Process PDF with AI intelligence
        result = processor.process_pdf_to_dataset(
            pdf_content=pdf_content,
            filename=file.filename,
            output_format="csv"
        )
        
        if not result["success"]:
            raise HTTPException(status_code=500, detail=result.get("error", "PDF processing failed"))
        
        print(f"✅ Successfully processed PDF: {result['dataset_info']['rows']} rows, {result['dataset_info']['columns']} columns")
        
        return JSONResponse(content={
            "success": True,
            "message": f"Successfully converted PDF to CSV dataset with {result['dataset_info']['rows']} rows",
            "document_analysis": result["document_analysis"],
            "dataset_info": result["dataset_info"],
            "data_quality": result["summary"]["data_quality"],
            "column_insights": result["summary"].get("column_insights", {}),
            "sample_data": result["sample_data"],
            "download_info": {
                "filename": result["dataset_info"]["file_path"].split('/')[-1],
                "format": "CSV",
                "ready_for_download": True
            }
        })
        
    except Exception as e:
        print(f"❌ PDF to CSV conversion error: {e}")
        raise HTTPException(status_code=500, detail=f"PDF processing failed: {str(e)}")

@app.post("/pdf/convert-to-json")
async def convert_pdf_to_json(file: UploadFile):
    """
    🧠 Intelligent PDF to JSON conversion using AI analysis
    Enhanced workflow: PDF → AI Document Analysis → Smart Data Extraction → JSON Dataset
    """
    
    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Only PDF files are supported")
    
    try:
        print(f"🔄 Starting intelligent PDF to JSON conversion: {file.filename}")
        
        # Read PDF content
        pdf_content = await file.read()
        
        # Initialize enhanced PDF processor with LM Studio connection
        processor = PDFToDatasetProcessor(lm_studio_url="http://localhost:1234")
        
        # Process PDF with AI intelligence
        result = processor.process_pdf_to_dataset(
            pdf_content=pdf_content,
            filename=file.filename,
            output_format="json"
        )
        
        if not result["success"]:
            raise HTTPException(status_code=500, detail=result.get("error", "PDF processing failed"))
        
        print(f"✅ Successfully processed PDF: {result['dataset_info']['rows']} rows, {result['dataset_info']['columns']} columns")
        
        return JSONResponse(content={
            "success": True,
            "message": f"Successfully converted PDF to JSON dataset with {result['dataset_info']['rows']} rows",
            "document_analysis": result["document_analysis"],
            "dataset_info": result["dataset_info"],
            "data_quality": result["summary"]["data_quality"],
            "column_insights": result["summary"].get("column_insights", {}),
            "sample_data": result["sample_data"],
            "download_info": {
                "filename": result["dataset_info"]["file_path"].split('/')[-1],
                "format": "JSON",
                "ready_for_download": True
            }
        })
        
    except Exception as e:
        print(f"❌ PDF to JSON conversion error: {e}")
        raise HTTPException(status_code=500, detail=f"PDF processing failed: {str(e)}")
        result = processor.process_pdf_to_dataset(
            pdf_content=pdf_content,
            filename=file.filename,
            output_format="json"
        )
        
        if not result["success"]:
            raise HTTPException(status_code=500, detail=result.get("error", "PDF processing failed"))
        
        # Save the output to file system for download
        if result.get("output") and result["output"].get("content"):
            output_filename = result["output"]["filename"]
            output_content = result["output"]["content"]
            
            # Save to cleaned directory
            os.makedirs("../data/cleaned", exist_ok=True)
            file_path = os.path.join("../data/cleaned", output_filename)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(output_content)
            
            print(f"✅ Saved PDF JSON dataset to: {file_path}")
        
        # Clean the result for JSON serialization
        cleaned_result = clean_for_json(result)
        
        return JSONResponse(content=cleaned_result)
        
    except Exception as e:
        print(f"❌ PDF processing error: {e}")
        raise HTTPException(status_code=500, detail=f"PDF processing failed: {str(e)}")

@app.get("/pdf/download/{filename}")
async def download_pdf_dataset(filename: str):
    """
    💾 Download processed PDF dataset
    """
    try:
        # Check both data directories
        cleaned_path = os.path.join("../data/cleaned", filename)
        labeled_path = os.path.join("../data/labeled", filename)
        
        file_path = None
        if os.path.exists(cleaned_path):
            file_path = cleaned_path
        elif os.path.exists(labeled_path):
            file_path = labeled_path
        
        if not file_path:
            raise HTTPException(status_code=404, detail="Dataset file not found")
        
        # Determine media type based on extension
        if filename.endswith('.csv'):
            media_type = 'text/csv'
        elif filename.endswith('.json'):
            media_type = 'application/json'
        else:
            media_type = 'application/octet-stream'
        
        return FileResponse(
            path=file_path,
            filename=filename,
            media_type=media_type
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"File download failed: {str(e)}")

@app.get("/pdf/health-check")
async def pdf_health_check():
    """
    🏥 Check if PDF processing dependencies are available
    """
    try:
        # Test if PDF libraries are available
        processor = PDFToDatasetProcessor()
        
        # Check if PDF libraries are installed
        try:
            import pdfplumber
            import fitz  # PyMuPDF
            pdf_libs_available = True
        except ImportError:
            pdf_libs_available = False
        
        return {
            "status": "healthy" if pdf_libs_available else "missing_dependencies",
            "pdf_libraries_available": pdf_libs_available,
            "required_packages": ["pdfplumber", "PyMuPDF"],
            "install_command": "pip install pdfplumber PyMuPDF",
            "features": [
                "📄 PDF text extraction",
                "🧹 Data cleaning & structuring", 
                "🧠 AI-powered labeling",
                "📊 DataFrame creation",
                "🧮 Analytics & anomaly detection",
                "💾 CSV/JSON output"
            ]
        }
        
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "pdf_libraries_available": False
        }
