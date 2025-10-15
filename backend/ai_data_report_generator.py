"""
AI-Powered Data Report Generator
Generates comprehensive reports for CSV and JSON files with intelligent insights
"""

import pandas as pd
import numpy as np
import json
import io
from datetime import datetime
from typing import Dict, List, Any, Optional
from collections import Counter
import re

class AIDataReportGenerator:
    def __init__(self):
        self.timestamp = datetime.now()
    
    def generate_csv_report(self, csv_content: bytes, filename: str) -> Dict[str, Any]:
        """Generate comprehensive AI report for CSV data"""
        
        print(f"📊 Generating AI report for CSV: {filename}")
        
        try:
            # Read CSV data
            df = pd.read_csv(io.BytesIO(csv_content))
            
            report = {
                "file_info": self._analyze_file_info(df, filename, "CSV"),
                "data_structure": self._analyze_csv_structure(df),
                "data_quality": self._analyze_data_quality(df),
                "statistical_insights": self._generate_statistical_insights(df),
                "pattern_analysis": self._analyze_patterns(df),
                "ai_insights": self._generate_ai_insights(df, "csv"),
                "recommendations": self._generate_recommendations(df, "csv"),
                "summary": self._generate_executive_summary(df, "csv")
            }
            
            print(f"✅ AI CSV report generated: {len(df)} rows, {len(df.columns)} columns")
            return report
            
        except Exception as e:
            print(f"❌ Error generating CSV report: {e}")
            return {"error": f"Failed to analyze CSV: {str(e)}"}
    
    def generate_json_report(self, json_content: bytes, filename: str) -> Dict[str, Any]:
        """Generate comprehensive AI report for JSON data"""
        
        print(f"📊 Generating AI report for JSON: {filename}")
        
        try:
            # Parse JSON data
            json_str = json_content.decode('utf-8')
            data = json.loads(json_str)
            
            # Convert to DataFrame if possible for unified analysis
            df = self._json_to_dataframe(data)
            
            report = {
                "file_info": self._analyze_file_info(df, filename, "JSON"),
                "json_structure": self._analyze_json_structure(data),
                "data_structure": self._analyze_csv_structure(df) if df is not None else None,
                "data_quality": self._analyze_data_quality(df) if df is not None else None,
                "schema_analysis": self._analyze_json_schema(data),
                "pattern_analysis": self._analyze_json_patterns(data),
                "ai_insights": self._generate_ai_insights(df if df is not None else data, "json"),
                "recommendations": self._generate_recommendations(df if df is not None else data, "json"),
                "summary": self._generate_executive_summary(df if df is not None else data, "json")
            }
            
            print(f"✅ AI JSON report generated")
            return report
            
        except Exception as e:
            print(f"❌ Error generating JSON report: {e}")
            return {"error": f"Failed to analyze JSON: {str(e)}"}
    
    def _analyze_file_info(self, df: pd.DataFrame, filename: str, file_type: str) -> Dict[str, Any]:
        """Analyze basic file information"""
        return {
            "filename": filename,
            "file_type": file_type,
            "analysis_timestamp": self.timestamp.isoformat(),
            "total_rows": len(df) if df is not None else 0,
            "total_columns": len(df.columns) if df is not None else 0,
            "file_size_estimate": f"{(len(df) * len(df.columns) * 10) / 1024:.1f} KB" if df is not None else "Unknown",
            "data_density": self._calculate_data_density(df) if df is not None else 0
        }
    
    def _analyze_csv_structure(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze CSV data structure"""
        if df is None or df.empty:
            return {"error": "No data to analyze"}
        
        column_info = {}
        for col in df.columns:
            column_info[col] = {
                "data_type": str(df[col].dtype),
                "null_count": df[col].isnull().sum(),
                "null_percentage": (df[col].isnull().sum() / len(df)) * 100,
                "unique_values": df[col].nunique(),
                "sample_values": df[col].dropna().head(3).tolist() if not df[col].dropna().empty else []
            }
        
        return {
            "columns": column_info,
            "data_types_summary": df.dtypes.value_counts().to_dict(),
            "memory_usage": df.memory_usage(deep=True).sum(),
            "categorical_columns": df.select_dtypes(include=['object']).columns.tolist(),
            "numeric_columns": df.select_dtypes(include=[np.number]).columns.tolist(),
            "datetime_columns": df.select_dtypes(include=['datetime']).columns.tolist()
        }
    
    def _analyze_data_quality(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze data quality metrics"""
        if df is None or df.empty:
            return {"error": "No data to analyze"}
        
        total_cells = len(df) * len(df.columns)
        null_cells = df.isnull().sum().sum()
        
        # Detect potential data quality issues
        issues = []
        
        # Check for high null percentage
        for col in df.columns:
            null_pct = (df[col].isnull().sum() / len(df)) * 100
            if null_pct > 50:
                issues.append(f"Column '{col}' has {null_pct:.1f}% missing values")
        
        # Check for duplicate rows
        duplicate_count = df.duplicated().sum()
        if duplicate_count > 0:
            issues.append(f"{duplicate_count} duplicate rows found")
        
        # Check for potential outliers in numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            q1 = df[col].quantile(0.25)
            q3 = df[col].quantile(0.75)
            iqr = q3 - q1
            outliers = len(df[(df[col] < q1 - 1.5 * iqr) | (df[col] > q3 + 1.5 * iqr)])
            if outliers > len(df) * 0.1:  # More than 10% outliers
                issues.append(f"Column '{col}' has {outliers} potential outliers")
        
        return {
            "completeness_score": ((total_cells - null_cells) / total_cells) * 100,
            "null_percentage": (null_cells / total_cells) * 100,
            "duplicate_rows": duplicate_count,
            "data_quality_issues": issues,
            "quality_grade": self._calculate_quality_grade(df),
            "consistency_score": self._calculate_consistency_score(df)
        }
    
    def _generate_statistical_insights(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate statistical insights"""
        if df is None or df.empty:
            return {"error": "No data to analyze"}
        
        insights = {}
        
        # Numeric columns analysis
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            insights["numeric_summary"] = df[numeric_cols].describe().to_dict()
            
            # Correlation analysis
            if len(numeric_cols) > 1:
                corr_matrix = df[numeric_cols].corr()
                strong_correlations = []
                
                for i in range(len(corr_matrix.columns)):
                    for j in range(i+1, len(corr_matrix.columns)):
                        corr_val = corr_matrix.iloc[i, j]
                        if abs(corr_val) > 0.7:  # Strong correlation
                            strong_correlations.append({
                                "column1": corr_matrix.columns[i],
                                "column2": corr_matrix.columns[j],
                                "correlation": corr_val
                            })
                
                insights["correlations"] = strong_correlations
        
        # Categorical columns analysis
        categorical_cols = df.select_dtypes(include=['object']).columns
        if len(categorical_cols) > 0:
            categorical_insights = {}
            for col in categorical_cols:
                value_counts = df[col].value_counts().head(5)
                categorical_insights[col] = {
                    "unique_count": df[col].nunique(),
                    "most_frequent": value_counts.to_dict(),
                    "cardinality": "high" if df[col].nunique() > len(df) * 0.5 else "low"
                }
            insights["categorical_summary"] = categorical_insights
        
        return insights
    
    def _analyze_patterns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze data patterns"""
        if df is None or df.empty:
            return {"error": "No data to analyze"}
        
        patterns = {}
        
        # Time series patterns (if timestamp column exists)
        timestamp_cols = []
        for col in df.columns:
            if any(keyword in col.lower() for keyword in ['time', 'date', 'timestamp']):
                timestamp_cols.append(col)
        
        if timestamp_cols:
            patterns["temporal_patterns"] = self._analyze_temporal_patterns(df, timestamp_cols[0])
        
        # IP address patterns
        ip_cols = [col for col in df.columns if 'ip' in col.lower()]
        if ip_cols:
            patterns["network_patterns"] = self._analyze_network_patterns(df, ip_cols[0])
        
        # User agent patterns
        ua_cols = [col for col in df.columns if 'agent' in col.lower() or 'browser' in col.lower()]
        if ua_cols:
            patterns["user_agent_patterns"] = self._analyze_user_agent_patterns(df, ua_cols[0])
        
        return patterns
    
    def _analyze_json_structure(self, data: Any) -> Dict[str, Any]:
        """Analyze JSON structure"""
        def analyze_value(value, path="root"):
            if isinstance(value, dict):
                return {
                    "type": "object",
                    "keys": list(value.keys()),
                    "nested_structure": {k: analyze_value(v, f"{path}.{k}") for k, v in value.items()}
                }
            elif isinstance(value, list):
                if value:
                    sample_types = [type(item).__name__ for item in value[:5]]
                    return {
                        "type": "array",
                        "length": len(value),
                        "sample_item_types": sample_types,
                        "sample_structure": analyze_value(value[0], f"{path}[0]") if value else None
                    }
                else:
                    return {"type": "empty_array"}
            else:
                return {"type": type(value).__name__, "sample_value": str(value)[:100]}
        
        return analyze_value(data)
    
    def _analyze_json_schema(self, data: Any) -> Dict[str, Any]:
        """Analyze JSON schema patterns"""
        def extract_schema(obj, path=""):
            schema = {}
            
            if isinstance(obj, dict):
                for key, value in obj.items():
                    current_path = f"{path}.{key}" if path else key
                    schema[key] = {
                        "type": type(value).__name__,
                        "path": current_path,
                        "nullable": value is None
                    }
                    
                    if isinstance(value, (dict, list)) and value:
                        schema[key]["nested"] = extract_schema(value, current_path)
            
            elif isinstance(obj, list) and obj:
                # Analyze first item as schema representative
                schema = extract_schema(obj[0], path)
            
            return schema
        
        return {
            "schema": extract_schema(data),
            "depth": self._calculate_json_depth(data),
            "total_keys": self._count_json_keys(data)
        }
    
    def _json_to_dataframe(self, data: Any) -> Optional[pd.DataFrame]:
        """Convert JSON to DataFrame if possible"""
        try:
            if isinstance(data, list):
                # List of objects -> DataFrame
                if data and isinstance(data[0], dict):
                    return pd.DataFrame(data)
                # List of arrays -> DataFrame
                elif data and isinstance(data[0], list):
                    return pd.DataFrame(data)
            elif isinstance(data, dict):
                # Single object -> DataFrame with one row
                return pd.DataFrame([data])
            
            return None
        except Exception:
            return None
    
    def _generate_ai_insights(self, data: Any, data_type: str) -> List[str]:
        """Generate AI-powered insights"""
        insights = []
        
        if data_type == "csv" and isinstance(data, pd.DataFrame):
            df = data
            
            # Data volume insights
            if len(df) > 100000:
                insights.append(f"🎯 Large dataset detected ({len(df):,} rows). Consider using batch processing for analysis.")
            elif len(df) < 100:
                insights.append(f"📊 Small dataset ({len(df)} rows). Results may have limited statistical significance.")
            
            # Column analysis insights
            if len(df.columns) > 50:
                insights.append("🧩 High-dimensional dataset. Consider dimensionality reduction techniques.")
            
            # Data quality insights
            null_percentage = (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100
            if null_percentage > 20:
                insights.append(f"⚠️ High missing data rate ({null_percentage:.1f}%). Data imputation strategies recommended.")
            elif null_percentage < 5:
                insights.append(f"✅ Excellent data completeness ({100-null_percentage:.1f}% complete).")
            
            # Pattern insights
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 0:
                for col in numeric_cols:
                    if df[col].std() == 0:
                        insights.append(f"🔍 Column '{col}' has constant values. Consider removing.")
            
            # Business insights based on column names
            if any('revenue' in col.lower() or 'sales' in col.lower() for col in df.columns):
                insights.append("💰 Financial data detected. Consider trend analysis and forecasting.")
            
            if any('ip' in col.lower() for col in df.columns):
                insights.append("🌐 Network data detected. Consider geolocation and security analysis.")
            
            if any('time' in col.lower() or 'date' in col.lower() for col in df.columns):
                insights.append("⏰ Temporal data detected. Time series analysis recommended.")
        
        elif data_type == "json":
            # JSON-specific insights
            insights.append("📋 JSON structure analysis reveals flexible schema design.")
            
            if isinstance(data, list):
                insights.append(f"📊 Array-based JSON with {len(data)} items detected.")
            elif isinstance(data, dict):
                insights.append("🗂️ Object-based JSON structure detected.")
        
        return insights
    
    def _generate_recommendations(self, data: Any, data_type: str) -> List[str]:
        """Generate actionable recommendations"""
        recommendations = []
        
        if data_type == "csv" and isinstance(data, pd.DataFrame):
            df = data
            
            # Data quality recommendations
            quality_grade = self._calculate_quality_grade(df)
            if quality_grade < 70:
                recommendations.append("🔧 Implement data validation rules to improve data quality.")
            
            # Performance recommendations
            if len(df) > 10000:
                recommendations.append("⚡ Consider using chunked processing for better performance.")
            
            # Analysis recommendations
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) >= 2:
                recommendations.append("📈 Run correlation analysis to identify relationships between variables.")
            
            categorical_cols = df.select_dtypes(include=['object']).columns
            if len(categorical_cols) > 0:
                recommendations.append("🏷️ Consider encoding categorical variables for machine learning applications.")
            
            # Security recommendations
            if any('ip' in col.lower() for col in df.columns):
                recommendations.append("🔒 Implement IP address anonymization for privacy compliance.")
            
        elif data_type == "json":
            recommendations.append("🔄 Consider converting to tabular format for statistical analysis.")
            recommendations.append("📝 Document JSON schema for better data governance.")
        
        return recommendations
    
    def _generate_executive_summary(self, data: Any, data_type: str) -> Dict[str, Any]:
        """Generate executive summary"""
        if data_type == "csv" and isinstance(data, pd.DataFrame):
            df = data
            
            return {
                "data_overview": f"Dataset contains {len(df):,} records across {len(df.columns)} fields",
                "data_quality_score": f"{self._calculate_quality_grade(df):.1f}%",
                "key_characteristics": [
                    f"{len(df.select_dtypes(include=[np.number]).columns)} numeric fields",
                    f"{len(df.select_dtypes(include=['object']).columns)} categorical fields",
                    f"{(df.isnull().sum().sum() / (len(df) * len(df.columns)) * 100):.1f}% missing data"
                ],
                "business_value": "High" if self._calculate_quality_grade(df) > 80 else "Medium",
                "next_steps": [
                    "Perform exploratory data analysis",
                    "Address data quality issues",
                    "Implement visualization dashboard"
                ]
            }
        else:
            return {
                "data_overview": f"JSON structure with complex nested schema",
                "key_characteristics": ["Flexible schema design", "Rich data structure"],
                "business_value": "Medium",
                "next_steps": ["Convert to tabular format", "Implement schema validation"]
            }
    
    # Helper methods
    def _calculate_data_density(self, df: pd.DataFrame) -> float:
        """Calculate data density (non-null percentage)"""
        if df is None or df.empty:
            return 0
        total_cells = len(df) * len(df.columns)
        non_null_cells = total_cells - df.isnull().sum().sum()
        return (non_null_cells / total_cells) * 100
    
    def _calculate_quality_grade(self, df: pd.DataFrame) -> float:
        """Calculate overall data quality grade"""
        if df is None or df.empty:
            return 0
        
        # Factors: completeness, consistency, uniqueness
        completeness = self._calculate_data_density(df)
        
        # Consistency (no mixed types in object columns)
        consistency = 100  # Simplified for now
        
        # Uniqueness (low duplicate rate)
        duplicate_rate = (df.duplicated().sum() / len(df)) * 100
        uniqueness = max(0, 100 - duplicate_rate)
        
        return (completeness + consistency + uniqueness) / 3
    
    def _calculate_consistency_score(self, df: pd.DataFrame) -> float:
        """Calculate data consistency score"""
        # Simplified consistency check
        return 95.0  # Placeholder
    
    def _analyze_temporal_patterns(self, df: pd.DataFrame, time_col: str) -> Dict[str, Any]:
        """Analyze temporal patterns in data"""
        try:
            # Convert to datetime if possible
            df[time_col] = pd.to_datetime(df[time_col], errors='coerce')
            
            if df[time_col].isnull().all():
                return {"error": "Could not parse timestamps"}
            
            time_data = df[time_col].dropna()
            
            return {
                "time_range": {
                    "start": time_data.min().isoformat() if not time_data.empty else None,
                    "end": time_data.max().isoformat() if not time_data.empty else None,
                    "duration": str(time_data.max() - time_data.min()) if len(time_data) > 1 else None
                },
                "frequency_analysis": {
                    "records_per_hour": len(time_data) / max(1, (time_data.max() - time_data.min()).total_seconds() / 3600),
                    "busiest_hour": time_data.dt.hour.mode().iloc[0] if not time_data.empty else None
                }
            }
        except Exception as e:
            return {"error": f"Temporal analysis failed: {str(e)}"}
    
    def _analyze_network_patterns(self, df: pd.DataFrame, ip_col: str) -> Dict[str, Any]:
        """Analyze network/IP patterns"""
        try:
            ip_data = df[ip_col].dropna()
            
            # Basic IP analysis
            unique_ips = ip_data.nunique()
            
            # IP type classification
            private_ips = ip_data[ip_data.str.contains(r'^(192\.168\.|10\.|172\.)', na=False, regex=True)].count()
            
            return {
                "unique_ips": unique_ips,
                "total_requests": len(ip_data),
                "ip_diversity": unique_ips / len(ip_data) if len(ip_data) > 0 else 0,
                "private_ip_percentage": (private_ips / len(ip_data)) * 100 if len(ip_data) > 0 else 0,
                "top_ips": ip_data.value_counts().head(5).to_dict()
            }
        except Exception as e:
            return {"error": f"Network analysis failed: {str(e)}"}
    
    def _analyze_user_agent_patterns(self, df: pd.DataFrame, ua_col: str) -> Dict[str, Any]:
        """Analyze user agent patterns"""
        try:
            ua_data = df[ua_col].dropna()
            
            # Browser detection
            browsers = {}
            for ua in ua_data:
                ua_str = str(ua).lower()
                if 'firefox' in ua_str:
                    browsers['firefox'] = browsers.get('firefox', 0) + 1
                elif 'chrome' in ua_str:
                    browsers['chrome'] = browsers.get('chrome', 0) + 1
                elif 'safari' in ua_str:
                    browsers['safari'] = browsers.get('safari', 0) + 1
                elif 'bot' in ua_str:
                    browsers['bot'] = browsers.get('bot', 0) + 1
                else:
                    browsers['other'] = browsers.get('other', 0) + 1
            
            return {
                "unique_user_agents": ua_data.nunique(),
                "browser_distribution": browsers,
                "bot_percentage": (browsers.get('bot', 0) / len(ua_data)) * 100 if len(ua_data) > 0 else 0
            }
        except Exception as e:
            return {"error": f"User agent analysis failed: {str(e)}"}
    
    def _calculate_json_depth(self, obj: Any, current_depth: int = 0) -> int:
        """Calculate maximum depth of JSON structure"""
        if isinstance(obj, dict):
            if not obj:
                return current_depth
            return max(self._calculate_json_depth(value, current_depth + 1) for value in obj.values())
        elif isinstance(obj, list):
            if not obj:
                return current_depth
            return max(self._calculate_json_depth(item, current_depth + 1) for item in obj)
        else:
            return current_depth
    
    def _count_json_keys(self, obj: Any) -> int:
        """Count total number of keys in JSON structure"""
        if isinstance(obj, dict):
            return len(obj) + sum(self._count_json_keys(value) for value in obj.values())
        elif isinstance(obj, list):
            return sum(self._count_json_keys(item) for item in obj)
        else:
            return 0
    
    def _analyze_json_patterns(self, data: Any) -> Dict[str, Any]:
        """Analyze patterns specific to JSON data"""
        patterns = {}
        
        # Array patterns
        if isinstance(data, list):
            patterns["array_analysis"] = {
                "length": len(data),
                "item_types": list(set(type(item).__name__ for item in data[:100])),  # Sample first 100
                "consistent_structure": self._check_json_array_consistency(data)
            }
        
        # Object patterns
        elif isinstance(data, dict):
            patterns["object_analysis"] = {
                "key_count": len(data),
                "nested_objects": sum(1 for v in data.values() if isinstance(v, dict)),
                "nested_arrays": sum(1 for v in data.values() if isinstance(v, list))
            }
        
        return patterns
    
    def _check_json_array_consistency(self, arr: List[Any]) -> bool:
        """Check if JSON array has consistent structure"""
        if not arr or len(arr) < 2:
            return True
        
        first_type = type(arr[0])
        
        # Check type consistency
        if not all(type(item) == first_type for item in arr[:10]):  # Sample first 10
            return False
        
        # If objects, check key consistency
        if isinstance(arr[0], dict):
            first_keys = set(arr[0].keys())
            return all(set(item.keys()) == first_keys for item in arr[1:6])  # Sample first 5
        
        return True