"""
AI Data Understanding and Auto Report Generation
Generates comprehensive, structured reports for CSV data using Mistral 7B
"""

import pandas as pd
import numpy as np
from datetime import datetime
import requests
import json
from typing import Dict, Any, List, Optional
import re

def convert_numpy_types(obj):
    """Convert numpy types to Python native types for JSON serialization"""
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {key: convert_numpy_types(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(item) for item in obj]
    elif pd.isna(obj):
        return None
    else:
        return obj

class AIDataAnalyzer:
    def __init__(self, lm_studio_url: str = "http://localhost:1234"):
        self.lm_studio_url = lm_studio_url
    
    def _call_ai(self, prompt: str) -> str:
        """Call Mistral 7B via LM Studio for AI analysis"""
        try:
            response = requests.post(
                f"{self.lm_studio_url}/v1/chat/completions",
                json={
                    "model": "mistral-7b-instruct-v0.1",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.3,
                    "max_tokens": 1500
                },
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()['choices'][0]['message']['content'].strip()
            else:
                return "AI analysis temporarily unavailable"
        except Exception as e:
            return f"AI analysis error: {str(e)}"
    
    def analyze_data_types(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze data types and characteristics"""
        analysis = {
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "columns": {},
            "data_quality": {}
        }
        
        for col in df.columns:
            col_info = {
                "dtype": str(df[col].dtype),
                "non_null_count": df[col].notna().sum(),
                "null_count": df[col].isna().sum(),
                "null_percentage": round((df[col].isna().sum() / len(df)) * 100, 2),
                "unique_values": df[col].nunique(),
                "unique_percentage": round((df[col].nunique() / len(df)) * 100, 2)
            }
            
            # Add type-specific analysis
            if pd.api.types.is_numeric_dtype(df[col]):
                col_info.update({
                    "min": convert_numpy_types(df[col].min()),
                    "max": convert_numpy_types(df[col].max()),
                    "mean": convert_numpy_types(round(df[col].mean(), 2)) if df[col].notna().any() else None,
                    "median": convert_numpy_types(round(df[col].median(), 2)) if df[col].notna().any() else None,
                    "std": convert_numpy_types(round(df[col].std(), 2)) if df[col].notna().any() else None
                })
            elif pd.api.types.is_string_dtype(df[col]) or df[col].dtype == 'object':
                most_common = df[col].value_counts().head(3)
                col_info.update({
                    "most_common": convert_numpy_types(most_common.to_dict()) if not most_common.empty else {},
                    "avg_length": convert_numpy_types(round(df[col].astype(str).str.len().mean(), 2)) if df[col].notna().any() else None,
                    "max_length": convert_numpy_types(df[col].astype(str).str.len().max()) if df[col].notna().any() else None
                })
            
            analysis["columns"][col] = col_info
        
        # Overall data quality metrics
        total_cells = len(df) * len(df.columns)
        null_cells = df.isna().sum().sum()
        analysis["data_quality"] = {
            "completeness_percentage": convert_numpy_types(round(((total_cells - null_cells) / total_cells) * 100, 2)),
            "total_missing_values": convert_numpy_types(null_cells),
            "columns_with_missing": convert_numpy_types(len([col for col in df.columns if df[col].isna().any()])),
            "duplicate_rows": convert_numpy_types(len(df) - len(df.drop_duplicates()))
        }
        
        return analysis
    
    def detect_patterns_and_insights(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Detect patterns and generate insights"""
        insights = {
            "potential_categories": [],
            "numerical_distributions": [],
            "text_patterns": [],
            "relationships": []
        }
        
        # Detect categorical columns
        for col in df.columns:
            unique_ratio = df[col].nunique() / len(df)
            if unique_ratio < 0.1 and df[col].nunique() > 1:  # Less than 10% unique values
                insights["potential_categories"].append({
                    "column": col,
                    "unique_values": df[col].nunique(),
                    "top_values": df[col].value_counts().head(5).to_dict()
                })
        
        # Analyze numerical distributions
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if df[col].notna().any():
                skewness = df[col].skew()
                distribution_type = "normal" if abs(skewness) < 0.5 else ("right-skewed" if skewness > 0.5 else "left-skewed")
                insights["numerical_distributions"].append({
                    "column": col,
                    "distribution": distribution_type,
                    "skewness": round(skewness, 2),
                    "outliers": self._detect_outliers(df[col])
                })
        
        # Detect text patterns
        text_cols = df.select_dtypes(include=['object']).columns
        for col in text_cols:
            patterns = self._analyze_text_patterns(df[col])
            if patterns:
                insights["text_patterns"].append({
                    "column": col,
                    "patterns": patterns
                })
        
        return insights
    
    def _detect_outliers(self, series: pd.Series) -> int:
        """Detect outliers using IQR method"""
        Q1 = series.quantile(0.25)
        Q3 = series.quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        outliers = series[(series < lower_bound) | (series > upper_bound)]
        return len(outliers)
    
    def _analyze_text_patterns(self, series: pd.Series) -> List[str]:
        """Analyze text patterns in a column"""
        patterns = []
        sample_values = series.dropna().astype(str).head(100)
        
        if sample_values.empty:
            return patterns
        
        # Check for email patterns
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        if sample_values.str.contains(email_pattern, regex=True, na=False).any():
            patterns.append("email_addresses")
        
        # Check for phone patterns
        phone_pattern = r'(\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'
        if sample_values.str.contains(phone_pattern, regex=True, na=False).any():
            patterns.append("phone_numbers")
        
        # Check for URL patterns
        url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
        if sample_values.str.contains(url_pattern, regex=True, na=False).any():
            patterns.append("urls")
        
        # Check for date patterns
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
            r'\d{2}-\d{2}-\d{4}'   # MM-DD-YYYY
        ]
        for pattern in date_patterns:
            if sample_values.str.contains(pattern, regex=True, na=False).any():
                patterns.append("date_format")
                break
        
        return patterns
    
    def generate_ai_insights(self, df: pd.DataFrame, basic_analysis: Dict) -> str:
        """Generate AI-powered insights using Mistral 7B"""
        
        # Prepare data summary for AI
        summary = f"""
        Dataset Overview:
        - Total Records: {basic_analysis['total_rows']:,}
        - Total Features: {basic_analysis['total_columns']}
        - Data Completeness: {basic_analysis['data_quality']['completeness_percentage']}%
        - Missing Values: {basic_analysis['data_quality']['total_missing_values']:,}
        
        Column Information:
        """
        
        for col, info in list(basic_analysis['columns'].items())[:10]:  # Limit to first 10 columns
            summary += f"\n- {col}: {info['dtype']}, {info['unique_values']} unique values, {info['null_percentage']}% missing"
        
        prompt = f"""
        As a data science expert, analyze this dataset and provide professional insights:

        {summary}

        Please provide a structured analysis covering:
        1. Data Quality Assessment
        2. Key Findings and Patterns
        3. Potential Use Cases
        4. Recommendations for Data Improvement
        5. Business Insights (if applicable)

        Keep the response concise but comprehensive, focusing on actionable insights.
        """
        
        return self._call_ai(prompt)
    
    def generate_comprehensive_report(self, df: pd.DataFrame, filename: str = "dataset") -> Dict[str, Any]:
        """Generate a comprehensive data understanding report"""
        
        report = {
            "metadata": {
                "filename": filename,
                "generated_at": datetime.now().isoformat(),
                "report_version": "1.0"
            },
            "executive_summary": {},
            "basic_analysis": {},
            "pattern_insights": {},
            "ai_insights": "",
            "recommendations": []
        }
        
        # Basic statistical analysis
        basic_analysis = self.analyze_data_types(df)
        report["basic_analysis"] = basic_analysis
        
        # Pattern detection
        pattern_insights = self.detect_patterns_and_insights(df)
        report["pattern_insights"] = pattern_insights
        
        # Executive summary
        report["executive_summary"] = {
            "dataset_size": f"{basic_analysis['total_rows']:,} rows × {basic_analysis['total_columns']} columns",
            "data_quality_score": basic_analysis["data_quality"]["completeness_percentage"],
            "key_statistics": {
                "numerical_columns": len(df.select_dtypes(include=[np.number]).columns),
                "categorical_columns": len(pattern_insights["potential_categories"]),
                "text_columns": len(df.select_dtypes(include=['object']).columns),
                "missing_data_columns": basic_analysis["data_quality"]["columns_with_missing"]
            }
        }
        
        # Generate AI insights
        ai_insights = self.generate_ai_insights(df, basic_analysis)
        report["ai_insights"] = ai_insights
        
        # Generate recommendations
        recommendations = self._generate_recommendations(basic_analysis, pattern_insights)
        report["recommendations"] = recommendations
        
        # Convert all numpy types to Python native types for JSON serialization
        report = convert_numpy_types(report)
        
        return report
    
    def _generate_recommendations(self, basic_analysis: Dict, pattern_insights: Dict) -> List[str]:
        """Generate data improvement recommendations"""
        recommendations = []
        
        # Data quality recommendations
        completeness = basic_analysis["data_quality"]["completeness_percentage"]
        if completeness < 90:
            recommendations.append(f"⚠️ Data completeness is {completeness}%. Consider investigating missing data patterns.")
        
        # Missing data recommendations
        if basic_analysis["data_quality"]["columns_with_missing"] > 0:
            recommendations.append("🔍 Some columns have missing values. Consider imputation strategies or data collection improvements.")
        
        # Duplicate data
        if basic_analysis["data_quality"]["duplicate_rows"] > 0:
            recommendations.append(f"📋 Found {basic_analysis['data_quality']['duplicate_rows']} duplicate rows. Consider deduplication.")
        
        # High cardinality recommendations
        high_cardinality_cols = []
        for col, info in basic_analysis["columns"].items():
            if info["unique_percentage"] > 95 and info["unique_values"] > 100:
                high_cardinality_cols.append(col)
        
        if high_cardinality_cols:
            recommendations.append(f"🏷️ High cardinality columns detected: {', '.join(high_cardinality_cols)}. Consider grouping or encoding strategies.")
        
        # Categorical recommendations
        if len(pattern_insights["potential_categories"]) > 0:
            recommendations.append("📊 Categorical patterns detected. Consider one-hot encoding or label encoding for machine learning.")
        
        # Text pattern recommendations
        if len(pattern_insights["text_patterns"]) > 0:
            recommendations.append("📝 Structured text patterns found. Consider feature extraction or validation rules.")
        
        return recommendations

    def format_report_as_html(self, report: Dict) -> str:
        """Format the report as HTML for better presentation"""
        
        html_template = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>AI Data Understanding Report</title>
            <style>
                body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 20px; line-height: 1.6; }}
                .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 10px; margin-bottom: 20px; }}
                .section {{ background: white; padding: 20px; margin-bottom: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
                .metric {{ display: inline-block; background: #f8f9fa; padding: 10px 15px; margin: 5px; border-radius: 5px; border-left: 4px solid #007bff; }}
                .recommendation {{ background: #f8f9fa; padding: 10px; margin: 5px 0; border-radius: 5px; border-left: 4px solid #28a745; }}
                .column-info {{ background: #f8f9fa; padding: 10px; margin: 5px; border-radius: 5px; }}
                table {{ width: 100%; border-collapse: collapse; margin: 10px 0; }}
                th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
                th {{ background-color: #f2f2f2; }}
                .ai-insights {{ background: linear-gradient(135deg, #ffeaa7 0%, #fab1a0 100%); padding: 20px; border-radius: 10px; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>🤖 AI Data Understanding Report</h1>
                <p><strong>Dataset:</strong> {report['metadata']['filename']} | <strong>Generated:</strong> {report['metadata']['generated_at'][:19]}</p>
            </div>

            <div class="section">
                <h2>📊 Executive Summary</h2>
                <div class="metric"><strong>Dataset Size:</strong> {report['executive_summary']['dataset_size']}</div>
                <div class="metric"><strong>Data Quality:</strong> {report['executive_summary']['data_quality_score']}%</div>
                <div class="metric"><strong>Numerical Columns:</strong> {report['executive_summary']['key_statistics']['numerical_columns']}</div>
                <div class="metric"><strong>Categorical Columns:</strong> {report['executive_summary']['key_statistics']['categorical_columns']}</div>
                <div class="metric"><strong>Text Columns:</strong> {report['executive_summary']['key_statistics']['text_columns']}</div>
            </div>

            <div class="section">
                <h2>🔍 Data Quality Analysis</h2>
                <p><strong>Completeness:</strong> {report['basic_analysis']['data_quality']['completeness_percentage']}%</p>
                <p><strong>Missing Values:</strong> {report['basic_analysis']['data_quality']['total_missing_values']:,} total</p>
                <p><strong>Duplicate Rows:</strong> {report['basic_analysis']['data_quality']['duplicate_rows']}</p>
                <p><strong>Columns with Missing Data:</strong> {report['basic_analysis']['data_quality']['columns_with_missing']}</p>
            </div>

            <div class="section">
                <h2>📈 Column Analysis</h2>
                <table>
                    <tr>
                        <th>Column Name</th>
                        <th>Data Type</th>
                        <th>Unique Values</th>
                        <th>Missing %</th>
                        <th>Additional Info</th>
                    </tr>
        """
        
        # Add column information
        for col, info in report['basic_analysis']['columns'].items():
            additional_info = ""
            if 'mean' in info and info['mean'] is not None:
                additional_info = f"Mean: {info['mean']}"
            elif 'most_common' in info and info['most_common']:
                top_value = list(info['most_common'].keys())[0]
                additional_info = f"Top: {top_value}"
            
            html_template += f"""
                    <tr>
                        <td>{col}</td>
                        <td>{info['dtype']}</td>
                        <td>{info['unique_values']}</td>
                        <td>{info['null_percentage']}%</td>
                        <td>{additional_info}</td>
                    </tr>
            """
        
        html_template += """
                </table>
            </div>

            <div class="section ai-insights">
                <h2>🧠 AI-Powered Insights</h2>
                <div style="white-space: pre-line; background: rgba(255,255,255,0.9); padding: 15px; border-radius: 5px;">
        """ + report['ai_insights'] + """
                </div>
            </div>

            <div class="section">
                <h2>💡 Recommendations</h2>
        """
        
        for rec in report['recommendations']:
            html_template += f'<div class="recommendation">{rec}</div>'
        
        html_template += """
            </div>
        </body>
        </html>
        """
        
        return html_template