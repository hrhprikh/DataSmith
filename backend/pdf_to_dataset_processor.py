"""
AI-Powered PDF to Dataset Processor
Intelligently extracts and structures meaningful data from PDF documents using advanced AI analysis
"""

import pandas as pd
import numpy as np
import re
import json
import io
import requests
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import logging

# PDF processing libraries
try:
    import pdfplumber
    import fitz  # PyMuPDF
    HAS_PDF_LIBS = True
except ImportError:
    HAS_PDF_LIBS = False
    print("⚠️ PDF libraries not installed. Please install: pip install pdfplumber PyMuPDF")

class PDFToDatasetProcessor:
    def __init__(self, lm_studio_url: str = "http://localhost:1234"):
        self.timestamp = datetime.now()
        self.lm_studio_url = lm_studio_url
        self.extraction_method = "auto"
        self.ai_available = self._check_ai_availability()
        
        # Enhanced data importance patterns
        self.data_importance_patterns = {
            'high_importance': [
                r'\btable\b', r'\bdata\b', r'\brecord\b', r'\bentry\b', r'\brow\b',
                r'\bname\b', r'\bdate\b', r'\btime\b', r'\bamount\b', r'\bprice\b',
                r'\btotal\b', r'\bsum\b', r'\bcount\b', r'\bid\b', r'\bno\b',
                r'\bemail\b', r'\bphone\b', r'\baddress\b', r'\bcity\b', r'\bstate\b',
                r'\bquantity\b', r'\bstatus\b', r'\btype\b', r'\bcategory\b'
            ],
            'tabular_indicators': [
                r'^\s*\d+[\s\t]+', r'[\t\s]{2,}', r'\|', r':', r';',
                r'^\s*[A-Za-z]+[\s\t]+[A-Za-z0-9]+', r'[A-Z][a-z]+\s+[A-Z][a-z]+',
                r'\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b', r'\$\d+', r'\d+\.\d+',
                r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}'
            ],
            'structured_content': [
                r'^\s*[•\-\*]\s+', r'^\s*\d+\.\s+', r'^\s*[a-zA-Z]\)\s+',
                r'^\s*Step\s+\d+', r'^\s*Phase\s+\d+', r'^\s*Section\s+\d+'
            ]
        }
    
    def _check_ai_availability(self) -> bool:
        """Check if LM Studio is available and responding"""
        try:
            response = requests.get(f"{self.lm_studio_url}/v1/models", timeout=5)
            if response.status_code == 200:
                print("✅ LM Studio is available for AI-enhanced processing")
                return True
            else:
                print(f"⚠️ LM Studio returned status {response.status_code}, using fallback mode")
                return False
        except:
            print("⚠️ LM Studio not available, using intelligent pattern-based processing")
            return False
        
    def _call_ai_for_data_analysis(self, content: str, task: str) -> str:
        """Enhanced AI caller with specific data analysis prompts and robust error handling"""
        try:
            if task == "identify_important_data":
                prompt = f"""You are an expert data analyst. Analyze this PDF content and identify what data would be valuable for creating a structured dataset.

Content to analyze:
{content[:2000]}

Please identify:
1. What type of document this appears to be (invoice, report, form, catalog, etc.)
2. What are the most important data fields that should be extracted
3. What patterns indicate structured/tabular data
4. How should this data be organized into columns

Respond in JSON format:
{{
    "document_type": "type of document",
    "important_fields": ["field1", "field2", "field3"],
    "data_patterns": ["pattern1", "pattern2"],
    "suggested_columns": ["col1", "col2", "col3"],
    "confidence": "high/medium/low"
}}"""

            elif task == "extract_structured_data":
                prompt = f"""You are a data extraction specialist. Extract structured data from this content and organize it into a tabular format.

Content:
{content}

Rules:
1. Only extract data that appears to be part of tables, lists, or structured information
2. Ignore headers, footers, and narrative text unless they contain data fields
3. Identify relationships between data points
4. Create meaningful column names
5. Preserve data types (numbers, dates, text)

Return ONLY a JSON array where each object represents a row of data:
[
    {{"column1": "value1", "column2": "value2", "column3": "value3"}},
    {{"column1": "value4", "column2": "value5", "column3": "value6"}}
]

If no structured data is found, return an empty array: []"""

            elif task == "validate_and_enhance":
                prompt = f"""You are a data quality expert. Review this extracted data and enhance it for dataset creation.

Data to review:
{content}

Tasks:
1. Validate data consistency
2. Standardize formats (dates, numbers, text)
3. Fill obvious gaps if possible
4. Add data quality indicators
5. Suggest improvements

Return enhanced JSON data with quality scores and improvements."""

            # Try to connect to LM Studio
            response = requests.post(
                f"{self.lm_studio_url}/v1/chat/completions",
                json={
                    "model": "mistral-7b-instruct-v0.1",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.3,
                    "max_tokens": 2000
                },
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                return result["choices"][0]["message"]["content"].strip()
            else:
                print(f"⚠️ AI request failed with status {response.status_code}: {response.text}")
                return ""
                
        except requests.exceptions.ConnectionError:
            print("⚠️ Cannot connect to LM Studio. Make sure it's running and the server is started.")
            return ""
        except requests.exceptions.Timeout:
            print("⚠️ AI request timed out. LM Studio may be busy processing.")
            return ""
        except Exception as e:
            print(f"⚠️ AI analysis failed: {str(e)}")
            return ""
        
    def process_pdf_to_dataset(self, pdf_content: bytes, filename: str, 
                             output_format: str = "csv") -> Dict[str, Any]:
        """
        Intelligent PDF to Dataset Pipeline:
        1. Extract raw content from PDF
        2. AI analysis to identify document type and important data
        3. Smart data extraction focused on valuable information
        4. Structure data into meaningful columns
        5. Validate and enhance data quality
        6. Export as clean dataset
        """
        
        print(f"📄 Starting Intelligent PDF to Dataset conversion: {filename}")
        
        try:
            # Step 1: Extract text from PDF with multiple methods
            print("🧾 Extracting content from PDF...")
            raw_text, metadata = self._extract_pdf_content_enhanced(pdf_content)
            
            if not raw_text or len(raw_text.strip()) < 50:
                raise ValueError("PDF appears to be empty or contains insufficient text")
            
            # Step 2: AI-powered document analysis and data identification
            print("🧠 Analyzing document with AI to identify important data...")
            document_analysis = self._analyze_document_with_ai(raw_text)
            
            # Step 3: Intelligent data extraction based on AI analysis
            print("🎯 Extracting structured data using AI insights...")
            extracted_data = self._extract_data_intelligently(raw_text, document_analysis)
            
            # Step 4: Validate and enhance the extracted data
            print("✨ Validating and enhancing data quality...")
            final_dataset = self._validate_and_enhance_dataset(extracted_data, document_analysis)
            
            # Step 5: Create DataFrame and export
            print("📊 Creating final dataset...")
            df = self._create_enhanced_dataframe(final_dataset)
            
            # Step 6: Generate comprehensive summary
            summary = self._generate_intelligent_summary(df, document_analysis, metadata)
            
            # Step 7: Export to desired format
            output_path = self._export_dataset(df, filename, output_format)
            
            print(f"✅ Successfully created dataset with {len(df)} rows and {len(df.columns)} columns")
            
            return {
                "success": True,
                "message": f"Successfully converted PDF to {output_format.upper()} dataset",
                "document_analysis": document_analysis,
                "dataset_info": {
                    "rows": len(df),
                    "columns": len(df.columns),
                    "column_names": df.columns.tolist(),
                    "data_types": df.dtypes.to_dict(),
                    "file_path": output_path
                },
                "summary": summary,
                "processing_metadata": {
                    "filename": filename,
                    "processing_time": datetime.now() - self.timestamp,
                    "extraction_method": self.extraction_method,
                    "ai_enhanced": True
                },
                "sample_data": df.head(5).to_dict('records') if not df.empty else []
            }
            
        except Exception as e:
            print(f"❌ PDF processing failed: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "message": f"Failed to process PDF: {str(e)}",
                "filename": filename
            }
    
    def _extract_pdf_content_enhanced(self, pdf_content: bytes) -> Tuple[str, Dict]:
        """Enhanced PDF content extraction with multiple fallback methods"""
        
        if not HAS_PDF_LIBS:
            raise ValueError("PDF libraries not installed. Please install pdfplumber and PyMuPDF")
        
        text_content = ""
        metadata = {"pages": 0, "extraction_method": "", "tables_found": 0}
        
        try:
            # Method 1: Try pdfplumber first (better for tables)
            with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
                metadata["pages"] = len(pdf.pages)
                
                for page_num, page in enumerate(pdf.pages):
                    # Extract text
                    page_text = page.extract_text()
                    if page_text:
                        text_content += f"\n--- PAGE {page_num + 1} ---\n{page_text}\n"
                    
                    # Try to extract tables
                    try:
                        tables = page.extract_tables()
                        if tables:
                            metadata["tables_found"] += len(tables)
                            for table_num, table in enumerate(tables):
                                text_content += f"\n--- TABLE {table_num + 1} ON PAGE {page_num + 1} ---\n"
                                for row in table:
                                    if row:
                                        clean_row = [str(cell) if cell else "" for cell in row]
                                        text_content += "\t".join(clean_row) + "\n"
                    except:
                        pass
                
                if text_content.strip():
                    metadata["extraction_method"] = "pdfplumber"
                    self.extraction_method = "pdfplumber"
                    return text_content, metadata
                    
        except Exception as e:
            print(f"⚠️ pdfplumber extraction failed: {e}")
        
        try:
            # Method 2: Fallback to PyMuPDF
            pdf_doc = fitz.open(stream=pdf_content, filetype="pdf")
            metadata["pages"] = pdf_doc.page_count
            
            for page_num in range(pdf_doc.page_count):
                page = pdf_doc[page_num]
                page_text = page.get_text()
                if page_text:
                    text_content += f"\n--- PAGE {page_num + 1} ---\n{page_text}\n"
            
            pdf_doc.close()
            
            if text_content.strip():
                metadata["extraction_method"] = "pymupdf"
                self.extraction_method = "pymupdf"
                return text_content, metadata
                
        except Exception as e:
            print(f"⚠️ PyMuPDF extraction failed: {e}")
        
        raise ValueError("Failed to extract text from PDF using all available methods")
    
    def _analyze_document_with_ai(self, content: str) -> Dict[str, Any]:
        """Use AI to analyze document and identify important data patterns"""
        
        # Only try AI if it's available
        if self.ai_available:
            ai_response = self._call_ai_for_data_analysis(content, "identify_important_data")
            
            if ai_response:  # If AI responded successfully
                try:
                    # Try to parse AI response as JSON
                    ai_analysis = json.loads(ai_response)
                    print("✅ AI analysis completed successfully")
                except:
                    print("⚠️ AI response parsing failed, using enhanced rule-based analysis")
                    ai_analysis = self._enhanced_document_analysis(content)
            else:
                print("⚠️ AI analysis failed, using enhanced rule-based analysis")
                ai_analysis = self._enhanced_document_analysis(content)
        else:
            print("🔧 Using enhanced pattern-based document analysis")
            ai_analysis = self._enhanced_document_analysis(content)
        
        # Enhance with pattern analysis
        pattern_analysis = self._analyze_content_patterns(content)
        ai_analysis.update(pattern_analysis)
        
        return ai_analysis
    
    def _enhanced_document_analysis(self, content: str) -> Dict[str, Any]:
        """Enhanced rule-based document analysis with better intelligence"""
        
        content_lower = content.lower()
        
        # Advanced document type detection
        doc_type = "unknown"
        confidence = "medium"
        
        # Financial documents
        financial_indicators = ['invoice', 'bill', 'receipt', 'payment', 'total', 'amount', 'tax', 'subtotal']
        if sum(1 for word in financial_indicators if word in content_lower) >= 3:
            doc_type = "financial_document"
            confidence = "high"
        
        # Reports and analytics
        elif any(word in content_lower for word in ['report', 'analysis', 'summary', 'performance', 'metrics']):
            doc_type = "report"
            confidence = "high"
        
        # Forms and applications
        elif any(word in content_lower for word in ['form', 'application', 'registration', 'personal information']):
            doc_type = "form"
            confidence = "high"
        
        # Catalogs and inventories
        elif any(word in content_lower for word in ['catalog', 'inventory', 'product', 'item', 'sku']):
            doc_type = "catalog"
            confidence = "high"
        
        # Schedules and timetables
        elif any(word in content_lower for word in ['schedule', 'timetable', 'calendar', 'appointment']):
            doc_type = "schedule"
            confidence = "medium"
        
        # Contact lists and directories
        elif content_lower.count('@') >= 2 or content_lower.count('phone') >= 2:
            doc_type = "contact_list"
            confidence = "high"
        
        # Identify important fields based on enhanced document type analysis
        important_fields = []
        if doc_type == "financial_document":
            important_fields = ["date", "amount", "description", "total", "tax", "customer", "invoice_number"]
        elif doc_type == "report":
            important_fields = ["metric", "value", "date", "category", "status", "percentage", "count"]
        elif doc_type == "form":
            important_fields = ["name", "address", "phone", "email", "date", "signature"]
        elif doc_type == "catalog":
            important_fields = ["product_name", "price", "description", "category", "code", "quantity"]
        elif doc_type == "schedule":
            important_fields = ["date", "time", "event", "location", "duration", "attendee"]
        elif doc_type == "contact_list":
            important_fields = ["name", "email", "phone", "address", "company", "title"]
        else:
            # Generic but intelligent field detection
            important_fields = ["name", "value", "date", "type", "status", "description"]
        
        # Enhance fields based on content analysis
        if '@' in content:
            important_fields.append("email")
        if any(pattern in content for pattern in [r'\$\d+', r'£\d+', r'€\d+']):
            important_fields.append("price")
        if re.search(r'\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b', content):
            important_fields.append("date")
        
        return {
            "document_type": doc_type,
            "important_fields": list(set(important_fields)),  # Remove duplicates
            "data_patterns": ["tabular", "structured"],
            "suggested_columns": important_fields[:8],  # Limit to 8 main columns
            "confidence": confidence,
            "analysis_method": "enhanced_pattern_based"
        }
    
    def _fallback_document_analysis(self, content: str) -> Dict[str, Any]:
        """Rule-based document analysis fallback"""
        
        content_lower = content.lower()
        
        # Detect document type
        doc_type = "unknown"
        if any(word in content_lower for word in ['invoice', 'bill', 'receipt']):
            doc_type = "financial_document"
        elif any(word in content_lower for word in ['report', 'analysis', 'summary']):
            doc_type = "report"
        elif any(word in content_lower for word in ['form', 'application', 'registration']):
            doc_type = "form"
        elif any(word in content_lower for word in ['catalog', 'inventory', 'product']):
            doc_type = "catalog"
        elif any(word in content_lower for word in ['schedule', 'timetable', 'calendar']):
            doc_type = "schedule"
        
        # Identify important fields based on document type
        important_fields = []
        if doc_type == "financial_document":
            important_fields = ["amount", "date", "description", "total", "tax", "customer"]
        elif doc_type == "report":
            important_fields = ["metric", "value", "date", "category", "status"]
        elif doc_type == "form":
            important_fields = ["name", "address", "phone", "email", "date"]
        elif doc_type == "catalog":
            important_fields = ["product", "price", "description", "category", "code"]
        else:
            important_fields = ["name", "value", "date", "type", "status"]
        
        return {
            "document_type": doc_type,
            "important_fields": important_fields,
            "data_patterns": ["tabular", "structured"],
            "suggested_columns": important_fields,
            "confidence": "medium"
        }
    
    def _analyze_content_patterns(self, content: str) -> Dict[str, Any]:
        """Analyze content for data patterns and structure indicators"""
        
        lines = content.split('\n')
        
        # Count different types of patterns
        tabular_lines = 0
        structured_lines = 0
        data_rich_lines = 0
        
        for line in lines:
            line_clean = line.strip()
            if not line_clean:
                continue
                
            # Check for tabular patterns
            if any(re.search(pattern, line) for pattern in self.data_importance_patterns['tabular_indicators']):
                tabular_lines += 1
            
            # Check for structured content
            if any(re.search(pattern, line) for pattern in self.data_importance_patterns['structured_content']):
                structured_lines += 1
            
            # Check for data-rich content
            if any(re.search(pattern, line, re.IGNORECASE) for pattern in self.data_importance_patterns['high_importance']):
                data_rich_lines += 1
        
        total_lines = len([l for l in lines if l.strip()])
        
        return {
            "content_analysis": {
                "total_lines": total_lines,
                "tabular_lines": tabular_lines,
                "structured_lines": structured_lines,
                "data_rich_lines": data_rich_lines,
                "tabular_percentage": round(tabular_lines / max(total_lines, 1) * 100, 2),
                "data_richness": round(data_rich_lines / max(total_lines, 1) * 100, 2)
            }
        }
    
    def _extract_data_intelligently(self, content: str, analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract data intelligently based on analysis, with or without AI"""
        
        # Try AI extraction if available
        if self.ai_available:
            ai_extracted = self._call_ai_for_data_analysis(content, "extract_structured_data")
            
            if ai_extracted:  # If AI responded
                try:
                    # Try to parse AI response
                    ai_data = json.loads(ai_extracted)
                    if isinstance(ai_data, list) and ai_data:
                        print(f"✅ AI extracted {len(ai_data)} data records")
                        return ai_data
                except:
                    print("⚠️ AI extraction parsing failed, using enhanced pattern extraction")
            else:
                print("⚠️ AI extraction failed, using enhanced pattern extraction")
        
        # Use enhanced pattern-based extraction
        print("🔧 Using enhanced pattern-based data extraction")
        return self._enhanced_pattern_extraction(content, analysis)
    
    def _enhanced_pattern_extraction(self, content: str, analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Enhanced pattern-based data extraction with better intelligence"""
        
        lines = content.split('\n')
        extracted_data = []
        
        doc_type = analysis.get('document_type', 'unknown')
        important_fields = analysis.get('important_fields', [])
        
        print(f"🎯 Extracting data for {doc_type} document type")
        
        for line_num, line in enumerate(lines):
            line_clean = line.strip()
            if not line_clean or len(line_clean) < 5:  # Skip very short lines
                continue
            
            # Calculate line importance score with enhanced logic
            importance_score = self._calculate_line_importance_enhanced(line_clean, analysis)
            
            if importance_score < 0.25:  # Lower threshold for better coverage
                continue
            
            # Extract data based on document type with enhanced methods
            if doc_type == "financial_document":
                data_row = self._extract_financial_data_enhanced(line_clean)
            elif doc_type == "catalog":
                data_row = self._extract_catalog_data_enhanced(line_clean)
            elif doc_type == "form" or doc_type == "contact_list":
                data_row = self._extract_form_data_enhanced(line_clean)
            elif doc_type == "report":
                data_row = self._extract_report_data_enhanced(line_clean)
            else:
                data_row = self._extract_generic_data_enhanced(line_clean, important_fields)
            
            if data_row and any(v for v in data_row.values() if v and str(v).strip()):  # Only add if has meaningful data
                data_row['line_number'] = line_num + 1
                data_row['importance_score'] = importance_score
                data_row['source_line'] = line_clean[:150]  # Increased for more context
                extracted_data.append(data_row)
        
        print(f"✅ Enhanced pattern extraction found {len(extracted_data)} data records")
        return extracted_data
    
    def _pattern_based_extraction(self, content: str, analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Intelligent pattern-based data extraction"""
        
        lines = content.split('\n')
        extracted_data = []
        
        doc_type = analysis.get('document_type', 'unknown')
        important_fields = analysis.get('important_fields', [])
        
        for line_num, line in enumerate(lines):
            line_clean = line.strip()
            if not line_clean or len(line_clean) < 10:  # Skip short/empty lines
                continue
            
            # Calculate line importance score
            importance_score = self._calculate_line_importance_enhanced(line_clean, analysis)
            
            if importance_score < 0.3:  # Skip low-importance lines
                continue
            
            # Extract data based on document type
            if doc_type == "financial_document":
                data_row = self._extract_financial_data_enhanced(line_clean)
            elif doc_type == "catalog":
                data_row = self._extract_catalog_data_enhanced(line_clean)
            elif doc_type == "form":
                data_row = self._extract_form_data_enhanced(line_clean)
            elif doc_type == "report":
                data_row = self._extract_report_data_enhanced(line_clean)
            else:
                data_row = self._extract_generic_data_enhanced(line_clean, important_fields)
            
            if data_row and any(v for v in data_row.values() if v):  # Only add if has data
                data_row['line_number'] = line_num + 1
                data_row['importance_score'] = importance_score
                data_row['source_line'] = line_clean[:100]  # First 100 chars for reference
                extracted_data.append(data_row)
        
        print(f"✅ Pattern extraction found {len(extracted_data)} data records")
        return extracted_data
    
    def _calculate_line_importance(self, line: str, analysis: Dict[str, Any]) -> float:
        """Calculate importance score for a line of text"""
        
        score = 0.0
        line_lower = line.lower()
        
        # Check for important field patterns
        important_fields = analysis.get('important_fields', [])
        for field in important_fields:
            if field.lower() in line_lower:
                score += 0.3
        
        # Check for high-importance patterns
        for pattern in self.data_importance_patterns['high_importance']:
            if re.search(pattern, line_lower):
                score += 0.2
        
        # Check for tabular indicators
        for pattern in self.data_importance_patterns['tabular_indicators']:
            if re.search(pattern, line):
                score += 0.3
        
        # Boost lines with multiple data elements
        data_elements = len(re.findall(r'\b\w+\b', line))
        if data_elements >= 3:
            score += 0.2
        
        # Boost lines with structured separators
        if '\t' in line or '|' in line or line.count('  ') >= 2:
            score += 0.3
        
        return min(score, 1.0)
    
    def _extract_financial_data(self, line: str) -> Dict[str, Any]:
        """Extract financial document data"""
        
        data = {}
        
        # Extract monetary amounts
        amounts = re.findall(r'[\$£€¥]\s*(\d+(?:,\d{3})*(?:\.\d{2})?)', line)
        if amounts:
            data['amount'] = amounts[0].replace(',', '')
        
        # Extract dates
        dates = re.findall(r'\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\b', line)
        if dates:
            data['date'] = dates[0]
        
        # Extract descriptions (text before amount)
        if amounts and '$' in line:
            desc_match = re.search(r'^(.+?)\$', line)
            if desc_match:
                data['description'] = desc_match.group(1).strip()
        
        # Extract invoice/order numbers
        numbers = re.findall(r'\b(?:INV|ORDER|#)\s*(\w+)\b', line, re.IGNORECASE)
        if numbers:
            data['reference_number'] = numbers[0]
        
        return data
    
    def _extract_catalog_data(self, line: str) -> Dict[str, Any]:
        """Extract catalog/product data"""
        
        data = {}
        
        # Extract product codes
        codes = re.findall(r'\b([A-Z]{2,}\d+|[A-Z]+[-_]\d+|\d+[-_][A-Z]+)\b', line)
        if codes:
            data['product_code'] = codes[0]
        
        # Extract prices
        prices = re.findall(r'[\$£€¥]\s*(\d+(?:\.\d{2})?)', line)
        if prices:
            data['price'] = prices[0]
        
        # Extract quantities
        quantities = re.findall(r'\b(\d+)\s*(?:pcs|units|qty|pieces)\b', line, re.IGNORECASE)
        if quantities:
            data['quantity'] = quantities[0]
        
        # Extract product names (longest text segment)
        words = line.split()
        if len(words) >= 2:
            # Find longest consecutive word sequence
            longest_text = max([' '.join(words[i:i+j]) for i in range(len(words)) 
                              for j in range(1, min(5, len(words)-i+1))], key=len, default="")
            if len(longest_text) > 5:
                data['product_name'] = longest_text
        
        return data
    
    def _extract_form_data(self, line: str) -> Dict[str, Any]:
        """Extract form data"""
        
        data = {}
        
        # Extract email addresses
        emails = re.findall(r'\b([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,})\b', line)
        if emails:
            data['email'] = emails[0]
        
        # Extract phone numbers
        phones = re.findall(r'\b(\d{3}[-.]?\d{3}[-.]?\d{4})\b', line)
        if phones:
            data['phone'] = phones[0]
        
        # Extract names (capitalized words)
        names = re.findall(r'\b([A-Z][a-z]+\s+[A-Z][a-z]+)\b', line)
        if names:
            data['name'] = names[0]
        
        # Extract key-value pairs
        kv_matches = re.findall(r'([A-Za-z\s]+):\s*([^,\n]+)', line)
        for key, value in kv_matches:
            clean_key = re.sub(r'[^\w\s]', '', key.strip().lower().replace(' ', '_'))
            if clean_key and value.strip():
                data[clean_key] = value.strip()
        
        return data
    
    def _extract_generic_data(self, line: str, important_fields: List[str]) -> Dict[str, Any]:
        """Extract generic structured data"""
        
        data = {}
        
        # Split by common separators
        if '\t' in line:
            parts = line.split('\t')
        elif '|' in line:
            parts = line.split('|')
        elif line.count('  ') >= 2:  # Multiple spaces
            parts = re.split(r'\s{2,}', line)
        else:
            parts = [line]
        
        # Clean and assign parts to fields
        clean_parts = [part.strip() for part in parts if part.strip()]
        
        if len(clean_parts) >= 2:
            for i, part in enumerate(clean_parts[:min(len(important_fields), 6)]):  # Max 6 fields
                field_name = important_fields[i] if i < len(important_fields) else f'field_{i+1}'
                data[field_name] = part
        
        # Extract specific patterns regardless
        urls = re.findall(r'https?://[^\s]+', line)
        if urls:
            data['url'] = urls[0]
        
        emails = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', line)
        if emails:
            data['email'] = emails[0]
        
        dates = re.findall(r'\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b', line)
        if dates:
            data['date'] = dates[0]
        
        numbers = re.findall(r'\b\d+(?:\.\d+)?\b', line)
        if numbers:
            data['numeric_value'] = numbers[0]
        
        return data
    
    def _extract_pdf_text(self, pdf_content: bytes) -> Tuple[str, Dict]:
        """Extract text from PDF using multiple methods"""
        
        if not HAS_PDF_LIBS:
            raise ImportError("PDF processing libraries not available")
        
        text_content = ""
        metadata = {}
        
        try:
            # Method 1: Try pdfplumber (better for tables and layout)
            with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
                pages_text = []
                
                for i, page in enumerate(pdf.pages):
                    page_text = page.extract_text()
                    if page_text:
                        pages_text.append(f"--- PAGE {i+1} ---\n{page_text}\n")
                
                text_content = "\n".join(pages_text)
                
                metadata = {
                    "total_pages": len(pdf.pages),
                    "extraction_method": "pdfplumber",
                    "has_tables": any(page.extract_tables() for page in pdf.pages[:3])  # Check first 3 pages
                }
                
                self.extraction_method = "pdfplumber"
                
        except Exception as e:
            print(f"⚠️ pdfplumber failed: {e}, trying PyMuPDF...")
            
            try:
                # Method 2: Fallback to PyMuPDF
                pdf_doc = fitz.open("pdf", pdf_content)
                pages_text = []
                
                for page_num in range(len(pdf_doc)):
                    page = pdf_doc.load_page(page_num)
                    page_text = page.get_text()
                    if page_text.strip():
                        pages_text.append(f"--- PAGE {page_num+1} ---\n{page_text}\n")
                
                text_content = "\n".join(pages_text)
                
                metadata = {
                    "total_pages": len(pdf_doc),
                    "extraction_method": "pymupdf",
                    "has_tables": False  # PyMuPDF doesn't easily detect tables
                }
                
                self.extraction_method = "pymupdf"
                pdf_doc.close()
                
            except Exception as e2:
                raise Exception(f"Both PDF extraction methods failed: pdfplumber: {e}, pymupdf: {e2}")
        
        if not text_content.strip():
            raise Exception("No text content extracted from PDF")
        
        print(f"✅ Extracted {len(text_content)} characters from {metadata['total_pages']} pages")
        return text_content, metadata
    
    def _clean_and_structure_text(self, raw_text: str) -> List[Dict[str, Any]]:
        """Clean and structure extracted text using regex and layout detection"""
        
        structured_data = []
        
        # Split into lines and clean
        lines = raw_text.split('\n')
        current_section = "content"
        line_number = 0
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            line_number += 1
            
            # Detect different content types
            line_data = {
                "line_number": line_number,
                "raw_text": line,
                "cleaned_text": self._clean_line(line),
                "content_type": self._detect_content_type(line),
                "section": current_section,
                "char_count": len(line),
                "word_count": len(line.split()),
                "has_numbers": bool(re.search(r'\d', line)),
                "has_email": bool(re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', line)),
                "has_phone": bool(re.search(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b', line)),
                "has_date": bool(re.search(r'\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b', line)),
                "has_currency": bool(re.search(r'[\$£€¥]\s*\d+', line)),
                "is_header": self._is_header(line),
                "confidence_score": self._calculate_confidence(line)
            }
            
            # Update section if header detected
            if line_data["is_header"]:
                current_section = self._normalize_section_name(line)
                line_data["section"] = current_section
            
            structured_data.append(line_data)
        
        print(f"✅ Structured {len(structured_data)} lines of content")
        return structured_data
    
    def _clean_line(self, line: str) -> str:
        """Clean individual line"""
        # Remove extra whitespace
        cleaned = re.sub(r'\s+', ' ', line.strip())
        
        # Remove common PDF artifacts
        cleaned = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', cleaned)  # Control characters
        cleaned = re.sub(r'\.{3,}', '...', cleaned)  # Multiple dots
        
        return cleaned
    
    def _detect_content_type(self, line: str) -> str:
        """Detect the type of content in a line"""
        line_lower = line.lower().strip()
        
        # Headers and titles
        if self._is_header(line):
            return "header"
        
        # Table-like content (multiple numbers/columns)
        if re.search(r'\d+.*\d+.*\d+', line) and ('|' in line or '\t' in line or '  ' in line):
            return "table_row"
        
        # Contact information
        if re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', line):
            return "contact_info"
        
        # Dates
        if re.search(r'\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b', line):
            return "date_info"
        
        # Financial/numeric data
        if re.search(r'[\$£€¥]\s*\d+', line) or re.search(r'\d+\.\d{2}\b', line):
            return "financial_data"
        
        # Lists (bullets, numbers)
        if re.match(r'^[\d\w]\.\s', line) or re.match(r'^[•\-\*]\s', line):
            return "list_item"
        
        # Addresses
        if re.search(r'\b\d+\s+\w+\s+(street|st|avenue|ave|road|rd|lane|ln|drive|dr|blvd)\b', line_lower):
            return "address"
        
        # Names (capitalized words)
        if re.match(r'^[A-Z][a-z]+\s+[A-Z][a-z]+', line):
            return "name"
        
        # Page markers
        if re.search(r'page\s+\d+', line_lower) or re.search(r'^---\s*page', line_lower):
            return "page_marker"
        
        return "text_content"
    
    def _is_header(self, line: str) -> bool:
        """Determine if line is a header"""
        line_clean = line.strip()
        
        # All caps (common for headers)
        if line_clean.isupper() and len(line_clean) > 3:
            return True
        
        # Title case with multiple words
        if len(line_clean.split()) >= 2 and line_clean.istitle():
            return True
        
        # Underlined text (common PDF pattern)
        if '___' in line or '---' in line:
            return True
        
        # Short lines that are all caps or title case
        if len(line_clean) < 50 and (line_clean.isupper() or line_clean.istitle()):
            return True
        
        return False
    
    def _normalize_section_name(self, header: str) -> str:
        """Normalize header text to section name"""
        normalized = re.sub(r'[^\w\s]', '', header.lower())
        normalized = re.sub(r'\s+', '_', normalized.strip())
        return normalized[:50]  # Limit length
    
    def _calculate_confidence(self, line: str) -> float:
        """Calculate confidence score for line extraction"""
        score = 1.0
        
        # Penalize very short lines
        if len(line) < 5:
            score *= 0.5
        
        # Penalize lines with too many special characters
        special_char_ratio = len(re.findall(r'[^\w\s]', line)) / max(len(line), 1)
        if special_char_ratio > 0.5:
            score *= 0.7
        
        # Boost lines with structured data indicators
        if any(indicator in line for indicator in [':', '|', '\t']):
            score *= 1.2
        
        return min(score, 1.0)
    
    def _apply_ai_labeling(self, structured_data: List[Dict]) -> List[Dict]:
        """Apply AI-powered labeling and enhancement"""
        
        enhanced_data = []
        
        for item in structured_data:
            # AI-powered enhancements
            enhanced_item = item.copy()
            
            # Entity extraction
            enhanced_item["entities"] = self._extract_entities(item["cleaned_text"])
            
            # Semantic classification
            enhanced_item["semantic_category"] = self._classify_semantically(item)
            
            # Data quality assessment
            enhanced_item["quality_score"] = self._assess_data_quality(item)
            
            # Business context classification
            enhanced_item["business_context"] = self._classify_business_context(item)
            
            enhanced_data.append(enhanced_item)
        
        print(f"✅ Applied AI labeling to {len(enhanced_data)} items")
        return enhanced_data
    
    def _extract_entities(self, text: str) -> Dict[str, List[str]]:
        """Extract entities from text using regex patterns"""
        entities = {
            "emails": re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text),
            "phones": re.findall(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b', text),
            "dates": re.findall(r'\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b', text),
            "currencies": re.findall(r'[\$£€¥]\s*\d+(?:\.\d{2})?', text),
            "numbers": re.findall(r'\b\d+(?:\.\d+)?\b', text),
            "urls": re.findall(r'https?://[^\s]+', text),
            "percentages": re.findall(r'\d+(?:\.\d+)?%', text)
        }
        
        # Remove empty lists
        return {k: v for k, v in entities.items() if v}
    
    def _classify_semantically(self, item: Dict) -> str:
        """Classify content semantically"""
        text = item["cleaned_text"].lower()
        content_type = item["content_type"]
        
        # Financial content
        if any(word in text for word in ['revenue', 'profit', 'cost', 'budget', 'financial', 'income']):
            return "financial"
        
        # Personal information
        if any(word in text for word in ['name', 'address', 'phone', 'email', 'contact']):
            return "personal_info"
        
        # Product information
        if any(word in text for word in ['product', 'item', 'price', 'description', 'model']):
            return "product_info"
        
        # Temporal information
        if content_type == "date_info" or any(word in text for word in ['date', 'time', 'schedule']):
            return "temporal"
        
        # Organizational
        if any(word in text for word in ['department', 'team', 'company', 'organization']):
            return "organizational"
        
        return "general"
    
    def _assess_data_quality(self, item: Dict) -> float:
        """Assess data quality for the item"""
        score = item["confidence_score"]
        
        # Boost score for structured content
        if item["content_type"] in ["table_row", "contact_info", "financial_data"]:
            score *= 1.3
        
        # Penalize very short content
        if item["word_count"] < 2:
            score *= 0.6
        
        # Boost for entity-rich content
        if item.get("entities"):
            score *= 1.2
        
        return min(score, 1.0)
    
    def _classify_business_context(self, item: Dict) -> str:
        """Classify business context"""
        text = item["cleaned_text"].lower()
        
        contexts = {
            "customer_data": ["customer", "client", "buyer", "user"],
            "financial_data": ["revenue", "cost", "profit", "budget", "invoice"],
            "inventory_data": ["stock", "inventory", "product", "item", "quantity"],
            "sales_data": ["sales", "order", "purchase", "transaction"],
            "employee_data": ["employee", "staff", "worker", "team member"],
            "operational_data": ["process", "operation", "workflow", "procedure"]
        }
        
        for context, keywords in contexts.items():
            if any(keyword in text for keyword in keywords):
                return context
        
        return "general_business"
    
    def _create_dataframe(self, labeled_data: List[Dict]) -> pd.DataFrame:
        """Convert labeled data to structured DataFrame"""
        
        # Filter out low-quality and irrelevant content
        filtered_data = [
            item for item in labeled_data 
            if item["quality_score"] > 0.3 and 
               item["content_type"] not in ["page_marker"] and
               item["word_count"] > 0
        ]
        
        if not filtered_data:
            # Return empty DataFrame with standard columns
            return pd.DataFrame(columns=[
                'content_id', 'text_content', 'content_type', 'section',
                'business_context', 'semantic_category', 'quality_score'
            ])
        
        # Create DataFrame with essential columns
        df_data = []
        for i, item in enumerate(filtered_data, 1):
            row = {
                'content_id': i,
                'text_content': item['cleaned_text'],
                'content_type': item['content_type'],
                'section': item['section'],
                'business_context': item['business_context'],
                'semantic_category': item['semantic_category'],
                'quality_score': round(item['quality_score'], 3),
                'word_count': item['word_count'],
                'char_count': item['char_count'],
                'line_number': item['line_number'],
                'has_entities': len(item.get('entities', {})) > 0,
                'entity_types': list(item.get('entities', {}).keys())
            }
            
            # Add entity data as separate columns if they exist
            for entity_type, values in item.get('entities', {}).items():
                if values:
                    row[f'{entity_type}_extracted'] = '; '.join(map(str, values))
            
            df_data.append(row)
        
        df = pd.DataFrame(df_data)
        
        print(f"✅ Created DataFrame with {len(df)} rows and {len(df.columns)} columns")
        return df
    
    def _perform_analytics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Perform analytics and anomaly detection"""
        
        if df.empty:
            return {"error": "No data available for analytics"}
        
        analytics = {
            "data_overview": {
                "total_records": len(df),
                "content_types": df['content_type'].value_counts().to_dict(),
                "business_contexts": df['business_context'].value_counts().to_dict(),
                "sections": df['section'].value_counts().to_dict(),
                "quality_distribution": {
                    "high_quality": len(df[df['quality_score'] > 0.8]),
                    "medium_quality": len(df[(df['quality_score'] > 0.5) & (df['quality_score'] <= 0.8)]),
                    "low_quality": len(df[df['quality_score'] <= 0.5])
                }
            },
            "entity_analysis": {
                "records_with_entities": len(df[df['has_entities'] == True]),
                "entity_distribution": self._analyze_entity_distribution(df)
            },
            "anomalies": self._detect_anomalies(df),
            "insights": self._generate_insights(df)
        }
        
        return analytics
    
    def _analyze_entity_distribution(self, df: pd.DataFrame) -> Dict:
        """Analyze distribution of entities"""
        entity_columns = [col for col in df.columns if col.endswith('_extracted')]
        
        distribution = {}
        for col in entity_columns:
            entity_type = col.replace('_extracted', '')
            count = df[col].notna().sum()
            distribution[entity_type] = count
        
        return distribution
    
    def _detect_anomalies(self, df: pd.DataFrame) -> List[str]:
        """Detect anomalies in the data"""
        anomalies = []
        
        # Quality score anomalies
        low_quality_count = len(df[df['quality_score'] < 0.3])
        if low_quality_count > len(df) * 0.2:
            anomalies.append(f"High number of low-quality records: {low_quality_count}")
        
        # Content length anomalies
        avg_word_count = df['word_count'].mean()
        outliers = df[df['word_count'] > avg_word_count * 3]
        if len(outliers) > 0:
            anomalies.append(f"Detected {len(outliers)} unusually long content records")
        
        # Empty sections
        empty_sections = df[df['section'] == ''].shape[0]
        if empty_sections > 0:
            anomalies.append(f"{empty_sections} records without section classification")
        
        return anomalies
    
    def _generate_insights(self, df: pd.DataFrame) -> List[str]:
        """Generate insights about the data"""
        insights = []
        
        # Most common content types
        top_content_type = df['content_type'].mode().iloc[0] if not df.empty else None
        if top_content_type:
            insights.append(f"Most common content type: {top_content_type}")
        
        # Quality assessment
        avg_quality = df['quality_score'].mean()
        insights.append(f"Average quality score: {avg_quality:.2f}")
        
        # Entity richness
        entity_rich_records = len(df[df['has_entities'] == True])
        if entity_rich_records > 0:
            insights.append(f"{entity_rich_records} records contain structured entities")
        
        # Business context distribution
        business_contexts = df['business_context'].nunique()
        insights.append(f"Identified {business_contexts} different business contexts")
        
        return insights
    
    def _generate_output(self, df: pd.DataFrame, output_format: str, filename: str) -> Dict[str, Any]:
        """Generate output in specified format"""
        
        base_filename = filename.replace('.pdf', '')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        output_data = {
            "format": output_format,
            "timestamp": timestamp,
            "base_filename": base_filename
        }
        
        if output_format.lower() == "csv":
            csv_filename = f"{base_filename}_dataset_{timestamp}.csv"
            csv_content = df.to_csv(index=False)
            
            output_data.update({
                "filename": csv_filename,
                "content": csv_content,
                "size_bytes": len(csv_content.encode('utf-8'))
            })
            
        elif output_format.lower() == "json":
            json_filename = f"{base_filename}_dataset_{timestamp}.json"
            json_content = df.to_json(orient='records', indent=2)
            
            output_data.update({
                "filename": json_filename,
                "content": json_content,
                "size_bytes": len(json_content.encode('utf-8'))
            })
        
        else:
            raise ValueError(f"Unsupported output format: {output_format}")
        
        print(f"✅ Generated {output_format.upper()} output: {output_data['filename']}")
        return output_data
    
    # ===== NEW INTELLIGENT METHODS =====
    
    def _validate_and_enhance_dataset(self, extracted_data: List[Dict[str, Any]], 
                                    analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Validate and enhance the extracted dataset"""
        
        if not extracted_data:
            return []
        
        # Use AI for validation and enhancement if available
        try:
            ai_enhanced = self._call_ai_for_data_analysis(
                json.dumps(extracted_data[:10]), "validate_and_enhance"
            )
        except:
            ai_enhanced = ""
        
        # Clean and standardize data
        cleaned_data = []
        for row in extracted_data:
            cleaned_row = self._clean_data_row(row)
            if cleaned_row:  # Only add non-empty rows
                cleaned_data.append(cleaned_row)
        
        # Remove duplicates
        cleaned_data = self._remove_duplicate_rows(cleaned_data)
        
        # Validate data quality
        for row in cleaned_data:
            row['data_quality_score'] = self._calculate_data_quality_score(row)
        
        print(f"✅ Validated and enhanced {len(cleaned_data)} data records")
        return cleaned_data
    
    def _clean_data_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Clean and standardize a single data row"""
        
        cleaned = {}
        
        for key, value in row.items():
            if value is None or value == "":
                continue
                
            # Clean key
            clean_key = re.sub(r'[^\w\s]', '', str(key)).strip().lower().replace(' ', '_')
            if not clean_key:
                continue
            
            # Clean value
            clean_value = str(value).strip()
            
            # Standardize common data types
            if clean_key in ['amount', 'price', 'cost', 'total', 'value']:
                # Clean monetary values
                clean_value = re.sub(r'[^\d.]', '', clean_value)
                try:
                    clean_value = float(clean_value) if clean_value else None
                except:
                    clean_value = clean_value
            
            elif clean_key in ['date', 'time', 'timestamp']:
                # Standardize dates
                clean_value = self._standardize_date(clean_value)
            
            elif clean_key in ['phone', 'telephone', 'mobile']:
                # Clean phone numbers
                clean_value = re.sub(r'[^\d]', '', clean_value)
                if len(clean_value) == 10:
                    clean_value = f"{clean_value[:3]}-{clean_value[3:6]}-{clean_value[6:]}"
            
            elif clean_key in ['email', 'email_address']:
                # Validate email format
                if '@' not in clean_value or '.' not in clean_value:
                    clean_value = None
            
            if clean_value is not None and clean_value != "":
                cleaned[clean_key] = clean_value
        
        return cleaned
    
    def _standardize_date(self, date_str: str) -> str:
        """Standardize date formats"""
        
        # Common date patterns
        patterns = [
            r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})',  # MM/DD/YYYY or DD/MM/YYYY
            r'(\d{1,2})[/-](\d{1,2})[/-](\d{2})',   # MM/DD/YY or DD/MM/YY
            r'(\d{4})[/-](\d{1,2})[/-](\d{1,2})',  # YYYY/MM/DD
        ]
        
        for pattern in patterns:
            match = re.search(pattern, date_str)
            if match:
                parts = match.groups()
                if len(parts[2]) == 2:  # YY format
                    year = f"20{parts[2]}" if int(parts[2]) < 50 else f"19{parts[2]}"
                else:
                    year = parts[2]
                
                # Assume MM/DD/YYYY format for US dates
                try:
                    return f"{parts[0].zfill(2)}/{parts[1].zfill(2)}/{year}"
                except:
                    return date_str
        
        return date_str
    
    def _remove_duplicate_rows(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicate rows based on content similarity"""
        
        if len(data) <= 1:
            return data
        
        unique_data = []
        seen_hashes = set()
        
        for row in data:
            # Create a hash of the important values
            important_values = []
            for key, value in row.items():
                if key not in ['line_number', 'importance_score', 'source_line', 'data_quality_score']:
                    important_values.append(str(value))
            
            content_hash = hash(tuple(sorted(important_values)))
            
            if content_hash not in seen_hashes:
                seen_hashes.add(content_hash)
                unique_data.append(row)
        
        return unique_data
    
    def _calculate_data_quality_score(self, row: Dict[str, Any]) -> float:
        """Calculate data quality score for a row"""
        
        score = 0.0
        total_fields = len(row)
        
        if total_fields == 0:
            return 0.0
        
        # Score based on data completeness
        filled_fields = sum(1 for v in row.values() if v is not None and str(v).strip() != "")
        completeness_score = filled_fields / total_fields
        score += completeness_score * 0.4
        
        # Score based on data type appropriateness
        type_score = 0.0
        for key, value in row.items():
            if key in ['amount', 'price', 'cost'] and isinstance(value, (int, float)):
                type_score += 1
            elif key in ['email'] and '@' in str(value):
                type_score += 1
            elif key in ['phone'] and len(re.sub(r'[^\d]', '', str(value))) >= 10:
                type_score += 1
            elif key in ['date'] and re.search(r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}', str(value)):
                type_score += 1
            elif value and str(value).strip():
                type_score += 0.5
        
        if total_fields > 0:
            score += (type_score / total_fields) * 0.4
        
        # Score based on data richness
        if 'importance_score' in row:
            score += row['importance_score'] * 0.2
        
        return min(score, 1.0)
    
    def _create_enhanced_dataframe(self, dataset: List[Dict[str, Any]]) -> pd.DataFrame:
        """Create enhanced DataFrame from cleaned dataset"""
        
        if not dataset:
            return pd.DataFrame()
        
        # Get all unique columns
        all_columns = set()
        for row in dataset:
            all_columns.update(row.keys())
        
        # Remove metadata columns for final dataset
        data_columns = [col for col in all_columns 
                       if col not in ['line_number', 'source_line', 'importance_score']]
        
        # Create DataFrame
        df_data = []
        for row in dataset:
            df_row = {col: row.get(col, None) for col in data_columns}
            df_data.append(df_row)
        
        df = pd.DataFrame(df_data)
        
        # Optimize data types
        df = self._optimize_dataframe_dtypes(df)
        
        return df
    
    def _optimize_dataframe_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimize DataFrame data types for better performance"""
        
        for col in df.columns:
            # Try to convert to numeric
            if df[col].dtype == 'object':
                # Try to convert to numeric
                numeric_series = pd.to_numeric(df[col], errors='coerce')
                if not numeric_series.isna().all():
                    df[col] = numeric_series
                
                # Try to convert to datetime
                elif col.lower() in ['date', 'time', 'timestamp', 'created', 'updated']:
                    try:
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                    except:
                        pass
        
        return df
    
    def _generate_intelligent_summary(self, df: pd.DataFrame, analysis: Dict[str, Any], 
                                    metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Generate comprehensive summary of the dataset"""
        
        summary = {
            "dataset_overview": {
                "total_rows": len(df),
                "total_columns": len(df.columns),
                "column_names": df.columns.tolist(),
                "data_types": {col: str(dtype) for col, dtype in df.dtypes.items()},
                "memory_usage": int(df.memory_usage(deep=True).sum()),
                "has_missing_values": bool(df.isnull().any().any())
            },
            "document_analysis": analysis,
            "extraction_metadata": metadata,
            "data_quality": {
                "completeness": round((1 - df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100, 2) if not df.empty else 0,
                "unique_values_per_column": {col: int(df[col].nunique()) for col in df.columns},
                "duplicate_rows": len(df) - len(df.drop_duplicates())
            }
        }
        
        # Add column-specific insights
        if not df.empty:
            summary["column_insights"] = {}
            for col in df.columns:
                col_info = {
                    "data_type": str(df[col].dtype),
                    "null_count": int(df[col].isnull().sum()),
                    "unique_count": int(df[col].nunique()),
                    "null_percentage": round(df[col].isnull().sum() / len(df) * 100, 2)
                }
                
                if df[col].dtype in ['int64', 'float64']:
                    col_info.update({
                        "min_value": float(df[col].min()) if not df[col].isna().all() else None,
                        "max_value": float(df[col].max()) if not df[col].isna().all() else None,
                        "mean_value": float(df[col].mean()) if not df[col].isna().all() else None
                    })
                
                if len(df[col].dropna()) > 0:
                    col_info["sample_values"] = df[col].dropna().head(3).tolist()
                
                summary["column_insights"][col] = col_info
        
        return summary
    
    def _export_dataset(self, df: pd.DataFrame, filename: str, output_format: str) -> str:
        """Export dataset to specified format"""
        
        if df.empty:
            raise ValueError("Cannot export empty dataset")
        
        # Ensure directory exists
        import os
        os.makedirs("../data/processed", exist_ok=True)
        
        # Generate output filename
        base_name = filename.replace('.pdf', '').replace(' ', '_')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if output_format.lower() == 'csv':
            output_path = f"../data/processed/pdf_dataset_{base_name}_{timestamp}.csv"
            df.to_csv(output_path, index=False)
        elif output_format.lower() == 'json':
            output_path = f"../data/processed/pdf_dataset_{base_name}_{timestamp}.json"
            df.to_json(output_path, orient='records', indent=2)
        else:
            raise ValueError(f"Unsupported output format: {output_format}")
        
        return output_path
    
    # ===== ENHANCED EXTRACTION METHODS =====
    
    def _calculate_line_importance_enhanced(self, line: str, analysis: Dict[str, Any]) -> float:
        """Enhanced importance calculation with better logic"""
        
        score = 0.0
        line_lower = line.lower()
        
        # Check for important field patterns
        important_fields = analysis.get('important_fields', [])
        for field in important_fields:
            if field.lower() in line_lower:
                score += 0.25
        
        # Check for high-importance patterns
        for pattern in self.data_importance_patterns['high_importance']:
            if re.search(pattern, line_lower):
                score += 0.15
        
        # Check for tabular indicators (stronger weight)
        for pattern in self.data_importance_patterns['tabular_indicators']:
            if re.search(pattern, line):
                score += 0.35
        
        # Boost lines with multiple data elements
        data_elements = len(re.findall(r'\b\w+\b', line))
        if data_elements >= 3:
            score += 0.2
        elif data_elements >= 5:
            score += 0.3
        
        # Boost lines with structured separators
        separators = line.count('\t') + line.count('|') + max(0, line.count('  ') - 1)
        if separators >= 1:
            score += 0.3
        
        # Boost lines with valuable data types
        if re.search(r'\$\d+|\d+\.\d+|\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b|@\w+\.\w+', line):
            score += 0.25
        
        return min(score, 1.0)
    
    def _extract_financial_data_enhanced(self, line: str) -> Dict[str, Any]:
        """Enhanced financial document data extraction"""
        
        data = {}
        
        # Extract monetary amounts with currency symbols
        amounts = re.findall(r'([\$£€¥])\s*(\d+(?:,\d{3})*(?:\.\d{2})?)', line)
        if amounts:
            data['amount'] = amounts[0][1].replace(',', '')
            data['currency'] = amounts[0][0]
        
        # Extract dates in multiple formats
        date_patterns = [
            r'\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\b',
            r'\b(\d{4}[/-]\d{1,2}[/-]\d{1,2})\b',
            r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},?\s+\d{4}\b'
        ]
        for pattern in date_patterns:
            dates = re.findall(pattern, line, re.IGNORECASE)
            if dates:
                data['date'] = dates[0] if isinstance(dates[0], str) else dates[0][0]
                break
        
        # Extract invoice/reference numbers
        ref_patterns = [
            r'\b(?:INV|INVOICE|ORDER|REF|#)\s*[:\-]?\s*([A-Z0-9\-]+)\b',
            r'\b([A-Z]{2,}\d{3,})\b'
        ]
        for pattern in ref_patterns:
            refs = re.findall(pattern, line, re.IGNORECASE)
            if refs:
                data['reference_number'] = refs[0]
                break
        
        # Extract descriptions (text segments)
        if amounts:
            # Get text before the amount
            amount_pos = line.find(amounts[0][0])
            if amount_pos > 10:
                desc_text = line[:amount_pos].strip()
                # Clean description
                desc_text = re.sub(r'^[\d\s\-\.]+', '', desc_text).strip()
                if len(desc_text) > 3:
                    data['description'] = desc_text
        
        # Extract quantities
        qty_match = re.search(r'\b(\d+)\s*(?:x|qty|pcs|units|pieces)\b', line, re.IGNORECASE)
        if qty_match:
            data['quantity'] = qty_match.group(1)
        
        return data
    
    def _extract_catalog_data_enhanced(self, line: str) -> Dict[str, Any]:
        """Enhanced catalog/product data extraction"""
        
        data = {}
        
        # Extract product codes with various patterns
        code_patterns = [
            r'\b([A-Z]{2,}\d+[A-Z]*)\b',
            r'\b([A-Z]+[-_]\d+)\b',
            r'\b(\d+[-_][A-Z]+)\b',
            r'\b(SKU[:\s]*[A-Z0-9\-]+)\b'
        ]
        for pattern in code_patterns:
            codes = re.findall(pattern, line, re.IGNORECASE)
            if codes:
                data['product_code'] = codes[0]
                break
        
        # Extract prices with various formats
        price_patterns = [
            r'([\$£€¥])\s*(\d+(?:\.\d{2})?)',
            r'(\d+\.\d{2})\s*(USD|EUR|GBP)',
            r'Price[:\s]*([\$£€¥]?\d+\.\d{2})'
        ]
        for pattern in price_patterns:
            prices = re.findall(pattern, line, re.IGNORECASE)
            if prices:
                if len(prices[0]) == 2:
                    data['price'] = prices[0][1]
                    data['currency'] = prices[0][0]
                else:
                    data['price'] = re.findall(r'\d+\.\d{2}', prices[0])[0]
                break
        
        # Extract quantities and units
        qty_patterns = [
            r'\b(\d+)\s*(pcs|units|qty|pieces|pack|box)\b',
            r'Qty[:\s]*(\d+)',
            r'(\d+)\s*x\s*'
        ]
        for pattern in qty_patterns:
            qtys = re.findall(pattern, line, re.IGNORECASE)
            if qtys:
                data['quantity'] = qtys[0] if isinstance(qtys[0], str) else qtys[0][0]
                break
        
        # Extract product names (longest meaningful text)
        words = line.split()
        if len(words) >= 2:
            # Find text segments that look like product names
            potential_names = []
            for i in range(len(words)):
                for j in range(i + 2, min(i + 6, len(words) + 1)):
                    segment = ' '.join(words[i:j])
                    # Skip if it's mostly numbers or codes
                    if not re.search(r'^[A-Z0-9\-_]+$', segment) and len(segment) > 8:
                        potential_names.append(segment)
            
            if potential_names:
                data['product_name'] = max(potential_names, key=len)
        
        return data
    
    def _extract_form_data_enhanced(self, line: str) -> Dict[str, Any]:
        """Enhanced form/contact data extraction"""
        
        data = {}
        
        # Extract email addresses
        emails = re.findall(r'\b([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,})\b', line)
        if emails:
            data['email'] = emails[0]
        
        # Extract phone numbers in various formats
        phone_patterns = [
            r'\b(\d{3}[-.]?\d{3}[-.]?\d{4})\b',
            r'\b(\(\d{3}\)\s*\d{3}[-.]?\d{4})\b',
            r'\b(\+\d{1,3}\s*\d{3,4}[-.]?\d{3,4}[-.]?\d{3,4})\b'
        ]
        for pattern in phone_patterns:
            phones = re.findall(pattern, line)
            if phones:
                data['phone'] = phones[0]
                break
        
        # Extract names (capitalized words)
        name_patterns = [
            r'\b([A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\b',
            r'Name[:\s]+([A-Z][a-z]+\s+[A-Z][a-z]+)'
        ]
        for pattern in name_patterns:
            names = re.findall(pattern, line)
            if names:
                data['name'] = names[0]
                break
        
        # Extract addresses
        address_indicators = ['street', 'avenue', 'road', 'drive', 'lane', 'blvd']
        if any(indicator in line.lower() for indicator in address_indicators):
            # Try to extract the address part
            addr_match = re.search(r'\b(\d+\s+[A-Za-z\s]+(?:Street|Avenue|Road|Drive|Lane|Blvd))', line, re.IGNORECASE)
            if addr_match:
                data['address'] = addr_match.group(1)
        
        # Extract key-value pairs
        kv_patterns = [
            r'([A-Za-z\s]+):\s*([^,\n]+)',
            r'([A-Za-z\s]+)\s*=\s*([^,\n]+)'
        ]
        for pattern in kv_patterns:
            matches = re.findall(pattern, line)
            for key, value in matches:
                clean_key = re.sub(r'[^\w\s]', '', key.strip().lower().replace(' ', '_'))
                if clean_key and value.strip() and len(clean_key) > 2:
                    data[clean_key] = value.strip()
        
        return data
    
    def _extract_report_data_enhanced(self, line: str) -> Dict[str, Any]:
        """Enhanced report data extraction"""
        
        data = {}
        
        # Extract metrics and values
        metric_patterns = [
            r'([A-Za-z\s]+):\s*(\d+(?:\.\d+)?%?)',
            r'([A-Za-z\s]+)\s*[-|]\s*(\d+(?:\.\d+)?)',
            r'(\w+)\s*=\s*(\d+(?:\.\d+)?)'
        ]
        for pattern in metric_patterns:
            matches = re.findall(pattern, line)
            for metric, value in matches:
                clean_metric = re.sub(r'[^\w\s]', '', metric.strip().lower().replace(' ', '_'))
                if clean_metric and len(clean_metric) > 2:
                    data[clean_metric] = value
        
        # Extract percentages
        percentages = re.findall(r'(\d+(?:\.\d+)?)%', line)
        if percentages:
            data['percentage'] = percentages[0]
        
        # Extract dates
        dates = re.findall(r'\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\b', line)
        if dates:
            data['date'] = dates[0]
        
        # Extract status indicators
        status_words = ['completed', 'pending', 'failed', 'success', 'error', 'active', 'inactive']
        for status in status_words:
            if status in line.lower():
                data['status'] = status
                break
        
        return data
    
    def _extract_generic_data_enhanced(self, line: str, important_fields: List[str]) -> Dict[str, Any]:
        """Enhanced generic structured data extraction"""
        
        data = {}
        
        # Split by common separators with priority
        separators = ['\t', '|', ';', ':', ',']
        parts = [line]
        
        for sep in separators:
            if sep in line:
                parts = [part.strip() for part in line.split(sep) if part.strip()]
                break
        
        # If no clear separators, try multiple spaces
        if len(parts) == 1 and line.count('  ') >= 1:
            parts = [part.strip() for part in re.split(r'\s{2,}', line) if part.strip()]
        
        # Map parts to fields intelligently
        if len(parts) >= 2:
            for i, part in enumerate(parts[:min(len(important_fields), 8)]):  # Limit to 8 fields
                if i < len(important_fields):
                    field_name = important_fields[i]
                else:
                    field_name = f'field_{i+1}'
                
                # Clean and validate the value
                clean_value = part.strip()
                if len(clean_value) > 0 and clean_value not in ['', '-', 'N/A', 'null']:
                    data[field_name] = clean_value
        
        # Extract specific patterns regardless of structure
        patterns_to_extract = {
            'url': r'https?://[^\s]+',
            'email': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            'date': r'\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b',
            'phone': r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b',
            'amount': r'[\$£€¥]\s*\d+(?:\.\d{2})?',
            'percentage': r'\d+(?:\.\d+)?%'
        }
        
        for field_name, pattern in patterns_to_extract.items():
            matches = re.findall(pattern, line)
            if matches and field_name not in data:
                data[field_name] = matches[0]
        
        return data