from pdf_to_dataset_processor import PDFToDatasetProcessor

print('📄 Testing PDF processor initialization...')
processor = PDFToDatasetProcessor()
print('✅ PDF processor initialized successfully')

# Test if PDF libraries are available
try:
    import pdfplumber
    import fitz
    print('✅ PDF libraries (pdfplumber, PyMuPDF) are available')
except ImportError as e:
    print(f'❌ PDF library import error: {e}')

print('🚀 PDF to Dataset Processor is ready!')