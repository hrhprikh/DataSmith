# 🌐 DataSmith AI Frontend

Modern web interface for DataSmith AI - Intelligent CSV Processing with Mistral 7B

## ✨ Features

### 🤖 AI Processing
- **Intelligent CSV Analysis** - Upload CSV files for full AI processing
- **Automatic Header Detection** - AI generates meaningful column names  
- **Smart Labeling** - Context-aware data categorization
- **Quick Analysis** - Get data insights without full processing

### 🔄 File Conversions
- **Smart CSV → JSON** - AI-powered header generation
- **Regular CSV → JSON** - Standard conversion with multiple formats
- **JSON → CSV** - Automatic nested structure flattening
- **Batch Processing** - Convert multiple files simultaneously

### 📊 Conversion Formats
- **Records**: `[{"col": "val"}, ...]` (default)
- **Index**: `{"0": {"col": "val"}, ...}`  
- **Values**: `[["val1", "val2"], ...]`
- **Split**: `{"columns": [...], "data": [...]}`

## 🚀 Quick Start

### 1. Start the Backend
```bash
cd ../backend
uvicorn app:app --reload
```

### 2. Start the Frontend
```bash
cd frontend
python server.py
```

### 3. Open Browser
- Frontend: `http://localhost:3000`
- API Docs: `http://localhost:8000/docs`

## 📁 File Structure

```
frontend/
├── index.html      # Main HTML structure
├── style.css       # Modern CSS styling  
├── script.js       # JavaScript functionality
├── server.py       # Simple HTTP server
└── README.md       # This file
```

## 🎨 Design Features

### 🌈 Modern UI
- **Gradient backgrounds** with glassmorphism effects
- **Responsive design** - works on all screen sizes
- **Smooth animations** and hover effects
- **Professional typography** using Inter font

### 🔧 User Experience  
- **Drag & drop** file uploads
- **Real-time progress** indicators
- **Detailed results** with download links
- **Tab-based navigation** for different features
- **Loading overlays** with status messages

### 📱 Responsive Layout
- **Desktop-first** design with mobile optimization
- **Grid-based** card layouts
- **Flexible containers** that adapt to screen size
- **Touch-friendly** interface elements

## 🎯 Usage Examples

### Smart CSV Conversion
1. Go to "File Conversions" → "CSV → JSON" 
2. Use "Smart Conversion" card
3. Upload your CSV file (even without proper headers)
4. Select JSON format and options
5. Click "Smart Convert"
6. AI will generate perfect headers like:
   - `"P001" → "patient_id"`
   - `"John" → "name"`  
   - `"45" → "age"`
   - `"Male" → "gender"`

### AI Processing
1. Go to "AI Processing"
2. Upload CSV in "Intelligent Processing" card
3. Enable "Force AI Processing" 
4. Click "Process with AI"
5. Get enhanced data with:
   - Improved headers
   - Smart labels and categories
   - Data quality insights
   - Statistical analysis

### Batch Operations
1. Go to "File Conversions" → "Batch Convert"
2. Upload multiple CSV/JSON files
3. Select target format
4. Process all files at once
5. Download individual results

## 🔗 API Integration

The frontend communicates with these backend endpoints:

- `POST /process/` - AI processing
- `POST /analyze/` - Quick analysis  
- `POST /convert/csv-to-json-smart/` - Smart CSV conversion
- `POST /convert/csv-to-json/` - Regular CSV conversion
- `POST /convert/json-to-csv/` - JSON to CSV
- `POST /convert/batch/` - Batch processing
- `GET /convert/download/{type}/{filename}` - File downloads

## 🎪 Interactive Features

### File Upload Areas
- **Visual feedback** on hover and drag
- **File type validation** 
- **Multiple file support** for batch operations
- **Progress indicators** during upload

### Results Display
- **Real-time updates** as operations complete
- **Detailed information** about conversions
- **Download buttons** for processed files
- **Error handling** with helpful messages
- **Success indicators** with file details

### Options & Settings
- **JSON format selection** (records, index, values, split)
- **Pretty formatting** toggle
- **Auto-flattening** for nested JSON
- **AI processing** force option
- **Batch target format** selection

## 🛠️ Technical Details

### Dependencies
- **No external frameworks** - Pure HTML/CSS/JavaScript
- **Font Awesome** icons via CDN
- **Inter font** from Google Fonts
- **Modern browser** features (ES6+, Fetch API)

### Browser Support
- **Chrome/Edge** 90+ (recommended)
- **Firefox** 88+
- **Safari** 14+
- **Mobile browsers** (responsive design)

### Performance
- **Lightweight** - No heavy frameworks
- **Fast loading** - Minimal dependencies  
- **Smooth animations** - Hardware-accelerated CSS
- **Efficient networking** - Modern Fetch API

## 🎨 Customization

### Colors & Themes
The CSS uses CSS custom properties for easy theming:

```css
:root {
  --primary-gradient: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
  --accent-color: #ffd700;
  --success-color: #28a745;
  --error-color: #dc3545;
}
```

### Layout Adjustments
- Modify `.container` max-width for different screen sizes
- Adjust `.card-grid` columns for layout changes
- Update `.section` padding for spacing

### Animation Customization  
- Change `transition` durations in CSS
- Modify `@keyframes` for different effects
- Adjust `transform` properties for interactions

## 📊 Browser DevTools

Use browser developer tools to:

### Network Tab
- Monitor API requests/responses
- Check file upload progress
- Debug connection issues

### Console Tab  
- View JavaScript logs and errors
- Check API connection status
- Debug file processing

### Elements Tab
- Inspect UI components
- Test responsive design
- Modify styles live

## 🚨 Troubleshooting

### Common Issues

**"Cannot connect to API"**
- Make sure backend is running on port 8000
- Check if `uvicorn app:app --reload` is active
- Verify no firewall blocking connections

**"File upload failed"**  
- Check file size (browser limits)
- Verify file format (.csv or .json)
- Try a different browser

**"Conversion errors"**
- Check if Mistral 7B (LM Studio) is running
- Verify CSV file format is valid
- Try the fallback conversion mode

### Performance Issues
- **Large files**: Use batch processing for better handling
- **Slow conversions**: Check AI model (LM Studio) performance
- **Memory issues**: Try smaller files or restart browser

## 🎉 Success Indicators

When everything works correctly:

1. ✅ **API Connection**: Green status in console
2. ✅ **File Uploads**: Files show as selected with checkmarks  
3. ✅ **Processing**: Loading indicators show progress
4. ✅ **Results**: Success cards appear with download links
5. ✅ **AI Features**: Smart headers generated automatically

## 💡 Tips & Best Practices

### File Preparation
- **CSV files**: Ensure consistent delimiters (comma, semicolon, tab)
- **JSON files**: Validate structure before upload
- **Large files**: Consider splitting into smaller chunks
- **Encoding**: Use UTF-8 when possible

### Optimal Usage
- **Smart conversion** for files with poor/no headers
- **Regular conversion** for well-structured files  
- **Batch processing** for multiple similar files
- **Analysis first** to understand data structure

### Performance
- **Process smaller files** for faster results
- **Use batch mode** for multiple files
- **Enable pretty formatting** only when needed
- **Check AI model** performance in LM Studio

---

🚀 **Ready to process your data intelligently!** Upload your CSV files and let DataSmith AI transform them with perfect headers and smart insights.