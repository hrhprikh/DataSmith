# 🚀 DataSmith AI - Render Deployment Guide

## 📋 Pre-Deployment Checklist

### ✅ **Required Files Added**
- ✅ `requirements.txt` - All Python dependencies
- ✅ `Procfile` - Process definition for Render
- ✅ `render.yaml` - Render service configuration
- ✅ `runtime.txt` - Python version specification
- ✅ `.env.example` - Environment variables template
- ✅ `build.sh` - Build script for setup

### ✅ **Code Optimizations**
- ✅ Updated CORS configuration for production
- ✅ Added production-ready server configuration
- ✅ Environment variable support
- ✅ Error handling for missing dependencies

## 🌐 **Deployment Steps**

### **1. Connect Repository to Render**
1. Go to [Render Dashboard](https://dashboard.render.com/)
2. Click "New +" → "Web Service"
3. Connect your GitHub repository: `https://github.com/hrhprikh/DataSmith`
4. Select the repository and continue

### **2. Configure Web Service**
```yaml
Name: datasmith-api
Environment: Python 3
Region: Oregon (US West) or your preferred region
Branch: master
Build Command: ./build.sh
Start Command: cd backend && python -m uvicorn app:app --host 0.0.0.0 --port $PORT
```

### **3. Environment Variables**
Set these in Render dashboard under "Environment":
```
PORT=8000
PYTHON_VERSION=3.9.16
DEBUG=False
LM_STUDIO_ENABLED=false
MAX_FILE_SIZE=50000000
PROCESSING_TIMEOUT=300
```

### **4. Advanced Settings**
```
Instance Type: Free (or upgrade as needed)
Auto-Deploy: Yes
Health Check Path: /
```

## 🔧 **Configuration Details**

### **Runtime Configuration**
- **Python Version**: 3.9.16 (stable and compatible)
- **Web Server**: Uvicorn with FastAPI
- **Process Type**: Web service
- **Health Check**: Root endpoint with status

### **Dependencies**
All required packages are in `requirements.txt`:
- FastAPI + Uvicorn for web server
- pandas + numpy for data processing
- pdfplumber + PyMuPDF for PDF processing
- requests for HTTP communications
- Additional utilities and middleware

### **File Structure**
```
DataSmith/
├── 🔧 Procfile (process definition)
├── 📦 requirements.txt (dependencies)
├── ⚙️ runtime.txt (Python version)
├── 🛠️ build.sh (build script)
├── 📋 render.yaml (service config)
├── 🌐 backend/ (FastAPI application)
└── 💻 frontend/ (static files)
```

## 🌟 **Features Available After Deployment**

### **🔗 API Endpoints**
- `GET /` - Health check and welcome
- `POST /process/` - AI-powered CSV processing
- `POST /pdf/convert-to-csv` - PDF to dataset conversion
- `POST /convert/csv-to-json/` - Data format conversion
- `GET /lm-studio/status` - AI service status

### **📱 Frontend Access**
- Modern web interface for data processing
- Real-time processing status
- File upload and download
- Interactive data visualization

## 🚀 **Post-Deployment**

### **1. Test Your Deployment**
```bash
# Test health endpoint
curl https://your-app-name.onrender.com/

# Test PDF processing
curl -X POST https://your-app-name.onrender.com/pdf/health

# Upload and process files via web interface
```

### **2. Monitor Your Application**
- Check Render dashboard for logs
- Monitor resource usage
- Set up alerts for downtime

### **3. Custom Domain (Optional)**
- Add custom domain in Render dashboard
- Configure DNS settings
- Enable HTTPS (automatic with Render)

## ⚡ **Performance Optimization**

### **Free Plan Limitations**
- CPU: 0.1 vCPU
- Memory: 512 MB RAM
- Bandwidth: 100 GB/month
- Build time: 10 minutes max

### **Recommended Upgrades**
For production use, consider upgrading to:
- **Starter Plan**: $7/month (0.5 vCPU, 512 MB RAM)
- **Standard Plan**: $25/month (1 vCPU, 2 GB RAM)

## 🔒 **Security Considerations**

### **Environment Variables**
Never commit sensitive data. Use Render's environment variables for:
- API keys
- Database URLs
- Secret keys
- External service URLs

### **CORS Configuration**
Current setup allows:
- Frontend origins
- Render subdomains
- Development localhost

## 🆘 **Troubleshooting**

### **Common Issues**

1. **Build Failures**
   - Check Python version compatibility
   - Verify all dependencies in requirements.txt
   - Review build logs in Render dashboard

2. **Runtime Errors**
   - Check application logs
   - Verify environment variables
   - Test locally first

3. **Memory Issues**
   - Optimize DataFrame operations
   - Use streaming for large files
   - Consider upgrading plan

### **Debug Commands**
```bash
# Check build logs
# Available in Render dashboard

# Test locally with production settings
export PORT=8000
cd backend && python app.py

# Verify dependencies
pip install -r requirements.txt
```

## 📞 **Support**

- **Render Documentation**: https://render.com/docs
- **DataSmith Issues**: https://github.com/hrhprikh/DataSmith/issues
- **Contact**: hrhprikh@gmail.com

---

## 🎉 **Ready to Deploy!**

Your DataSmith AI Platform is now ready for deployment on Render. Follow the steps above and you'll have a production-ready data processing platform available worldwide!

**Deployment URL will be**: `https://your-app-name.onrender.com`