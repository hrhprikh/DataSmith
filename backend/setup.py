"""
🔧 Setup script for DataSmith AI - handles installation issues
"""

import subprocess
import sys
import os

def run_command(cmd, description):
    """Run a command and handle errors gracefully"""
    print(f"🔄 {description}")
    print(f"Running: {cmd}")
    
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ {description} - Success!")
            return True
        else:
            print(f"❌ {description} - Failed!")
            print(f"Error: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ {description} - Exception: {e}")
        return False

def install_packages():
    """Install required packages with fallback methods"""
    
    print("🚀 Setting up DataSmith AI dependencies...")
    print("=" * 50)
    
    # Method 1: Try simple requirements
    if run_command("pip install -r requirements_simple.txt", "Installing from simple requirements"):
        return True
    
    print("\n⚠️ Batch installation failed, trying individual packages...")
    
    # Method 2: Install core packages individually
    core_packages = [
        "fastapi",
        "uvicorn[standard]", 
        "requests",
        "python-multipart"
    ]
    
    failed_packages = []
    
    for package in core_packages:
        if not run_command(f"pip install {package}", f"Installing {package}"):
            failed_packages.append(package)
    
    # Method 3: Try installing pandas/numpy with pre-compiled wheels
    print("\n📊 Installing data processing packages...")
    
    data_packages = [
        ("numpy", "pip install numpy"),
        ("pandas", "pip install pandas"),
        ("aiofiles", "pip install aiofiles")
    ]
    
    for name, cmd in data_packages:
        if not run_command(cmd, f"Installing {name}"):
            # Try alternative installation methods
            print(f"🔄 Trying alternative installation for {name}...")
            
            if name == "numpy":
                # Try installing from conda-forge or use older version
                alt_cmds = [
                    "pip install numpy==1.24.3",
                    "pip install numpy --no-build-isolation",
                    "pip install --only-binary=all numpy"
                ]
            elif name == "pandas":
                alt_cmds = [
                    "pip install pandas==2.0.3", 
                    "pip install pandas --no-build-isolation",
                    "pip install --only-binary=all pandas"
                ]
            else:
                alt_cmds = [f"pip install --no-build-isolation {name}"]
            
            success = False
            for alt_cmd in alt_cmds:
                if run_command(alt_cmd, f"Alternative installation: {alt_cmd}"):
                    success = True
                    break
            
            if not success:
                failed_packages.append(name)
    
    # Report results
    if failed_packages:
        print(f"\n⚠️ Some packages failed to install: {failed_packages}")
        print("💡 You can try manual installation or run without these packages")
        return False
    else:
        print(f"\n✅ All packages installed successfully!")
        return True

def test_imports():
    """Test if all required modules can be imported"""
    print("\n🧪 Testing imports...")
    
    required_modules = [
        ("fastapi", "FastAPI web framework"),
        ("uvicorn", "ASGI server"),
        ("requests", "HTTP client"),
        ("pandas", "Data processing"),
        ("numpy", "Numerical computing")
    ]
    
    failed_imports = []
    
    for module, description in required_modules:
        try:
            __import__(module)
            print(f"✅ {module} - {description}")
        except ImportError as e:
            print(f"❌ {module} - {description} - {e}")
            failed_imports.append(module)
    
    return len(failed_imports) == 0

def create_minimal_version():
    """Create a minimal version that works without pandas if needed"""
    
    minimal_app = '''
from fastapi import FastAPI, UploadFile, HTTPException
from fastapi.responses import JSONResponse
import requests
import json
import os

app = FastAPI(
    title="DataSmith AI - Minimal Version", 
    description="Basic CSV processing without pandas dependency"
)

@app.get("/")
def root():
    return {
        "message": "🚀 DataSmith AI - Minimal Version",
        "status": "Running without pandas dependency",
        "note": "Install pandas for full functionality"
    }

@app.post("/process-basic/")
async def process_basic(file: UploadFile):
    """Basic file processing without pandas"""
    
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files supported")
    
    content = await file.read()
    lines = content.decode('utf-8').split('\\n')
    
    return {
        "message": "Basic processing completed",
        "filename": file.filename,
        "lines_count": len(lines),
        "first_line": lines[0] if lines else "Empty file",
        "note": "Install pandas for AI processing features"
    }
'''
    
    with open('app_minimal.py', 'w') as f:
        f.write(minimal_app)
    
    print("💡 Created app_minimal.py as fallback option")

if __name__ == "__main__":
    print("🔧 DataSmith AI Setup")
    print("=" * 30)
    
    # Try to install packages
    if install_packages():
        # Test imports
        if test_imports():
            print("\n🎉 Setup completed successfully!")
            print("💡 You can now run: uvicorn app:app --reload")
        else:
            print("\n⚠️ Some imports failed. Creating minimal version...")
            create_minimal_version()
            print("💡 Run: uvicorn app_minimal:app --reload")
    else:
        print("\n⚠️ Installation had issues. Creating minimal version...")
        create_minimal_version()
        print("💡 Try running: uvicorn app_minimal:app --reload")
    
    print("\n📚 Next steps:")
    print("1. Start the server: uvicorn app:app --reload")
    print("2. Test with: python test_ai_enhancements.py")
    print("3. Or run minimal version if needed")