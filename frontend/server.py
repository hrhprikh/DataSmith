"""
🌐 Simple HTTP Server for DataSmith AI Frontend
Serves the frontend files with proper MIME types
"""

import http.server
import socketserver
import os
import webbrowser
from pathlib import Path

# Configuration
PORT = 3000
DIRECTORY = Path(__file__).parent

class CustomHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DIRECTORY, **kwargs)
    
    def end_headers(self):
        # Add CORS headers for API communication
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        super().end_headers()
    
    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()
    
    def log_message(self, format, *args):
        # Custom logging
        print(f"🌐 {args[0]} - {args[1]}")

def start_server():
    """Start the frontend server"""
    
    print("🚀 Starting DataSmith AI Frontend Server")
    print("=" * 50)
    
    try:
        # Change to frontend directory
        os.chdir(DIRECTORY)
        
        # Create server
        with socketserver.TCPServer(("", PORT), CustomHandler) as httpd:
            print(f"✅ Server running at: http://localhost:{PORT}")
            print(f"📁 Serving directory: {DIRECTORY}")
            print(f"🔗 API endpoint: http://localhost:8000")
            print("\n💡 Make sure your DataSmith AI backend is running on port 8000")
            print("   Start backend with: uvicorn app:app --reload")
            
            # Try to open browser automatically
            try:
                webbrowser.open(f"http://localhost:{PORT}")
                print(f"🌏 Opening browser automatically...")
            except:
                print(f"🌏 Open your browser and go to: http://localhost:{PORT}")
            
            print(f"\n📋 Available files:")
            for file in ["index.html", "style.css", "script.js"]:
                if os.path.exists(file):
                    print(f"   ✅ {file}")
                else:
                    print(f"   ❌ {file} (missing)")
            
            print(f"\n🛑 Press Ctrl+C to stop the server")
            print("=" * 50)
            
            # Start serving
            httpd.serve_forever()
            
    except KeyboardInterrupt:
        print(f"\n\n🛑 Server stopped by user")
    except Exception as e:
        print(f"\n❌ Server error: {e}")
        print(f"💡 Make sure port {PORT} is not already in use")

if __name__ == "__main__":
    start_server()