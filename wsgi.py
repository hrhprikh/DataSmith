# WSGI configuration for DataSmith AI
# NOTE: This file is NOT needed for FastAPI deployment on Render
# FastAPI uses ASGI, not WSGI. This file is only for reference.
# 
# Your FastAPI app should be deployed using:
# gunicorn -k uvicorn.workers.UvicornWorker backend.app:app
#
# If you really need WSGI compatibility (not recommended for FastAPI):

import os
import sys

# Add the backend directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'backend'))

# Import the FastAPI app
from backend.app import app

# Note: This converts ASGI to WSGI (loses async benefits)
# Only use if you absolutely must use WSGI servers
try:
    from asgiref.wsgi import WsgiToAsgi
    application = WsgiToAsgi(app)
except ImportError:
    # Fallback - but this won't work properly with async FastAPI features
    def application(environ, start_response):
        start_response('500 Internal Server Error', [('Content-Type', 'text/plain')])
        return [b'WSGI not compatible with FastAPI. Use ASGI instead.']

# WARNING: Using WSGI with FastAPI loses all async benefits
# Recommended: Use the Procfile configuration instead