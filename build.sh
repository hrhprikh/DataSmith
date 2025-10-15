#!/bin/bash
# Build script for Render deployment

set -o errexit  # Exit on error

echo "📦 Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

echo "📁 Creating necessary directories..."
mkdir -p data/raw data/cleaned data/processed data/labeled uploads temp

echo "🔧 Setting up backend environment..."
# Create uploads and temp directories in backend
mkdir -p backend/uploads backend/temp

echo "✅ Build completed successfully!"
echo "🚀 DataSmith AI is ready for deployment with Gunicorn + Uvicorn!"