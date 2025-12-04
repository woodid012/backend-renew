# Vercel serverless function entry point for Flask app
import sys
import os

# Add parent directory to path so we can import app
current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, current_dir)

# Change to the project root directory
os.chdir(current_dir)

# Import the Flask app from app.py
from app import app

# Vercel Python runtime expects a handler function
# Flask apps work with WSGI, so we use the app directly
handler = app

