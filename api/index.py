# Vercel serverless function entry point for Flask app
import sys
import os

# Force unbuffered output for Vercel logs
sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None
sys.stderr.reconfigure(line_buffering=True) if hasattr(sys.stderr, 'reconfigure') else None

# Add parent directory to path so we can import app
current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, current_dir)

# Change to the project root directory
os.chdir(current_dir)

print("="*80, flush=True)
print("🚀 VERCEL SERVERLESS FUNCTION INITIALIZING", flush=True)
print(f"📁 Working Directory: {current_dir}", flush=True)
print("="*80, flush=True)

# Import the Flask app from app.py
try:
    from app import app
    print("✅ Flask app imported successfully", flush=True)
except Exception as e:
    print(f"❌ Failed to import Flask app: {e}", flush=True)
    import traceback
    traceback.print_exc()
    raise

# Vercel Python runtime expects a handler function
# Flask apps work with WSGI, so we use the app directly
handler = app

print("✅ Vercel handler ready", flush=True)

