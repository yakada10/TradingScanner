# Procfile — used by Heroku-compatible platforms and local tools like Honcho/Foreman.
# For Render, the start command in render.yaml takes precedence.

# Single-process mode (Render free tier — worker runs as inline thread):
web: python -m uvicorn web.app:app --host 0.0.0.0 --port ${PORT:-5000}

# Multi-process mode (run both with `honcho start` locally):
# web: python -m uvicorn web.app:app --host 0.0.0.0 --port ${PORT:-5000}
# worker: python worker/scanner_worker.py
