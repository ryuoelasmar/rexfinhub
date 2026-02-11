@echo off
echo ==========================================
echo  ETP Filing Tracker - Local Dev Server
echo ==========================================
echo.
echo Starting at http://localhost:8000
echo Hot-reload enabled (edit code, browser refreshes)
echo Press Ctrl+C to stop
echo.
cd /d D:\REX_ETP_TRACKER
python -m uvicorn webapp.main:app --reload --port 8000
