@echo off
cd /d "C:\Users\Administrator\Scripts\InventoryHealthDashboard_DATA"

:: Activate virtual environment
call venv\Scripts\activate.bat

:: Run Python script using venv's Python
python reports.py