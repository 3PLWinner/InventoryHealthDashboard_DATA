@echo off
cd /d "C:\Users\Administrator\Scripts\InventoryHealthDashboard_DATA"

:: Activate the existing virtual environment
call venv\Scripts\activate.bat

:: Run the Python script
python reports.py