@echo off
cd /d "C:\Users\Administrator\Scripts\InventoryHealthDashboard_DATA"

:: Activate virtual environment
call venv\Scripts\activate.bat 

python reports.py