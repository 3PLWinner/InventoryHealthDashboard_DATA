@echo off
::cd /d "C:\Users\Administrator\Scripts\InventoryHealthDashboard_DATA"

:: Activate virtual environment
venv\Scripts\activate.bat 

pip install -r requirements.txt

python reports.py