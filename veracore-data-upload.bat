@echo off
echo Starting Veracore Data Retrieval at %date% %time%
cd /d "C:\Users\Administrator\Scripts\InventoryHealthDashboard_DATA"

:: Activate virtual environment
call .\venv\Scripts\activate.bat 

pip install -r requirements.txt

:: Run the script with output logging
python reports.py >> script_log.txt 2>&1

echo Script completed at %date% %time%