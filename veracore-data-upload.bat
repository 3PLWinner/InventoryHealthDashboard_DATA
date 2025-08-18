@echo off
echo Starting Veracore Data Retrieval at %date% %time%

:: Navigate to the project directory
cd /d "C:\Users\Administrator\Scripts\InventoryHealthDashboard_DATA"
echo Current directory: %cd%

:: Check if virtual environment exists
if not exist "venv\Scripts\activate.bat" (
    echo ERROR: Virtual environment not found!
    echo Creating virtual environment...
    python -m venv venv
    echo Installing requirements...
    venv\Scripts\python.exe -m pip install -r requirements.txt
)

:: Activate virtual environment
echo Activating virtual environment...
call venv\Scripts\activate.bat
if errorlevel 1 (
    echo ERROR: Failed to activate virtual environment!
    pause
    exit /b 1
)

:: Check if .env file exists
if not exist ".env" (
    echo ERROR: .env file not found!
    echo Please create .env file with required credentials
    pause
    exit /b 1
)

:: Run the Python script with logging
echo Running Python script...
python reports.py

:: Check if the script ran successfully
if errorlevel 1 (
    echo ERROR: Python script failed with exit code %errorlevel%
) else (
    echo Python script completed successfully
)

echo Script completed at %date% %time%