@echo off
cd /d "C:Users\Administrator\Scripts\InventoryHealthDashboard_DATA

IF NOT EXIST "venv" (
    echo Creating virtual environment...
    python -m venv venv
    if errorlevel 1 (
        echo Failed to create virtual environment.
        exit /b 1
    )
) ELSE (
    echo Virtual environment already exists.
)

call venv\Scripts\activate.bat

pip install -r requirements.txt --disable-pip-version-check --quiet

python reports.py

:: Only pause if running interactively (not in Task Scheduler)
echo %CMDCMDLINE% | find /i "cmd.exe" >nul && pause