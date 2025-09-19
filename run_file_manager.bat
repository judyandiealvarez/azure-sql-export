@echo off
echo Simple File Manager Launcher
echo ============================

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo Error: Python is not installed or not in PATH
    echo Please install Python from https://python.org
    pause
    exit /b 1
)

REM Try to run the launcher script
python run_file_manager.py
if errorlevel 1 (
    echo.
    echo If you see tkinter errors, try:
    echo 1. Reinstall Python with tkinter option checked
    echo 2. Or run: python file_manager.py directly
    pause
    exit /b 1
)

pause
