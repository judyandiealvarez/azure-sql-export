#!/bin/bash

echo "Simple File Manager Launcher"
echo "============================"

# Check if Python 3 is installed
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is not installed"
    echo "Please install Python 3.6 or higher"
    exit 1
fi

# Check Python version
python_version=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
required_version="3.6"

if [ "$(printf '%s\n' "$required_version" "$python_version" | sort -V | head -n1)" != "$required_version" ]; then
    echo "Error: Python 3.6 or higher is required"
    echo "Current version: $python_version"
    exit 1
fi

# Check if file_manager.py exists
if [ ! -f "file_manager.py" ]; then
    echo "Error: file_manager.py not found in current directory"
    exit 1
fi

echo "All checks passed. Starting Simple File Manager..."

# Try to run the launcher script first
if [ -f "run_file_manager.py" ]; then
    python3 run_file_manager.py
else
    # Fallback to direct execution
    python3 file_manager.py
fi
