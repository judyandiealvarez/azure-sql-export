# Simple File Manager - Installation Guide

## System Requirements
- Python 3.6 or higher
- tkinter (GUI library - usually included with Python)

## Installation Steps

### Windows
1. **Install Python:**
   - Download Python from https://python.org
   - Make sure to check "Add Python to PATH" during installation
   - tkinter is included by default

2. **Run the application:**
   ```cmd
   python file_manager.py
   ```

### macOS
1. **Install Python:**
   ```bash
   # Using Homebrew (recommended)
   brew install python@3.11
   brew install python-tk
   
   # Or download from python.org
   ```

2. **Run the application:**
   ```bash
   python3 file_manager.py
   ```

### Linux (Ubuntu/Debian)
1. **Install Python and tkinter:**
   ```bash
   sudo apt update
   sudo apt install python3 python3-tk
   ```

2. **Run the application:**
   ```bash
   python3 file_manager.py
   ```

### Linux (CentOS/RHEL/Fedora)
1. **Install Python and tkinter:**
   ```bash
   # CentOS/RHEL
   sudo yum install python3 tkinter
   
   # Fedora
   sudo dnf install python3 tkinter
   ```

2. **Run the application:**
   ```bash
   python3 file_manager.py
   ```

## Troubleshooting

### "No module named '_tkinter'" Error
- **Windows:** Reinstall Python with tkinter option checked
- **macOS:** Run `brew install python-tk`
- **Linux:** Install python3-tk package

### Permission Errors
- Make sure the file has execute permissions
- Run with appropriate user permissions

## Files Needed
- `file_manager.py` - Main application
- `requirements_file_manager.txt` - Requirements info
- `FILE_MANAGER_README.md` - Documentation

## Quick Start
1. Download all files to a folder
2. Open terminal/command prompt in that folder
3. Run: `python file_manager.py` (or `python3 file_manager.py`)
4. The file manager window should open centered on screen
