#!/usr/bin/env python3
"""
Simple File Manager Launcher
This script checks for Python and tkinter availability before launching the file manager.
"""

import sys
import os

def check_python_version():
    """Check if Python version is 3.6 or higher"""
    if sys.version_info < (3, 6):
        print("Error: Python 3.6 or higher is required.")
        print(f"Current version: {sys.version}")
        return False
    return True

def check_tkinter():
    """Check if tkinter is available"""
    try:
        import tkinter as tk
        return True
    except ImportError:
        print("Error: tkinter is not available.")
        print("Please install python3-tk package:")
        print("  - Ubuntu/Debian: sudo apt install python3-tk")
        print("  - CentOS/RHEL: sudo yum install tkinter")
        print("  - macOS: brew install python-tk")
        print("  - Windows: Reinstall Python with tkinter option")
        return False

def main():
    """Main launcher function"""
    print("Simple File Manager Launcher")
    print("=" * 30)
    
    # Check Python version
    if not check_python_version():
        input("Press Enter to exit...")
        return 1
    
    # Check tkinter
    if not check_tkinter():
        input("Press Enter to exit...")
        return 1
    
    # Check if file_manager.py exists
    if not os.path.exists("file_manager.py"):
        print("Error: file_manager.py not found in current directory.")
        input("Press Enter to exit...")
        return 1
    
    print("All checks passed. Starting Simple File Manager...")
    
    # Import and run the file manager
    try:
        from file_manager import main as run_file_manager
        run_file_manager()
    except Exception as e:
        print(f"Error starting file manager: {e}")
        input("Press Enter to exit...")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
