# Simple File Manager - Distribution Package

## Files to Include
When distributing the Simple File Manager to another PC, include these files:

### Required Files:
- `file_manager.py` - Main application
- `run_file_manager.py` - Python launcher with checks
- `run_file_manager.bat` - Windows batch file launcher
- `run_file_manager.sh` - Unix/Linux/macOS shell script launcher
- `requirements_file_manager.txt` - Requirements information
- `INSTALLATION.md` - Installation instructions
- `FILE_MANAGER_README.md` - User documentation

### Optional Files:
- `DISTRIBUTION_PACKAGE.md` - This file

## Distribution Methods

### Method 1: Zip Archive
1. Create a zip file with all the files above
2. Send to the target PC
3. Extract and follow installation instructions

### Method 2: Git Clone
```bash
git clone https://github.com/judyandiealvarez/azure-sql-export.git
cd azuresqlfetch
python3 file_manager.py
```

### Method 3: Direct Download
1. Download individual files from the repository
2. Place in a folder
3. Run the appropriate launcher

## Platform-Specific Instructions

### Windows Users:
1. Double-click `run_file_manager.bat`
2. Or run: `python file_manager.py`

### macOS Users:
1. Run: `./run_file_manager.sh`
2. Or run: `python3 file_manager.py`

### Linux Users:
1. Run: `./run_file_manager.sh`
2. Or run: `python3 file_manager.py`

## Quick Test
To verify everything works:
1. Extract files to a folder
2. Open terminal/command prompt in that folder
3. Run the appropriate launcher script
4. The file manager should open centered on screen

## Troubleshooting
- If tkinter errors occur, see INSTALLATION.md for platform-specific fixes
- The launcher scripts will check requirements and provide helpful error messages
- All files are self-contained and don't require internet connection to run
