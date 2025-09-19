# Simple File Manager

A dual-pane file manager application similar to Double Commander, built with Python and tkinter.

## Features

- **Dual-pane interface**: Two side-by-side file browser panels
- **File operations**: Copy, move, delete, and rename files/folders
- **Directory navigation**: Double-click to enter folders, breadcrumb navigation
- **File information**: Display file size, modification date, and icons
- **Menu system**: File, View, and Help menus with keyboard shortcuts
- **Toolbar**: Quick access buttons for common operations
- **Status bar**: Shows current operation status

## Requirements

- Python 3.6 or higher
- No additional packages required (uses only standard library)

## Installation

1. Download or clone the repository
2. Ensure Python 3.6+ is installed on your system
3. No additional dependencies to install

## Usage

### Running the Application

```bash
python file_manager.py
```

### Basic Operations

1. **Navigation**: 
   - Double-click folders to enter them
   - Click ".." to go up one directory level
   - Click on a panel to make it active

2. **File Operations**:
   - Select files/folders by clicking on them
   - Use toolbar buttons or menu items for operations:
     - **Copy**: Copies selected items to the other panel
     - **Move**: Moves selected items to the other panel
     - **Delete**: Deletes selected items (with confirmation)
     - **Rename**: Renames the selected item

3. **Creating New Items**:
   - **New Folder**: Creates a new directory
   - **New File**: Creates a new empty file

### Keyboard Shortcuts

- **F5**: Refresh panels
- **F7**: Create new folder
- **F8**: Delete selected items
- **Ctrl+C**: Copy files
- **Ctrl+X**: Move files
- **F2**: Rename selected item

### Menu Options

- **File Menu**:
  - New Folder/File
  - Copy, Move, Delete, Rename
  - Exit

- **View Menu**:
  - Refresh panels
  - Show/Hide hidden files (placeholder)

- **Help Menu**:
  - About dialog

## Interface Layout

```
┌─────────────────────────────────────────────────────────────┐
│ File  View  Help                                           │
├─────────────────────────────────────────────────────────────┤
│ [Copy] [Move] [Delete] [Rename] [New Folder] [Refresh]     │
├─────────────────────────────────────────────────────────────┤
│ ┌─────────────────────┐ ┌─────────────────────┐             │
│ │ Left Panel          │ │ Right Panel         │             │
│ │ Path: /home/user    │ │ Path: /home/user    │             │
│ │ ┌─────────────────┐ │ │ ┌─────────────────┐ │             │
│ │ │ 📁 Documents    │ │ │ │ 📁 Downloads    │ │             │
│ │ │ 📁 Pictures     │ │ │ │ 📁 Music        │ │             │
│ │ │ 📄 file.txt     │ │ │ │ 📄 document.pdf │ │             │
│ │ └─────────────────┘ │ │ └─────────────────┘ │             │
│ └─────────────────────┘ └─────────────────────┘             │
├─────────────────────────────────────────────────────────────┤
│ Status: Ready                                              │
└─────────────────────────────────────────────────────────────┘
```

## Technical Details

- **GUI Framework**: tkinter (Python's built-in GUI library)
- **File Operations**: Uses `shutil` and `os` modules
- **Cross-platform**: Works on Windows, macOS, and Linux
- **Memory efficient**: Only loads visible directory contents

## Limitations

- No file preview functionality
- No advanced search capabilities
- No tab support
- No archive handling
- No network drive support
- Hidden files toggle is not implemented

## Future Enhancements

- File preview pane
- Advanced search and filtering
- Tab support for multiple directories
- Archive file support (ZIP, RAR, etc.)
- Network drive mounting
- File comparison tools
- Customizable interface themes

## License

This project is open source and available under the MIT License.

## Contributing

Feel free to submit issues, feature requests, or pull requests to improve this file manager.
