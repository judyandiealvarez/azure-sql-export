#!/usr/bin/env python3
"""
Simple File Manager - A dual-pane file manager similar to Double Commander
"""

import tkinter as tk
from tkinter import ttk, messagebox, filedialog, simpledialog
import os
import shutil
import stat
from pathlib import Path
import datetime


class FileManager:
    def __init__(self, root):
        self.root = root
        self.root.title("Simple File Manager")
        self.root.geometry("1200x800")
        
        # Current directories for both panels
        self.left_dir = os.path.expanduser("~")
        self.right_dir = os.path.expanduser("~")
        self.active_panel = "left"  # Track which panel is active
        
        self.setup_ui()
        self.refresh_panels()
        
    def setup_ui(self):
        """Setup the user interface"""
        # Create main menu
        self.create_menu()
        
        # Create toolbar
        self.create_toolbar()
        
        # Create main frame with two panels
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Left panel
        left_frame = ttk.LabelFrame(main_frame, text="Left Panel")
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 2))
        
        # Right panel
        right_frame = ttk.LabelFrame(main_frame, text="Right Panel")
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=(2, 0))
        
        # Setup both panels
        self.setup_panel(left_frame, "left")
        self.setup_panel(right_frame, "right")
        
        # Status bar
        self.status_var = tk.StringVar()
        self.status_bar = ttk.Label(self.root, textvariable=self.status_var, relief=tk.SUNKEN)
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)
        
    def create_menu(self):
        """Create the main menu bar"""
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)
        
        # File menu
        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="File", menu=file_menu)
        file_menu.add_command(label="New Folder", command=self.new_folder)
        file_menu.add_command(label="New File", command=self.new_file)
        file_menu.add_separator()
        file_menu.add_command(label="Copy", command=self.copy_files)
        file_menu.add_command(label="Move", command=self.move_files)
        file_menu.add_command(label="Delete", command=self.delete_files)
        file_menu.add_command(label="Rename", command=self.rename_file)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.root.quit)
        
        # View menu
        view_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="View", menu=view_menu)
        view_menu.add_command(label="Refresh", command=self.refresh_panels)
        view_menu.add_command(label="Show Hidden Files", command=self.toggle_hidden)
        
        # Help menu
        help_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Help", menu=help_menu)
        help_menu.add_command(label="About", command=self.show_about)
        
    def create_toolbar(self):
        """Create the toolbar"""
        toolbar = ttk.Frame(self.root)
        toolbar.pack(side=tk.TOP, fill=tk.X, padx=5, pady=2)
        
        ttk.Button(toolbar, text="Copy", command=self.copy_files).pack(side=tk.LEFT, padx=2)
        ttk.Button(toolbar, text="Move", command=self.move_files).pack(side=tk.LEFT, padx=2)
        ttk.Button(toolbar, text="Delete", command=self.delete_files).pack(side=tk.LEFT, padx=2)
        ttk.Button(toolbar, text="Rename", command=self.rename_file).pack(side=tk.LEFT, padx=2)
        
        ttk.Separator(toolbar, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=5)
        
        ttk.Button(toolbar, text="New Folder", command=self.new_folder).pack(side=tk.LEFT, padx=2)
        ttk.Button(toolbar, text="Refresh", command=self.refresh_panels).pack(side=tk.LEFT, padx=2)
        
    def setup_panel(self, parent, panel_name):
        """Setup a file browser panel"""
        # Directory path
        path_frame = ttk.Frame(parent)
        path_frame.pack(fill=tk.X, padx=5, pady=2)
        
        ttk.Label(path_frame, text="Path:").pack(side=tk.LEFT)
        path_var = tk.StringVar()
        path_entry = ttk.Entry(path_frame, textvariable=path_var, state="readonly")
        path_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(5, 0))
        
        # Store references
        setattr(self, f"{panel_name}_path_var", path_var)
        setattr(self, f"{panel_name}_path_entry", path_entry)
        
        # Treeview for files and folders
        tree_frame = ttk.Frame(parent)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=2)
        
        # Create treeview with scrollbars
        tree = ttk.Treeview(tree_frame, columns=("size", "modified"), show="tree headings")
        tree.heading("#0", text="Name")
        tree.heading("size", text="Size")
        tree.heading("modified", text="Modified")
        
        tree.column("#0", width=300)
        tree.column("size", width=100)
        tree.column("modified", width=150)
        
        # Scrollbars
        v_scrollbar = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=tree.yview)
        h_scrollbar = ttk.Scrollbar(tree_frame, orient=tk.HORIZONTAL, command=tree.xview)
        tree.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)
        
        # Pack treeview and scrollbars
        tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        v_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        h_scrollbar.pack(side=tk.BOTTOM, fill=tk.X)
        
        # Bind events
        tree.bind("<Double-1>", lambda e: self.on_double_click(panel_name))
        tree.bind("<Button-1>", lambda e: self.on_click(panel_name))
        tree.bind("<Button-3>", lambda e: self.show_context_menu(e, panel_name))
        
        # Store reference
        setattr(self, f"{panel_name}_tree", tree)
        
    def get_current_dir(self, panel_name):
        """Get current directory for a panel"""
        return getattr(self, f"{panel_name}_dir")
        
    def set_current_dir(self, panel_name, path):
        """Set current directory for a panel"""
        setattr(self, f"{panel_name}_dir", path)
        
    def get_tree(self, panel_name):
        """Get tree widget for a panel"""
        return getattr(self, f"{panel_name}_tree")
        
    def get_path_var(self, panel_name):
        """Get path variable for a panel"""
        return getattr(self, f"{panel_name}_path_var")
        
    def on_click(self, panel_name):
        """Handle single click on panel"""
        self.active_panel = panel_name
        
    def on_double_click(self, panel_name):
        """Handle double click on panel"""
        tree = self.get_tree(panel_name)
        selection = tree.selection()
        
        if selection:
            item = selection[0]
            item_text = tree.item(item, "text")
            current_dir = self.get_current_dir(panel_name)
            new_path = os.path.join(current_dir, item_text)
            
            if os.path.isdir(new_path):
                self.set_current_dir(panel_name, new_path)
                self.refresh_panel(panel_name)
                
    def refresh_panel(self, panel_name):
        """Refresh a specific panel"""
        tree = self.get_tree(panel_name)
        path_var = self.get_path_var(panel_name)
        current_dir = self.get_current_dir(panel_name)
        
        # Clear tree
        for item in tree.get_children():
            tree.delete(item)
            
        # Update path
        path_var.set(current_dir)
        
        try:
            # Add parent directory entry
            if current_dir != os.path.dirname(current_dir):
                tree.insert("", 0, text="..", values=("", ""))
                
            # Get directory contents
            items = []
            for item in os.listdir(current_dir):
                item_path = os.path.join(current_dir, item)
                try:
                    stat_info = os.stat(item_path)
                    size = ""
                    if os.path.isfile(item_path):
                        size = self.format_size(stat_info.st_size)
                    
                    modified = datetime.datetime.fromtimestamp(stat_info.st_mtime).strftime("%Y-%m-%d %H:%M")
                    
                    # Determine if it's a directory
                    is_dir = os.path.isdir(item_path)
                    icon = "📁" if is_dir else "📄"
                    
                    items.append((item, size, modified, is_dir, icon))
                except (OSError, PermissionError):
                    continue
                    
            # Sort items (directories first, then files)
            items.sort(key=lambda x: (not x[3], x[0].lower()))
            
            # Insert items into tree
            for item, size, modified, is_dir, icon in items:
                tree.insert("", tk.END, text=f"{icon} {item}", values=(size, modified))
                
        except (OSError, PermissionError) as e:
            messagebox.showerror("Error", f"Cannot access directory: {e}")
            
    def refresh_panels(self):
        """Refresh both panels"""
        self.refresh_panel("left")
        self.refresh_panel("right")
        
    def format_size(self, size_bytes):
        """Format file size in human readable format"""
        if size_bytes == 0:
            return "0 B"
        size_names = ["B", "KB", "MB", "GB", "TB"]
        i = 0
        while size_bytes >= 1024 and i < len(size_names) - 1:
            size_bytes /= 1024.0
            i += 1
        return f"{size_bytes:.1f} {size_names[i]}"
        
    def get_selected_items(self, panel_name):
        """Get selected items from a panel"""
        tree = self.get_tree(panel_name)
        selection = tree.selection()
        current_dir = self.get_current_dir(panel_name)
        
        items = []
        for item in selection:
            item_text = tree.item(item, "text")
            # Remove icon prefix
            if item_text.startswith("📁 ") or item_text.startswith("📄 "):
                item_text = item_text[2:]
            items.append(os.path.join(current_dir, item_text))
            
        return items
        
    def copy_files(self):
        """Copy selected files"""
        source_items = self.get_selected_items(self.active_panel)
        if not source_items:
            messagebox.showwarning("Warning", "No files selected")
            return
            
        # Get destination directory
        dest_panel = "right" if self.active_panel == "left" else "left"
        dest_dir = self.get_current_dir(dest_panel)
        
        try:
            for source in source_items:
                if os.path.isfile(source):
                    shutil.copy2(source, dest_dir)
                elif os.path.isdir(source):
                    dest_path = os.path.join(dest_dir, os.path.basename(source))
                    shutil.copytree(source, dest_path)
                    
            self.refresh_panel(dest_panel)
            self.status_var.set(f"Copied {len(source_items)} item(s)")
            
        except Exception as e:
            messagebox.showerror("Error", f"Copy failed: {e}")
            
    def move_files(self):
        """Move selected files"""
        source_items = self.get_selected_items(self.active_panel)
        if not source_items:
            messagebox.showwarning("Warning", "No files selected")
            return
            
        # Get destination directory
        dest_panel = "right" if self.active_panel == "left" else "left"
        dest_dir = self.get_current_dir(dest_panel)
        
        try:
            for source in source_items:
                dest_path = os.path.join(dest_dir, os.path.basename(source))
                shutil.move(source, dest_path)
                
            self.refresh_panel(self.active_panel)
            self.refresh_panel(dest_panel)
            self.status_var.set(f"Moved {len(source_items)} item(s)")
            
        except Exception as e:
            messagebox.showerror("Error", f"Move failed: {e}")
            
    def delete_files(self):
        """Delete selected files"""
        items = self.get_selected_items(self.active_panel)
        if not items:
            messagebox.showwarning("Warning", "No files selected")
            return
            
        # Confirm deletion
        if messagebox.askyesno("Confirm Delete", f"Delete {len(items)} item(s)?"):
            try:
                for item in items:
                    if os.path.isfile(item):
                        os.remove(item)
                    elif os.path.isdir(item):
                        shutil.rmtree(item)
                        
                self.refresh_panel(self.active_panel)
                self.status_var.set(f"Deleted {len(items)} item(s)")
                
            except Exception as e:
                messagebox.showerror("Error", f"Delete failed: {e}")
                
    def rename_file(self):
        """Rename selected file"""
        items = self.get_selected_items(self.active_panel)
        if not items:
            messagebox.showwarning("Warning", "No file selected")
            return
            
        if len(items) > 1:
            messagebox.showwarning("Warning", "Please select only one item to rename")
            return
            
        old_path = items[0]
        old_name = os.path.basename(old_path)
        
        new_name = simpledialog.askstring("Rename", f"Enter new name:", initialvalue=old_name)
        if new_name and new_name != old_name:
            try:
                new_path = os.path.join(os.path.dirname(old_path), new_name)
                os.rename(old_path, new_path)
                self.refresh_panel(self.active_panel)
                self.status_var.set(f"Renamed to {new_name}")
            except Exception as e:
                messagebox.showerror("Error", f"Rename failed: {e}")
                
    def new_folder(self):
        """Create new folder"""
        current_dir = self.get_current_dir(self.active_panel)
        folder_name = simpledialog.askstring("New Folder", "Enter folder name:")
        
        if folder_name:
            try:
                new_path = os.path.join(current_dir, folder_name)
                os.makedirs(new_path, exist_ok=True)
                self.refresh_panel(self.active_panel)
                self.status_var.set(f"Created folder: {folder_name}")
            except Exception as e:
                messagebox.showerror("Error", f"Create folder failed: {e}")
                
    def new_file(self):
        """Create new file"""
        current_dir = self.get_current_dir(self.active_panel)
        file_name = simpledialog.askstring("New File", "Enter file name:")
        
        if file_name:
            try:
                new_path = os.path.join(current_dir, file_name)
                with open(new_path, 'w') as f:
                    pass
                self.refresh_panel(self.active_panel)
                self.status_var.set(f"Created file: {file_name}")
            except Exception as e:
                messagebox.showerror("Error", f"Create file failed: {e}")
                
    def show_context_menu(self, event, panel_name):
        """Show context menu"""
        # This is a placeholder for context menu functionality
        pass
        
    def toggle_hidden(self):
        """Toggle hidden files display"""
        # This is a placeholder for hidden files functionality
        messagebox.showinfo("Info", "Hidden files toggle not implemented yet")
        
    def show_about(self):
        """Show about dialog"""
        messagebox.showinfo("About", "Simple File Manager\nA dual-pane file manager\nVersion 1.0")


def main():
    root = tk.Tk()
    app = FileManager(root)
    root.mainloop()


if __name__ == "__main__":
    main()
