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
import subprocess
from dulwich import porcelain
from dulwich.repo import Repo


class FileManager:
    def __init__(self, root):
        self.root = root
        self.root.title("Simple File Manager")
        self.root.geometry("1200x800")
        
        # Center the window on screen
        self.center_window()
        
        # Current directories for both panels
        self.left_dir = os.path.expanduser("~")
        self.right_dir = os.path.expanduser("~")
        self.active_panel = "left"  # Track which panel is active
        
        # Track selection position for each panel
        self.left_selection_index = 0
        self.right_selection_index = 0
        
        # Hidden files setting
        self.show_hidden_files = False
        
        self.setup_ui()
        self.refresh_panels()
        # Set initial focus to left panel
        self.get_tree("left").focus_force()
        # Ensure initial selection is visible
        self.root.after(100, self.ensure_selection_visible)
        # Bind F-key shortcuts
        self.bind_f_keys()
        
    def center_window(self):
        """Center the window on the screen"""
        # Update the window to get accurate dimensions
        self.root.update_idletasks()
        
        # Get window dimensions
        width = self.root.winfo_width()
        height = self.root.winfo_height()
        
        # Get screen dimensions
        screen_width = self.root.winfo_screenwidth()
        screen_height = self.root.winfo_screenheight()
        
        # Calculate position to center the window
        x = (screen_width - width) // 2
        y = (screen_height - height) // 2
        
        # Set the window position
        self.root.geometry(f"{width}x{height}+{x}+{y}")
        
    def setup_ui(self):
        """Setup the user interface"""
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
        
        # Create toolbar at bottom
        self.create_toolbar()
        
        # Create main menu (after toolbar to access hidden_var)
        self.create_menu()
        
        # Status bar
        self.status_var = tk.StringVar()
        self.status_bar = ttk.Label(self.root, textvariable=self.status_var, relief=tk.SUNKEN)
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)
        # Make status bar non-focusable
        self.status_bar.configure(takefocus=False)
        
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
        view_menu.add_checkbutton(label="Show Hidden Files", 
                                 variable=self.hidden_var,
                                 command=self.toggle_hidden_files)
        
        # Git menu
        git_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Git", menu=git_menu)
        git_menu.add_command(label="Clone Repository", command=self.git_clone)
        git_menu.add_command(label="Pull Changes", command=self.git_pull)
        git_menu.add_command(label="Add All & Commit", command=self.git_commit_all)
        git_menu.add_command(label="Push Changes", command=self.git_push)
        git_menu.add_separator()
        git_menu.add_command(label="Git Status", command=self.git_status)
        
        # Help menu
        help_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Help", menu=help_menu)
        help_menu.add_command(label="About", command=self.show_about)
        
    def create_toolbar(self):
        """Create the toolbar at bottom"""
        toolbar = ttk.Frame(self.root)
        toolbar.pack(side=tk.BOTTOM, fill=tk.X, padx=5, pady=2)
        
        # F3 - View (placeholder)
        view_btn = ttk.Button(toolbar, text="F3 View", command=self.view_file)
        view_btn.pack(side=tk.LEFT, padx=2)
        view_btn.configure(takefocus=False)
        
        # F4 - Edit (placeholder)
        edit_btn = ttk.Button(toolbar, text="F4 Edit", command=self.edit_file)
        edit_btn.pack(side=tk.LEFT, padx=2)
        edit_btn.configure(takefocus=False)
        
        # F5 - Copy
        copy_btn = ttk.Button(toolbar, text="F5 Copy", command=self.copy_files)
        copy_btn.pack(side=tk.LEFT, padx=2)
        copy_btn.configure(takefocus=False)
        
        # F6 - Move
        move_btn = ttk.Button(toolbar, text="F6 Move", command=self.move_files)
        move_btn.pack(side=tk.LEFT, padx=2)
        move_btn.configure(takefocus=False)
        
        # F7 - New Folder
        new_folder_btn = ttk.Button(toolbar, text="F7 MkDir", command=self.new_folder)
        new_folder_btn.pack(side=tk.LEFT, padx=2)
        new_folder_btn.configure(takefocus=False)
        
        # F8 - Delete
        delete_btn = ttk.Button(toolbar, text="F8 Delete", command=self.delete_files)
        delete_btn.pack(side=tk.LEFT, padx=2)
        delete_btn.configure(takefocus=False)
        
        ttk.Separator(toolbar, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=5)
        
        # F9 - Menu (placeholder)
        menu_btn = ttk.Button(toolbar, text="F9 Menu", command=self.show_menu)
        menu_btn.pack(side=tk.LEFT, padx=2)
        menu_btn.configure(takefocus=False)
        
        # F10 - Quit
        quit_btn = ttk.Button(toolbar, text="F10 Quit", command=self.root.quit)
        quit_btn.pack(side=tk.LEFT, padx=2)
        quit_btn.configure(takefocus=False)
        
        ttk.Separator(toolbar, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=5)
        
        # Additional buttons
        rename_btn = ttk.Button(toolbar, text="Rename", command=self.rename_file)
        rename_btn.pack(side=tk.LEFT, padx=2)
        rename_btn.configure(takefocus=False)
        
        refresh_btn = ttk.Button(toolbar, text="Refresh", command=self.refresh_panels)
        refresh_btn.pack(side=tk.LEFT, padx=2)
        refresh_btn.configure(takefocus=False)
        
        ttk.Separator(toolbar, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=5)
        
        # Hidden files checkbox
        self.hidden_var = tk.BooleanVar(value=self.show_hidden_files)
        hidden_checkbox = ttk.Checkbutton(toolbar, text="Show Hidden", 
                                        variable=self.hidden_var, 
                                        command=self.toggle_hidden_files)
        hidden_checkbox.pack(side=tk.LEFT, padx=2)
        hidden_checkbox.configure(takefocus=False)
        
        ttk.Separator(toolbar, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=5)
        
        # Git buttons
        git_pull_btn = ttk.Button(toolbar, text="Git Pull", command=self.git_pull)
        git_pull_btn.pack(side=tk.LEFT, padx=2)
        git_pull_btn.configure(takefocus=False)
        
        git_commit_btn = ttk.Button(toolbar, text="Git Commit", command=self.git_commit_all)
        git_commit_btn.pack(side=tk.LEFT, padx=2)
        git_commit_btn.configure(takefocus=False)
        
        git_push_btn = ttk.Button(toolbar, text="Git Push", command=self.git_push)
        git_push_btn.pack(side=tk.LEFT, padx=2)
        git_push_btn.configure(takefocus=False)
        
    def bind_f_keys(self):
        """Bind F-key shortcuts like Double Commander"""
        self.root.bind("<F3>", lambda e: self.view_file())
        self.root.bind("<F4>", lambda e: self.edit_file())
        self.root.bind("<F5>", lambda e: self.copy_files())
        self.root.bind("<F6>", lambda e: self.move_files())
        self.root.bind("<F7>", lambda e: self.new_folder())
        self.root.bind("<F8>", lambda e: self.delete_files())
        self.root.bind("<F9>", lambda e: self.show_menu())
        self.root.bind("<F10>", lambda e: self.root.quit())
        
    def view_file(self):
        """F3 - View file (placeholder)"""
        messagebox.showinfo("Info", "F3 View - Not implemented yet")
        
    def edit_file(self):
        """F4 - Edit file (placeholder)"""
        messagebox.showinfo("Info", "F4 Edit - Not implemented yet")
        
    def show_menu(self):
        """F9 - Show menu (placeholder)"""
        messagebox.showinfo("Info", "F9 Menu - Not implemented yet")
        
    def toggle_hidden_files(self):
        """Toggle hidden files display"""
        self.show_hidden_files = self.hidden_var.get()
        self.refresh_panels()
        
    def setup_panel(self, parent, panel_name):
        """Setup a file browser panel"""
        # Directory path
        path_frame = ttk.Frame(parent)
        path_frame.pack(fill=tk.X, padx=5, pady=2)
        
        ttk.Label(path_frame, text="Path:").pack(side=tk.LEFT)
        path_var = tk.StringVar()
        path_entry = ttk.Entry(path_frame, textvariable=path_var, state="readonly")
        path_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(5, 0))
        # Make path entry non-focusable so Tab only switches between panels
        path_entry.configure(takefocus=False)
        
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
        
        # Configure selection appearance
        style = ttk.Style()
        style.configure("Treeview", background="#e1e1e1", foreground="black", fieldbackground="#e1e1e1")
        style.map("Treeview", 
                 background=[('selected', '#0078d4')], 
                 foreground=[('selected', 'white')],
                 focuscolor='none')  # Remove focus outline
        
        # Also configure the treeview directly
        tree.configure(selectmode='browse')  # Single selection mode
        
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
        tree.bind("<Return>", lambda e: self.on_double_click(panel_name))
        tree.bind("<BackSpace>", lambda e: self.go_up_directory(panel_name))
        tree.bind("<Tab>", lambda e: self.switch_panel())
        tree.bind("<Prior>", lambda e: self.page_up(panel_name))  # Page Up
        tree.bind("<Next>", lambda e: self.page_down(panel_name))  # Page Down
        tree.bind("<Home>", lambda e: self.go_to_first(panel_name))  # Home
        tree.bind("<End>", lambda e: self.go_to_last(panel_name))  # End
        
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
        
    def save_selection_position(self, panel_name):
        """Save current selection position for a panel"""
        tree = self.get_tree(panel_name)
        selection = tree.selection()
        if selection:
            children = tree.get_children()
            try:
                index = children.index(selection[0])
                setattr(self, f"{panel_name}_selection_index", index)
            except ValueError:
                pass
                
    def restore_selection_position(self, panel_name):
        """Restore saved selection position for a panel"""
        tree = self.get_tree(panel_name)
        children = tree.get_children()
        if children:
            index = getattr(self, f"{panel_name}_selection_index", 0)
            # Make sure index is within bounds
            index = min(index, len(children) - 1)
            if index >= 0:
                tree.selection_set(children[index])
                tree.focus(children[index])
                tree.see(children[index])
                tree.focus_force()
                
    def clear_inactive_panel_selection(self, panel_name):
        """Clear selection from inactive panel"""
        tree = self.get_tree(panel_name)
        tree.selection_remove(tree.selection())
        
    def force_selection_visible(self, panel_name):
        """Force selection to be visible"""
        tree = self.get_tree(panel_name)
        selection = tree.selection()
        if selection:
            # Force selection to be visible
            tree.selection_set(selection[0])
            tree.focus(selection[0])
            tree.see(selection[0])
            tree.focus_force()
            tree.update_idletasks()
            # Force a redraw
            tree.update()
        
    def page_up(self, panel_name):
        """Move selection up by page and scroll"""
        tree = self.get_tree(panel_name)
        children = tree.get_children()
        if not children:
            return
            
        current_selection = tree.selection()
        if current_selection:
            try:
                current_index = children.index(current_selection[0])
            except ValueError:
                current_index = 0
        else:
            current_index = 0
            
        # Calculate page size (approximate number of visible items)
        tree_height = tree.winfo_height()
        item_height = 20  # Approximate item height
        page_size = max(1, tree_height // item_height - 1)
        
        # If tree height is 0, use a default page size
        if tree_height <= 0:
            page_size = 10
        
        # Move up by page size
        new_index = max(0, current_index - page_size)
        
        # Update selection and scroll
        tree.selection_set(children[new_index])
        tree.focus(children[new_index])
        tree.see(children[new_index])
        tree.focus_force()
        tree.update_idletasks()  # Force UI update
        
        # Save the new position
        setattr(self, f"{panel_name}_selection_index", new_index)
        
    def page_down(self, panel_name):
        """Move selection down by page and scroll"""
        tree = self.get_tree(panel_name)
        children = tree.get_children()
        if not children:
            return
            
        current_selection = tree.selection()
        if current_selection:
            try:
                current_index = children.index(current_selection[0])
            except ValueError:
                current_index = 0
        else:
            current_index = 0
            
        # Calculate page size (approximate number of visible items)
        tree_height = tree.winfo_height()
        item_height = 20  # Approximate item height
        page_size = max(1, tree_height // item_height - 1)
        
        # If tree height is 0, use a default page size
        if tree_height <= 0:
            page_size = 10
        
        # Move down by page size
        new_index = min(len(children) - 1, current_index + page_size)
        
        # Update selection and scroll
        tree.selection_set(children[new_index])
        tree.focus(children[new_index])
        tree.see(children[new_index])
        tree.focus_force()
        tree.update_idletasks()  # Force UI update
        
        # Save the new position
        setattr(self, f"{panel_name}_selection_index", new_index)
        
    def go_to_first(self, panel_name):
        """Move selection to first item"""
        tree = self.get_tree(panel_name)
        children = tree.get_children()
        if children:
            tree.selection_set(children[0])
            tree.focus(children[0])
            tree.see(children[0])
            tree.focus_force()
            setattr(self, f"{panel_name}_selection_index", 0)
            
    def go_to_last(self, panel_name):
        """Move selection to last item"""
        tree = self.get_tree(panel_name)
        children = tree.get_children()
        if children:
            last_index = len(children) - 1
            tree.selection_set(children[last_index])
            tree.focus(children[last_index])
            tree.see(children[last_index])
            tree.focus_force()
            setattr(self, f"{panel_name}_selection_index", last_index)
        
    def on_click(self, panel_name):
        """Handle single click on panel"""
        # Save current panel's selection position if switching panels
        if self.active_panel != panel_name:
            self.save_selection_position(self.active_panel)
            # Clear selection from the panel that's becoming inactive
            self.clear_inactive_panel_selection(self.active_panel)
        
        self.active_panel = panel_name
        # Set focus to the clicked panel
        tree = self.get_tree(panel_name)
        tree.focus_force()
        # Don't restore selection on click - let the user click on items to select them
        
    def on_double_click(self, panel_name):
        """Handle double click on panel"""
        tree = self.get_tree(panel_name)
        selection = tree.selection()
        
        if selection:
            item = selection[0]
            item_text = tree.item(item, "text")
            current_dir = self.get_current_dir(panel_name)
            
            # Remove emoji icon from item text
            if item_text.startswith("📁 ") or item_text.startswith("📄 "):
                item_text = item_text[2:]
            
            # Handle ".." specially - go up one directory
            if item_text == "..":
                self.go_up_directory(panel_name)
                return
            
            new_path = os.path.join(current_dir, item_text)
            
            if os.path.isdir(new_path):
                # Reset selection index when entering a new directory
                setattr(self, f"{panel_name}_selection_index", 0)
                self.set_current_dir(panel_name, new_path)
                self.refresh_panel(panel_name)
                # Set focus to the panel after navigation
                tree.focus_force()
                
    def go_up_directory(self, panel_name):
        """Go up one directory level"""
        current_dir = self.get_current_dir(panel_name)
        parent_dir = os.path.dirname(current_dir)
        if parent_dir != current_dir:  # Not at root
            # Find the folder we're coming from to select it in parent directory
            folder_name = os.path.basename(current_dir)
            
            self.set_current_dir(panel_name, parent_dir)
            self.refresh_panel(panel_name)
            
            # Find and select the folder we just came from
            tree = self.get_tree(panel_name)
            children = tree.get_children()
            for i, child in enumerate(children):
                child_text = tree.item(child, "text")
                # Remove emoji icon from item text
                if child_text.startswith("📁 ") or child_text.startswith("📄 "):
                    child_text = child_text[2:]
                
                if child_text == folder_name:
                    tree.selection_set(child)
                    tree.focus(child)
                    tree.see(child)
                    setattr(self, f"{panel_name}_selection_index", i)
                    break
            
            # Set focus to the panel after navigation
            tree.focus_force()
                
    def switch_panel(self):
        """Switch focus between left and right panels"""
        # Save current panel's selection position
        self.save_selection_position(self.active_panel)
        
        # Clear selection from current panel (it's becoming inactive)
        self.clear_inactive_panel_selection(self.active_panel)
        
        # Switch to the other panel
        if self.active_panel == "left":
            self.active_panel = "right"
        else:
            self.active_panel = "left"
        
        tree = self.get_tree(self.active_panel)
        tree.focus_set()
        # Force focus to stay
        tree.focus_force()
        # Restore the selection position for the new active panel
        self.restore_selection_position(self.active_panel)
                
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
            # Force directory refresh by accessing it
            os.listdir(current_dir)
            
            # Add parent directory entry
            if current_dir != os.path.dirname(current_dir):
                tree.insert("", 0, text="..", values=("", ""))
                
            # Get directory contents with forced refresh
            items = []
            try:
                # Use os.scandir for better performance and to force refresh
                with os.scandir(current_dir) as entries:
                    for entry in entries:
                        item = entry.name
                        # Filter hidden files based on setting
                        if not self.show_hidden_files and item.startswith('.'):
                            continue
                            
                        item_path = entry.path
                        try:
                            stat_info = entry.stat()
                            size = ""
                            if entry.is_file():
                                size = self.format_size(stat_info.st_size)
                            
                            modified = datetime.datetime.fromtimestamp(stat_info.st_mtime).strftime("%Y-%m-%d %H:%M")
                            
                            # Determine if it's a directory
                            is_dir = entry.is_dir()
                            icon = "📁" if is_dir else "📄"
                            
                            items.append((item, size, modified, is_dir, icon))
                        except (OSError, PermissionError):
                            continue
            except OSError:
                # Fallback to os.listdir if scandir fails
                for item in os.listdir(current_dir):
                    # Filter hidden files based on setting
                    if not self.show_hidden_files and item.startswith('.'):
                        continue
                        
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
            
            # Only show selection for the active panel
            children = tree.get_children()
            if children and panel_name == self.active_panel:
                # Try to restore saved position, otherwise use first item
                index = getattr(self, f"{panel_name}_selection_index", 0)
                index = min(index, len(children) - 1)
                if index >= 0:
                    tree.selection_set(children[index])
                    tree.focus(children[index])
                    tree.see(children[index])
                    tree.focus_force()
                    # Force update to ensure selection is visible
                    tree.update_idletasks()
            elif children and panel_name != self.active_panel:
                # Clear selection from inactive panel
                tree.selection_remove(tree.selection())
            
            # Force multiple updates to ensure refresh
            tree.update_idletasks()
            tree.update()
            self.root.update_idletasks()
                
        except (OSError, PermissionError) as e:
            messagebox.showerror("Error", f"Cannot access directory: {e}")
            
    def ensure_selection_visible(self):
        """Ensure selection is visible on the active panel"""
        tree = self.get_tree(self.active_panel)
        children = tree.get_children()
        if children:
            tree.selection_set(children[0])
            tree.focus(children[0])
            tree.see(children[0])
            tree.focus_force()
        
    def refresh_panels(self):
        """Refresh both panels"""
        self.status_var.set("Refreshing panels...")
        self.refresh_panel("left")
        self.refresh_panel("right")
        self.status_var.set("Panels refreshed")
        
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
                
                # Find and select the newly created folder
                tree = self.left_tree if self.active_panel == "left" else self.right_tree
                children = tree.get_children()
                for child in children:
                    item_text = tree.item(child, "text")
                    # Remove emoji prefix if present
                    clean_name = item_text.replace("📁 ", "").replace("📄 ", "")
                    if clean_name == folder_name:
                        tree.selection_set(child)
                        tree.focus(child)
                        tree.see(child)
                        break
                
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
                
                # Find and select the newly created file
                tree = self.left_tree if self.active_panel == "left" else self.right_tree
                children = tree.get_children()
                for child in children:
                    item_text = tree.item(child, "text")
                    # Remove emoji prefix if present
                    clean_name = item_text.replace("📁 ", "").replace("📄 ", "")
                    if clean_name == file_name:
                        tree.selection_set(child)
                        tree.focus(child)
                        tree.see(child)
                        break
                
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
        
    def _repo(self, path: str) -> Repo:
        return Repo(path)
    
    def git_clone(self):
        """Clone a git repository"""
        url = simpledialog.askstring("Git Clone", "Enter repository URL:")
        if url:
            # Use current active panel directory
            current_dir = self.get_current_dir(self.active_panel)
            try:
                porcelain.clone(source=url, target=current_dir)
                messagebox.showinfo("Success", f"Repository cloned successfully!")
                self.refresh_panels()
                self.navigate_to_cloned_repo(current_dir, url)
            except Exception as e:
                messagebox.showerror("Error", f"Clone failed:\n{e}")
    
    def navigate_to_cloned_repo(self, parent_dir, repo_url):
        """Navigate to the cloned repository directory"""
        try:
            # Extract repository name from URL
            repo_name = repo_url.split('/')[-1]
            if repo_name.endswith('.git'):
                repo_name = repo_name[:-4]
            
            # Construct the path to the cloned repository
            repo_path = os.path.join(parent_dir, repo_name)
            
            # Check if the repository directory exists
            if os.path.exists(repo_path):
                # Navigate to the repository directory
                self.navigate_to_directory(self.active_panel, repo_path)
                self.status_var.set(f"Navigated to cloned repository: {repo_name}")
            else:
                self.status_var.set("Repository cloned but directory not found")
        except Exception as e:
            self.status_var.set(f"Error navigating to cloned repository: {e}")
    
    def navigate_to_directory(self, panel_name, directory_path):
        """Navigate to a specific directory"""
        try:
            # Update the path entry
            if panel_name == "left":
                self.left_path_var.set(directory_path)
                self.left_current_dir = directory_path
            else:
                self.right_path_var.set(directory_path)
                self.right_current_dir = directory_path
            
            # Refresh the panel to show the new directory
            self.refresh_panel(panel_name)
            
            # Set focus to the panel
            tree = self.left_tree if panel_name == "left" else self.right_tree
            tree.focus_force()
            
        except Exception as e:
            self.status_var.set(f"Error navigating to directory: {e}")
    
    def git_pull(self):
        """Pull changes from remote repository"""
        current_dir = self.get_current_dir(self.active_panel)
        try:
            porcelain.pull(current_dir, remote_location='origin')
            messagebox.showinfo("Success", "Pull successful!")
            self.refresh_panels()
        except Exception as e:
            messagebox.showerror("Error", f"Pull failed:\n{e}")
    
    def git_commit_all(self):
        """Add all files and commit"""
        current_dir = self.get_current_dir(self.active_panel)
        
        # Get commit message
        message = simpledialog.askstring("Git Commit", "Enter commit message:")
        if not message:
            return
        try:
            porcelain.add(current_dir, paths=['.'])
            porcelain.commit(current_dir, message=message.encode('utf-8'))
            messagebox.showinfo("Success", "Commit successful!")
            self.refresh_panels()
        except Exception as e:
            messagebox.showerror("Error", f"Commit failed:\n{e}")
    
    def git_push(self):
        """Push changes to remote repository"""
        current_dir = self.get_current_dir(self.active_panel)
        try:
            porcelain.push(current_dir, remote_location='origin')
            messagebox.showinfo("Success", "Push successful!")
        except Exception as e:
            messagebox.showerror("Error", f"Push failed:\n{e}")
    
    def git_status(self):
        """Show git status"""
        current_dir = self.get_current_dir(self.active_panel)
        try:
            st = porcelain.status(current_dir)
            # Create a new window to show status
            status_window = tk.Toplevel(self.root)
            status_window.title("Git Status")
            status_window.geometry("600x400")
            
            text_widget = tk.Text(status_window, wrap=tk.WORD)
            text_widget.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
            text = []
            text.append("Changes to be committed:\n" + "\n".join(f"  {p.decode()}" for p in st.staged))
            text.append("\nChanges not staged for commit:\n" + "\n".join(f"  {p.decode()}" for p in st.unstaged))
            text.append("\nUntracked files:\n" + "\n".join(f"  {p.decode()}" for p in st.untracked))
            text_widget.insert(tk.END, "\n".join(text))
            text_widget.config(state=tk.DISABLED)
            
            scrollbar = ttk.Scrollbar(status_window, orient=tk.VERTICAL, command=text_widget.yview)
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            text_widget.configure(yscrollcommand=scrollbar.set)
        except Exception as e:
            messagebox.showerror("Error", f"Status failed:\n{e}")


def main():
    root = tk.Tk()
    app = FileManager(root)
    root.mainloop()


if __name__ == "__main__":
    main()
