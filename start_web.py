#!/usr/bin/env python3
"""
Start the Azure SQL Database Web Interface
"""

import os
import sys
from pathlib import Path

# Add current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from azure_sql_web import app

if __name__ == '__main__':
    print("Starting Azure SQL Database Web Interface...")
    print("Open your browser and go to: http://localhost:5000")
    print("Press Ctrl+C to stop the server")
    
    app.run(debug=True, host='0.0.0.0', port=5000)
