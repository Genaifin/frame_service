#!/usr/bin/env python3
"""
Quick start script for running ValidusBoxes without Docker
Usage: python start_server.py
"""

import os
import sys
import subprocess
from pathlib import Path
from dotenv import load_dotenv

def check_admin_privileges():
    """Check if the script is running with administrator privileges"""
    try:
        if os.name == 'nt':  # Windows
            import ctypes
            return ctypes.windll.shell32.IsUserAnAdmin()
        else:  # Unix/Linux
            return os.geteuid() == 0
    except:
        return False

def check_requirements():
    """Check if all requirements are met"""
    print("🔍 Checking requirements...")
    
    # Check for administrator privileges (required for port 80)
    if not check_admin_privileges():
        print("❌ Administrator privileges required to run on port 80")
        print("💡 Solutions:")
        print("   1. Run this script as administrator")
        print("   2. Use a different port (e.g., 8000) by editing .env file")
        print("   3. Use nginx as reverse proxy on port 80")
        return False
    
    # Check Python version
    if sys.version_info < (3, 11):
        print("❌ Python 3.11+ required. Current version:", sys.version)
        return False
    
    # Check if virtual environment exists
    venv_path = Path("venv")
    if not venv_path.exists():
        print("⚠️  Virtual environment not found. Please create one with:")
        print("   python -m venv venv")
        print("   Then activate it and run: pip install -r requirements.txt")
        return False
    
    # Check .env file
    env_path = Path(".env")
    if not env_path.exists():
        print("⚠️  .env file not found. Creating default configuration...")
        create_env_file()
    
    return True

def create_env_file():
    """Create a default .env file for non-Docker deployment"""
    env_content = """# Server Configuration for Non-Docker Deployment
HOST=0.0.0.0
PORT=8000
SERVER_DOMAIN=dev.aithonsolutions.com
SERVER_IP=34.237.142.231

# Database Configuration for Local PostgreSQL
DB_TYPE=postgresql
DB_HOST=localhost
DB_PORT=5432
DB_NAME=validus_boxes
DB_USER=postgres
DB_PASSWORD=

# Application Configuration
BASE_PATH=.
TEMP_DIR=./temp
OUTPUT_DIR=./output_documents
BACKEND_OUTPUT_DIR=./data/frameDemo/l1
ENABLE_BACKEND_OUTPUT=true

# CORS Configuration
ALLOWED_ORIGINS=*
CORS_CREDENTIALS=true

# Logging Configuration
LOG_LEVEL=INFO
"""
    
    with open(".env", "w") as f:
        f.write(env_content)
    
    print("✅ Created .env file with default configuration")
    print("⚠️  Please update database credentials in .env file")

def create_directories():
    """Create required directories"""
    print("📁 Creating required directories...")
    
    directories = [
        "temp",
        "output_documents", 
        "data/frameDemo/l0",
        "data/frameDemo/l1",
        "queue"
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
    
    print("✅ Directories created")

def check_database():
    """Check database connection"""
    print("🗄️  Checking database connection...")
    
    try:
        # Try to import database models to test connection
        from database_models import DatabaseManager
        
        db_manager = DatabaseManager()
        print("✅ Database connection successful")
        return True
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print("📋 Please ensure:")
        print("   1. PostgreSQL is installed and running")
        print("   2. Database credentials in .env are correct")
        print("   3. Database 'validus_boxes' exists")
        return False

def run_migrations():
    """Run database migrations"""
    print("🔄 Running database migrations...")
    
    try:
        result = subprocess.run(["alembic", "upgrade", "head"], 
                              capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Database migrations completed")
            return True
        else:
            print(f"❌ Migration failed: {result.stderr}")
            return False
            
    except FileNotFoundError:
        print("⚠️  Alembic not found. Installing...")
        subprocess.run([sys.executable, "-m", "pip", "install", "alembic"])
        return run_migrations()

def start_server():
    """Start the FastAPI server"""
    print("🚀 Starting ValidusBoxes server...")
    print(f"📍 Server will be accessible at:")
    print(f"   - Local: http://localhost:8000")
    print(f"   - External: http://dev.aithonsolutions.com")
    print(f"   - API Docs: http://dev.aithonsolutions.com/docs")
    print()
    print("Press Ctrl+C to stop the server")
    print("-" * 50)
    
    try:
        # Use uvicorn to start the server
        cmd = [
            sys.executable, "-m", "uvicorn", 
            "server.APIServer:app",
            "--host", "0.0.0.0",
            "--port", "8000",
            "--reload",
            "--log-config", "uvicornLogConfig.yaml"
        ]
        
        subprocess.run(cmd)
        
    except KeyboardInterrupt:
        print("\n🛑 Server stopped")
    except Exception as e:
        print(f"❌ Failed to start server: {e}")

def main():
    """Main function"""
    print("🎯 ValidusBoxes Quick Start (Non-Docker)")
    print("=" * 40)
    
    # Load environment variables
    load_dotenv()
    
    # Check requirements
    if not check_requirements():
        sys.exit(1)
    
    # Create directories
    create_directories()
    
    # Check database
    if not check_database():
        print("⚠️  Database issues detected. Server may not work properly.")
        response = input("Continue anyway? (y/N): ")
        if response.lower() != 'y':
            sys.exit(1)
    
    # Run migrations
    if not run_migrations():
        print("⚠️  Migration issues detected. Server may not work properly.")
        print("   Continuing anyway...")
    
    # Start server
    start_server()

if __name__ == "__main__":
    main()
