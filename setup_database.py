#!/usr/bin/env python3
"""
Robust database setup script that handles fresh database initialization.
This script ensures proper migration application regardless of database state.
"""
import os
import sys
from database_models import DatabaseManager
from sqlalchemy import text
import subprocess

def check_database_state():
    """Check if database has been properly migrated"""
    db = DatabaseManager()
    session = db.get_session()
    
    try:
        # Check if alembic_version table exists (in public schema)
        result = session.execute(text("SELECT table_name FROM information_schema.tables WHERE table_name = 'alembic_version'"))
        alembic_tables = [row[0] for row in result]
        alembic_exists = "alembic_version" in alembic_tables
        
        if not alembic_exists:
            return "fresh", "No alembic_version table - fresh database"
        
        # Check if migration is applied
        result = session.execute(text("SELECT version_num FROM alembic_version"))
        versions = [row[0] for row in result]
        
        if not versions:
            return "empty", "alembic_version table exists but no migrations applied"
        
        # Check if tables actually exist
        result = session.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"))
        public_tables = [row[0] for row in result]
        
        # If we have migration records but no actual tables, something is wrong
        if len(public_tables) <= 1:  # Only alembic_version table
            return "inconsistent", f"Migration shows {versions} but no tables exist"
        
        return "migrated", f"Database properly migrated with {versions}"
        
    except Exception as e:
        return "error", f"Error checking database state: {e}"
    finally:
        session.close()

def reset_alembic_state():
    """Reset Alembic to base state"""
    print("🔄 Resetting Alembic state...")
    try:
        result = subprocess.run([sys.executable, "-m", "alembic", "stamp", "base"], 
                              capture_output=True, text=True, cwd=os.getcwd())
        if result.returncode == 0:
            print("✅ Alembic state reset to base")
            return True
        else:
            print(f"❌ Failed to reset Alembic state: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Error resetting Alembic state: {e}")
        return False

def apply_migration():
    """Apply the migration"""
    print("🚀 Applying database migration...")
    try:
        result = subprocess.run([sys.executable, "-m", "alembic", "upgrade", "head"], 
                              capture_output=True, text=True, cwd=os.getcwd())
        if result.returncode == 0:
            print("✅ Migration applied successfully")
            return True
        else:
            print(f"❌ Failed to apply migration: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Error applying migration: {e}")
        return False

def setup_database():
    """Main database setup function"""
    print("🗄️ Database Setup Starting...")
    print("=" * 50)
    
    # Check current database state
    state, message = check_database_state()
    print(f"📊 Database State: {state}")
    print(f"   {message}")
    print()
    
    if state == "migrated":
        print("✅ Database is already properly set up!")
        return True
    
    elif state == "fresh":
        print("💫 Fresh database detected - applying migration...")
        return apply_migration()
    
    elif state in ["empty", "inconsistent"]:
        print("🔧 Inconsistent state detected - resetting and reapplying...")
        if reset_alembic_state():
            return apply_migration()
        else:
            return False
    
    elif state == "error":
        print("❌ Error checking database state")
        print("   Please check your database connection and try again")
        return False
    
    return False

def run_seeder():
    """Run the database seeder"""
    print("\n🌱 Running database seeder...")
    try:
        result = subprocess.run([sys.executable, "database_seeder.py"], 
                              capture_output=True, text=True, cwd=os.getcwd())
        if result.returncode == 0:
            print("✅ Database seeding completed successfully")
            return True
        else:
            print(f"❌ Database seeding failed: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Error running seeder: {e}")
        return False

if __name__ == "__main__":
    print("🎯 ValidusBoxes Database Setup")
    print("=" * 50)
    
    # Setup database schema
    if setup_database():
        print("\n📈 Database schema setup completed!")
        
        # Ask if user wants to seed data
        response = input("\n🤔 Would you like to populate the database with initial data? (y/n): ").lower().strip()
        if response in ['y', 'yes']:
            if run_seeder():
                print("\n🎉 Database setup and seeding completed successfully!")
            else:
                print("\n⚠️ Database setup completed but seeding failed")
        else:
            print("\n✅ Database schema ready (no data populated)")
    else:
        print("\n❌ Database setup failed!")
        sys.exit(1)
