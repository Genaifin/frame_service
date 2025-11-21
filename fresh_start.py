#!/usr/bin/env python3
"""
Simple Fresh Start Script
Use this whenever you need to start with a clean database
"""

from database_models import DatabaseManager, Base
from sqlalchemy import text

def fresh_start():
    """Quick fresh start - drops everything and recreates"""
    print("🔄 Starting fresh database setup...")
    
    db = DatabaseManager()
    
    with db.engine.connect() as conn:
        # Drop and recreate schemas
        conn.execute(text("DROP SCHEMA IF EXISTS nexbridge CASCADE"))
        conn.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
        conn.execute(text("CREATE SCHEMA nexbridge"))
        conn.commit()
        print("✓ Schemas reset")
    
    # Create all tables
    Base.metadata.create_all(db.engine)
    print("✓ All tables created")
    
    # Set Alembic version
    with db.engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS alembic_version (
                version_num VARCHAR(32) NOT NULL,
                CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
            )
        """))
        conn.execute(text("DELETE FROM alembic_version"))
        conn.execute(text("INSERT INTO alembic_version (version_num) VALUES ('e34c1d90804b')"))
        conn.commit()
        print("✓ Alembic version set")
    
    # Verify
    with db.engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema IN ('public', 'nexbridge')
        """))
        count = result.scalar()
        print(f"✓ {count} tables created")
    
    print("✅ Fresh start complete! Run: python database_seeder.py")

if __name__ == "__main__":
    fresh_start()
