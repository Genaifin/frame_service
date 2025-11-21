from logging.config import fileConfig
import os
import sys

from sqlalchemy import engine_from_config, text
from sqlalchemy import pool

from alembic import context

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import our database models and manager
from database_models import Base, DatabaseManager

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
target_metadata = Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    # Use our DatabaseManager to get the connection URL
    db_manager = DatabaseManager()
    
    # Get the connection URL from the engine
    if db_manager.engine:
        url = str(db_manager.engine.url)
    else:
        # Fallback to building the URL manually
        if db_manager.db_type == "postgresql":
            params = db_manager.connection_params
            url = f"postgresql+pg8000://{params['user']}:{params['password']}@{params['host']}:{params['port']}/{params['database']}"
    
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    # Use our DatabaseManager to get the engine
    db_manager = DatabaseManager()
    connectable = db_manager.engine

    with connectable.connect() as connection:
        context.configure(
            connection=connection, 
            target_metadata=target_metadata,
            version_table_schema='public'
        )

        with context.begin_transaction():
            try:
                context.run_migrations()
            except KeyError as e:
                # Handle Alembic merge head tracking errors
                # This is a known issue with Alembic when merging multiple heads
                # The error occurs in Alembic's internal head tracking mechanism
                error_msg = str(e)
                if '38832ba5acac' in error_msg or ('heads' in error_msg.lower() and 'remove' in error_msg.lower()):
                    # This is a known Alembic bug with complex merge scenarios
                    # The migration operations may have succeeded, but head tracking failed
                    print("\n" + "="*70)
                    print("WARNING: Alembic encountered a head tracking error during merge")
                    print("="*70)
                    print(f"Error: {error_msg}")
                    print("\nThis is a known issue with Alembic's internal head tracking.")
                    print("The migration operations may have succeeded despite this error.")
                    print("\nTo resolve this:")
                    print("1. Check if the migration operations completed successfully")
                    print("2. If they did, manually update alembic_version table:")
                    print("   UPDATE alembic_version SET version_num = '63806784dabd';")
                    print("3. Then continue with: alembic upgrade head")
                    print("="*70 + "\n")
                    # Re-raise with a more helpful message
                    raise RuntimeError(
                        f"Alembic head tracking failed during merge: {error_msg}. "
                        "This is a known Alembic issue with complex merges. "
                        "Check the database state - migrations may have succeeded. "
                        "You may need to manually update alembic_version table."
                    ) from e
                else:
                    raise


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
