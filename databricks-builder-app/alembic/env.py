"""Alembic environment configuration for database migrations.

Uses sync psycopg2 driver for migrations (simpler and avoids async event loop issues).
Runtime database access uses async asyncpg driver.
"""

import os
from logging.config import fileConfig

from alembic import context
from dotenv import load_dotenv
from sqlalchemy import create_engine, pool

# Load environment variables from .env.local
load_dotenv('.env.local')

# Import models for autogenerate support
from server.db.models import Base

# this is the Alembic Config object
config = context.config

# Setup logging from alembic.ini
if config.config_file_name is not None:
  fileConfig(config.config_file_name)

# Target metadata for autogenerate
target_metadata = Base.metadata


def get_url():
  """Get database URL from environment.

  Returns standard PostgreSQL URL (uses sync psycopg2 driver for migrations).
  """
  url = os.environ.get('LAKEBASE_PG_URL')
  if not url:
    raise ValueError('LAKEBASE_PG_URL environment variable not set')

  # Ensure URL uses sync driver (psycopg2) for migrations
  # Remove asyncpg if present
  if url.startswith('postgresql+asyncpg://'):
    url = url.replace('postgresql+asyncpg://', 'postgresql://', 1)

  return url


def run_migrations_offline():
  """Run migrations in 'offline' mode.

  This configures the context with just a URL
  and not an Engine, though an Engine is acceptable
  here as well. By skipping the Engine creation
  we don't even need a DBAPI to be available.

  Calls to context.execute() here emit the given string to the
  script output.
  """
  url = get_url()
  context.configure(
    url=url,
    target_metadata=target_metadata,
    literal_binds=True,
    dialect_opts={'paramstyle': 'named'},
  )

  with context.begin_transaction():
    context.run_migrations()


def run_migrations_online():
  """Run migrations in 'online' mode using sync engine."""
  url = get_url()

  connectable = create_engine(
    url,
    poolclass=pool.NullPool,
  )

  with connectable.connect() as connection:
    context.configure(
      connection=connection,
      target_metadata=target_metadata,
    )

    with context.begin_transaction():
      context.run_migrations()

  connectable.dispose()


if context.is_offline_mode():
  run_migrations_offline()
else:
  run_migrations_online()
