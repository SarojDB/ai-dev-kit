"""Async database connection and session management.

Uses PostgreSQL via Lakebase with async SQLAlchemy and asyncpg driver.
"""

import os
import ssl
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from sqlalchemy.ext.asyncio import (
  AsyncEngine,
  AsyncSession,
  async_sessionmaker,
  create_async_engine,
)

from .models import Base

# Global engine and session factory
_engine: Optional[AsyncEngine] = None
_async_session_maker: Optional[async_sessionmaker[AsyncSession]] = None


def get_database_url() -> Optional[str]:
  """Get database URL from environment.

  Converts standard PostgreSQL URL to async format if needed.

  Returns:
      Database URL string or None if not configured
  """
  url = os.environ.get('LAKEBASE_PG_URL')
  if url and url.startswith('postgresql://'):
    url = url.replace('postgresql://', 'postgresql+asyncpg://', 1)
  return url


def _prepare_async_url(url: str) -> tuple[str, dict]:
  """Prepare URL for asyncpg driver.

  asyncpg doesn't support sslmode in URL query params like psycopg2 does.
  This function extracts sslmode and converts it to connect_args.

  Args:
      url: Database URL (may contain sslmode parameter)

  Returns:
      Tuple of (cleaned_url, connect_args)
  """
  if url.startswith('postgresql://'):
    url = url.replace('postgresql://', 'postgresql+asyncpg://', 1)

  parsed = urlparse(url)
  query_params = parse_qs(parsed.query)
  connect_args = {}

  if 'sslmode' in query_params:
    sslmode = query_params.pop('sslmode')[0]
    if sslmode in ('require', 'verify-ca', 'verify-full'):
      ssl_context = ssl.create_default_context()
      if sslmode == 'require':
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
      connect_args['ssl'] = ssl_context
    elif sslmode == 'prefer':
      connect_args['ssl'] = 'prefer'

  new_query = urlencode(query_params, doseq=True)
  cleaned_url = urlunparse(parsed._replace(query=new_query))

  return cleaned_url, connect_args


def init_database(database_url: Optional[str] = None) -> AsyncEngine:
  """Initialize async database connection.

  Args:
      database_url: Optional database URL. If not provided, reads from LAKEBASE_PG_URL

  Returns:
      SQLAlchemy AsyncEngine instance

  Raises:
      ValueError: If no database URL is available
  """
  global _engine, _async_session_maker

  url = database_url or get_database_url()
  if not url:
    raise ValueError('No database URL provided. Set LAKEBASE_PG_URL environment variable.')

  url, connect_args = _prepare_async_url(url)

  _engine = create_async_engine(
    url,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,
    pool_recycle=3600,
    echo=False,
    connect_args=connect_args,
  )

  _async_session_maker = async_sessionmaker(
    _engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
  )

  return _engine


def get_engine() -> AsyncEngine:
  """Get the database engine, initializing if needed."""
  global _engine
  if _engine is None:
    init_database()
  return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
  """Get the async session factory, initializing if needed."""
  global _async_session_maker
  if _async_session_maker is None:
    init_database()
  return _async_session_maker


async def get_session() -> AsyncSession:
  """Create a new async database session."""
  factory = get_session_factory()
  return factory()


@asynccontextmanager
async def session_scope() -> AsyncGenerator[AsyncSession, None]:
  """Provide a transactional scope around a series of operations.

  Yields:
      SQLAlchemy AsyncSession instance

  Example:
      async with session_scope() as session:
          result = await session.execute(select(Model))
  """
  session = await get_session()
  try:
    yield session
    await session.commit()
  except Exception:
    await session.rollback()
    raise
  finally:
    await session.close()


async def create_tables():
  """Create all database tables asynchronously.

  For production, use Alembic migrations instead.
  """
  engine = get_engine()
  async with engine.begin() as conn:
    await conn.run_sync(Base.metadata.create_all)


def is_postgres_configured() -> bool:
  """Check if PostgreSQL is configured."""
  return bool(os.environ.get('LAKEBASE_PG_URL'))


def get_lakebase_project_id() -> Optional[str]:
  """Get Lakebase project ID from environment."""
  return os.environ.get('LAKEBASE_PROJECT_ID') or None


async def test_database_connection() -> Optional[str]:
  """Test database connection and return error message if failed.

  Returns:
      None if connection is successful, error message string if failed
  """
  if not is_postgres_configured():
    return None

  try:
    from sqlalchemy import text

    if _engine is None:
      init_database()

    async with _engine.connect() as conn:
      await conn.execute(text('SELECT 1'))

    return None
  except Exception as e:
    return str(e)


def run_migrations() -> None:
  """Run Alembic migrations programmatically.

  Safe to run multiple times - Alembic tracks applied migrations.
  """
  if not is_postgres_configured():
    return

  import logging

  from alembic import command
  from alembic.config import Config

  logger = logging.getLogger(__name__)
  logger.info('Running database migrations...')

  try:
    alembic_cfg = Config('alembic.ini')
    command.upgrade(alembic_cfg, 'head')
    logger.info('Database migrations completed')
  except Exception as e:
    logger.error(f'Migration failed: {e}')
    raise
