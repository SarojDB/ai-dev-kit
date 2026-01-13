"""Configuration and user info endpoints."""

import logging

from fastapi import APIRouter, Request

from ..db import get_lakebase_project_id, is_postgres_configured, test_database_connection
from ..services.user import get_current_user, get_workspace_url

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get('/me')
async def get_user_info(request: Request):
  """Get current user information and app configuration."""
  user_email = await get_current_user(request)
  workspace_url = get_workspace_url()
  lakebase_configured = is_postgres_configured()
  lakebase_project_id = get_lakebase_project_id()

  # Test database connection if configured
  lakebase_error = None
  if lakebase_configured:
    lakebase_error = await test_database_connection()

  return {
    'user': user_email,
    'workspace_url': workspace_url,
    'lakebase_configured': lakebase_configured,
    'lakebase_project_id': lakebase_project_id,
    'lakebase_error': lakebase_error,
  }


@router.get('/health')
async def health_check():
  """Health check endpoint."""
  return {'status': 'healthy'}
