"""Claude Code agent invocation endpoints.

Handles streaming responses from Claude Code agent sessions.
"""

import json
import logging
from datetime import datetime
from typing import AsyncGenerator, Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from ..services.agent import get_project_directory, stream_agent_response
from ..services.backup_manager import mark_for_backup
from ..services.storage import ConversationStorage, ProjectStorage
from ..services.user import get_current_user, get_current_token, get_workspace_url

logger = logging.getLogger(__name__)
router = APIRouter()


def sse_event(data: dict) -> str:
  """Format data as SSE event."""
  return f'data: {json.dumps(data)}\n\n'


def create_error_stream(error: str, message: str = '') -> StreamingResponse:
  """Create an SSE-compatible error response."""

  async def error_generator():
    yield sse_event({'type': 'error', 'error': error, 'message': message})
    yield 'data: [DONE]\n\n'

  return StreamingResponse(
    error_generator(),
    media_type='text/event-stream',
    headers={
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no',
    },
  )


class InvokeAgentRequest(BaseModel):
  """Request to invoke the Claude Code agent."""

  project_id: str
  conversation_id: Optional[str] = None  # Will create new if not provided
  message: str
  cluster_id: Optional[str] = None  # Databricks cluster for code execution
  default_catalog: Optional[str] = None  # Default Unity Catalog
  default_schema: Optional[str] = None  # Default schema


@router.post('/invoke_agent')
async def invoke_agent(request: Request, body: InvokeAgentRequest):
  """Invoke the Claude Code agent with streaming response.

  Creates a new conversation if conversation_id is not provided.
  Messages are saved to storage after the stream completes.

  Streams events:
    - conversation.created: New conversation ID
    - text: Text content from Claude
    - thinking: Claude's reasoning process
    - tool_use: Tool invocation details
    - tool_result: Tool execution results
    - result: Final result with session_id, cost, etc.
    - error: Error messages
    - stream.completed: Stream finished
  """
  logger.info(
    f'Invoking agent for project: {body.project_id}, conversation: {body.conversation_id}'
  )

  # Get current user and Databricks auth
  user_email = await get_current_user(request)
  user_token = await get_current_token(request)
  workspace_url = get_workspace_url()

  # Verify project exists and belongs to user
  project_storage = ProjectStorage(user_email)
  project = await project_storage.get(body.project_id)
  if not project:
    logger.error(f'Project not found: {body.project_id}')
    return create_error_stream(
      error=f'Project not found: {body.project_id}',
      message='Please verify the project exists',
    )

  # Get or create conversation
  conv_storage = ConversationStorage(user_email, body.project_id)
  conversation_id = body.conversation_id

  if not conversation_id:
    # Create new conversation with auto-title from message
    title = body.message[:50] + ('...' if len(body.message) > 50 else '')
    conversation = await conv_storage.create(title=title)
    conversation_id = conversation.id
    logger.info(f'Created new conversation: {conversation_id}')
  else:
    # Verify conversation exists and get session_id for resumption
    conversation = await conv_storage.get(conversation_id)
    if not conversation:
      logger.error(f'Conversation not found: {conversation_id}')
      return create_error_stream(error=f'Conversation not found: {conversation_id}')

  # Get session_id from conversation for resumption
  session_id = conversation.session_id if conversation else None

  async def stream_and_store() -> AsyncGenerator[str, None]:
    """Stream Claude response and save messages after completion."""
    # Emit conversation_id first so frontend knows which conversation
    yield sse_event({'type': 'conversation.created', 'conversation_id': conversation_id})

    # Collect response data
    final_text = ''
    new_session_id: Optional[str] = None
    error_message: Optional[str] = None

    try:
      # Stream all events from Claude
      async for event in stream_agent_response(
        project_id=body.project_id,
        message=body.message,
        session_id=session_id,
        cluster_id=body.cluster_id,
        default_catalog=body.default_catalog,
        default_schema=body.default_schema,
        databricks_host=workspace_url,
        databricks_token=user_token,
      ):
        event_type = event.get('type', '')

        if event_type == 'text':
          text = event.get('text', '')
          final_text += text
          yield sse_event({'type': 'text', 'text': text})

        elif event_type == 'thinking':
          yield sse_event({
            'type': 'thinking',
            'thinking': event.get('thinking', ''),
          })

        elif event_type == 'tool_use':
          yield sse_event({
            'type': 'tool_use',
            'tool_id': event.get('tool_id', ''),
            'tool_name': event.get('tool_name', ''),
            'tool_input': event.get('tool_input', {}),
          })

        elif event_type == 'tool_result':
          yield sse_event({
            'type': 'tool_result',
            'tool_use_id': event.get('tool_use_id', ''),
            'content': event.get('content', ''),
            'is_error': event.get('is_error', False),
          })

        elif event_type == 'result':
          new_session_id = event.get('session_id')
          yield sse_event({
            'type': 'result',
            'session_id': new_session_id,
            'duration_ms': event.get('duration_ms'),
            'total_cost_usd': event.get('total_cost_usd'),
            'is_error': event.get('is_error', False),
            'num_turns': event.get('num_turns'),
          })

        elif event_type == 'error':
          error_message = event.get('error', 'Unknown error')
          logger.error(f'Agent error received: {error_message}')
          yield sse_event({'type': 'error', 'error': error_message})

        elif event_type == 'system':
          # Extract session_id from init event if not already set
          data = event.get('data')
          if event.get('subtype') == 'init' and data and not new_session_id:
            new_session_id = data.get('session_id')
          yield sse_event({
            'type': 'system',
            'subtype': event.get('subtype', ''),
            'data': data,
          })

    except Exception as e:
      logger.error(f'Error during agent stream: {e}')
      error_message = str(e)
      yield sse_event({'type': 'error', 'error': str(e)})

    # Save messages to storage after stream completes
    try:
      # Save user message
      await conv_storage.add_message(
        conversation_id=conversation_id,
        role='user',
        content=body.message,
      )

      # Save assistant response (or error)
      if final_text or error_message:
        content = final_text if final_text else f'Error: {error_message}'
        is_error = bool(error_message and not final_text)
        logger.info(f'Saving assistant message: {len(content)} chars, is_error={is_error}')
        await conv_storage.add_message(
          conversation_id=conversation_id,
          role='assistant',
          content=content,
          is_error=is_error,
        )
      else:
        logger.warning('No response to save (no text and no error)')

      # Update session_id for conversation resumption
      if new_session_id:
        await conv_storage.update_session_id(conversation_id, new_session_id)

      # Update cluster_id if provided
      if body.cluster_id:
        await conv_storage.update_cluster_id(conversation_id, body.cluster_id)

      # Update catalog/schema if provided
      if body.default_catalog or body.default_schema:
        await conv_storage.update_catalog_schema(
          conversation_id, body.default_catalog, body.default_schema
        )

      logger.info(
        f'Saved messages to conversation {conversation_id}: '
        f'text={len(final_text)} chars, error={error_message is not None}'
      )

      # Mark project for backup (will be processed by backup worker)
      mark_for_backup(body.project_id)

    except Exception as e:
      logger.error(f'Failed to save messages: {e}')

    # Send completion event
    yield sse_event({
      'type': 'stream.completed',
      'is_error': error_message is not None,
    })
    yield 'data: [DONE]\n\n'

  return StreamingResponse(
    stream_and_store(),
    media_type='text/event-stream',
    headers={
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no',
    },
  )


@router.get('/projects/{project_id}/files')
async def list_project_files(request: Request, project_id: str):
  """List files in a project directory."""
  user_email = await get_current_user(request)

  # Verify project exists and belongs to user
  project_storage = ProjectStorage(user_email)
  project = await project_storage.get(project_id)
  if not project:
    raise HTTPException(status_code=404, detail=f'Project {project_id} not found')

  # Get project directory and list files
  project_dir = get_project_directory(project_id)

  files = []
  for path in project_dir.rglob('*'):
    if path.is_file():
      rel_path = path.relative_to(project_dir)
      files.append(
        {
          'path': str(rel_path),
          'name': path.name,
          'size': path.stat().st_size,
          'modified': datetime.fromtimestamp(path.stat().st_mtime).isoformat(),
        }
      )

  return {'project_id': project_id, 'files': files}
