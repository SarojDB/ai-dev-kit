"""Database models for Projects, Conversations, and Messages."""

import uuid
from datetime import datetime, timezone
from typing import Any, List, Optional

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, LargeBinary, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def generate_uuid() -> str:
  return str(uuid.uuid4())


def utc_now() -> datetime:
  return datetime.now(timezone.utc)


class Base(DeclarativeBase):
  """Base class for SQLAlchemy models."""

  pass


class Project(Base):
  """Project model - user-scoped container for conversations."""

  __tablename__ = 'projects'

  id: Mapped[str] = mapped_column(String(50), primary_key=True, default=generate_uuid)
  name: Mapped[str] = mapped_column(String(255), nullable=False)
  user_email: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
  created_at: Mapped[datetime] = mapped_column(
    DateTime(timezone=True), default=utc_now, nullable=False
  )

  # Relationships
  conversations: Mapped[List['Conversation']] = relationship(
    'Conversation', back_populates='project', cascade='all, delete-orphan'
  )

  __table_args__ = (Index('ix_projects_user_created', 'user_email', 'created_at'),)

  def to_dict(self) -> dict[str, Any]:
    """Convert to dictionary."""
    return {
      'id': self.id,
      'name': self.name,
      'user_email': self.user_email,
      'created_at': self.created_at.isoformat() if self.created_at else None,
      'conversation_count': len(self.conversations) if self.conversations else 0,
    }


class Conversation(Base):
  """Conversation model - represents a Claude Code agent session."""

  __tablename__ = 'conversations'

  id: Mapped[str] = mapped_column(String(50), primary_key=True, default=generate_uuid)
  project_id: Mapped[str] = mapped_column(
    String(50), ForeignKey('projects.id', ondelete='CASCADE'), nullable=False
  )
  title: Mapped[str] = mapped_column(String(255), default='New Conversation')
  created_at: Mapped[datetime] = mapped_column(
    DateTime(timezone=True), default=utc_now, nullable=False
  )

  # Claude agent session ID (for resuming sessions)
  session_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

  # Databricks cluster ID for code execution
  cluster_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

  # Default Unity Catalog context
  default_catalog: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
  default_schema: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

  # Relationships
  project: Mapped['Project'] = relationship('Project', back_populates='conversations')
  messages: Mapped[List['Message']] = relationship(
    'Message', back_populates='conversation', cascade='all, delete-orphan'
  )

  __table_args__ = (Index('ix_conversations_project_created', 'project_id', 'created_at'),)

  def to_dict(self) -> dict[str, Any]:
    """Convert to dictionary with messages."""
    return {
      'id': self.id,
      'project_id': self.project_id,
      'title': self.title,
      'created_at': self.created_at.isoformat() if self.created_at else None,
      'session_id': self.session_id,
      'cluster_id': self.cluster_id,
      'default_catalog': self.default_catalog,
      'default_schema': self.default_schema,
      'messages': [m.to_dict() for m in self.messages] if self.messages else [],
    }

  def to_dict_summary(self) -> dict[str, Any]:
    """Convert to dictionary without messages (for list views)."""
    return {
      'id': self.id,
      'project_id': self.project_id,
      'title': self.title,
      'created_at': self.created_at.isoformat() if self.created_at else None,
      'cluster_id': self.cluster_id,
      'default_catalog': self.default_catalog,
      'default_schema': self.default_schema,
      'message_count': len(self.messages) if self.messages else 0,
    }


class Message(Base):
  """Message model - individual chat messages within a conversation."""

  __tablename__ = 'messages'

  id: Mapped[str] = mapped_column(String(50), primary_key=True, default=generate_uuid)
  conversation_id: Mapped[str] = mapped_column(
    String(50), ForeignKey('conversations.id', ondelete='CASCADE'), nullable=False
  )
  role: Mapped[str] = mapped_column(String(20), nullable=False)  # "user" or "assistant"
  content: Mapped[str] = mapped_column(Text, nullable=False)
  timestamp: Mapped[datetime] = mapped_column(
    DateTime(timezone=True), default=utc_now, nullable=False
  )
  is_error: Mapped[bool] = mapped_column(Boolean, default=False)

  # Relationships
  conversation: Mapped['Conversation'] = relationship('Conversation', back_populates='messages')

  __table_args__ = (Index('ix_messages_conversation_timestamp', 'conversation_id', 'timestamp'),)

  def to_dict(self) -> dict[str, Any]:
    """Convert to dictionary."""
    return {
      'id': self.id,
      'conversation_id': self.conversation_id,
      'role': self.role,
      'content': self.content,
      'timestamp': self.timestamp.isoformat() if self.timestamp else None,
      'is_error': self.is_error,
    }


class ProjectBackup(Base):
  """Stores zipped backup of project files for restore after app restart."""

  __tablename__ = 'project_backup'

  project_id: Mapped[str] = mapped_column(
    String(50), ForeignKey('projects.id', ondelete='CASCADE'), primary_key=True
  )
  backup_data: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
  updated_at: Mapped[datetime] = mapped_column(
    DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False
  )
