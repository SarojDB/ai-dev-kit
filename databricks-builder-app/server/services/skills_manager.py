"""Skills manager for copying and managing Databricks skills.

Handles copying skills from the source repository to the app and project directories.
"""

import logging
import os
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)

# Source skills directory (sibling to this app)
SKILLS_SOURCE_DIR = Path(__file__).parent.parent.parent.parent / 'databricks-skills'

# Local cache of skills within this app (copied on startup)
APP_SKILLS_DIR = Path(__file__).parent.parent.parent / 'skills'


def _get_enabled_skills() -> list[str] | None:
  """Get list of enabled skills from environment.

  Returns:
      List of skill names to include, or None to include all skills
  """
  enabled = os.environ.get('ENABLED_SKILLS', '').strip()
  if not enabled:
    return None
  return [s.strip() for s in enabled.split(',') if s.strip()]


def get_available_skills() -> list[dict]:
  """Get list of available skills with their metadata.

  Returns:
      List of dicts with name, description, and path for each skill
  """
  skills = []

  if not APP_SKILLS_DIR.exists():
    logger.warning(f'Skills directory not found: {APP_SKILLS_DIR}')
    return skills

  for skill_dir in APP_SKILLS_DIR.iterdir():
    if not skill_dir.is_dir():
      continue

    skill_md = skill_dir / 'SKILL.md'
    if not skill_md.exists():
      continue

    # Parse frontmatter to get name and description
    try:
      content = skill_md.read_text()
      if content.startswith('---'):
        # Extract YAML frontmatter
        end_idx = content.find('---', 3)
        if end_idx > 0:
          frontmatter = content[3:end_idx].strip()
          name = None
          description = None

          for line in frontmatter.split('\n'):
            if line.startswith('name:'):
              name = line.split(':', 1)[1].strip().strip('"\'')
            elif line.startswith('description:'):
              description = line.split(':', 1)[1].strip().strip('"\'')

          if name:
            skills.append({
              'name': name,
              'description': description or '',
              'path': str(skill_dir),
            })
    except Exception as e:
      logger.warning(f'Failed to parse skill {skill_dir}: {e}')

  return skills


def copy_skills_to_app() -> bool:
  """Copy skills from source repo to app's skills directory.

  Called on server startup to ensure we have the latest skills.
  Only copies skills listed in ENABLED_SKILLS env var (if set).

  Returns:
      True if successful, False otherwise
  """
  if not SKILLS_SOURCE_DIR.exists():
    logger.warning(f'Skills source directory not found: {SKILLS_SOURCE_DIR}')
    return False

  enabled_skills = _get_enabled_skills()
  if enabled_skills:
    logger.info(f'Filtering skills to: {enabled_skills}')

  try:
    # Remove existing skills directory if it exists
    if APP_SKILLS_DIR.exists():
      shutil.rmtree(APP_SKILLS_DIR)

    # Copy skill directories (filtered by ENABLED_SKILLS if set)
    APP_SKILLS_DIR.mkdir(parents=True, exist_ok=True)

    copied_count = 0
    for item in SKILLS_SOURCE_DIR.iterdir():
      if item.is_dir() and (item / 'SKILL.md').exists():
        # Skip if not in enabled list (when list is specified)
        if enabled_skills and item.name not in enabled_skills:
          logger.debug(f'Skipping skill (not enabled): {item.name}')
          continue

        dest = APP_SKILLS_DIR / item.name
        shutil.copytree(item, dest)
        copied_count += 1
        logger.debug(f'Copied skill: {item.name}')

    logger.info(f'Copied {copied_count} skills to {APP_SKILLS_DIR}')
    return True

  except Exception as e:
    logger.error(f'Failed to copy skills: {e}')
    return False


def copy_skills_to_project(project_dir: Path) -> bool:
  """Copy skills to a project's .claude/skills directory.

  Args:
      project_dir: Path to the project directory

  Returns:
      True if successful, False otherwise
  """
  if not APP_SKILLS_DIR.exists():
    logger.warning('App skills directory not found, trying to copy from source')
    copy_skills_to_app()

  if not APP_SKILLS_DIR.exists():
    logger.warning('No skills available to copy')
    return False

  try:
    # Create .claude/skills directory in project
    project_skills_dir = project_dir / '.claude' / 'skills'
    project_skills_dir.mkdir(parents=True, exist_ok=True)

    # Copy all skills
    copied_count = 0
    for skill_dir in APP_SKILLS_DIR.iterdir():
      if skill_dir.is_dir() and (skill_dir / 'SKILL.md').exists():
        dest = project_skills_dir / skill_dir.name
        if dest.exists():
          shutil.rmtree(dest)
        shutil.copytree(skill_dir, dest)
        copied_count += 1

    logger.info(f'Copied {copied_count} skills to project: {project_dir}')
    return True

  except Exception as e:
    logger.error(f'Failed to copy skills to project: {e}')
    return False


def get_skills_summary() -> str:
  """Get a summary of available skills for the system prompt.

  Returns:
      Markdown-formatted summary of skills
  """
  skills = get_available_skills()

  if not skills:
    return ''

  lines = ['## Available Skills', '']
  lines.append('Use the `Skill` tool to invoke these skills for specialized guidance:')
  lines.append('')

  for skill in skills:
    lines.append(f"- **{skill['name']}**: {skill['description']}")

  lines.append('')
  lines.append('To use a skill, invoke it by name (e.g., `skill: "sdp"`).')

  return '\n'.join(lines)
