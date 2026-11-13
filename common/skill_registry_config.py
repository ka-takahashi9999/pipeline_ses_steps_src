"""Single production mode configuration for the skill canonical registry."""

import os


DEFAULT_SKILL_REGISTRY_MODE = "registry_overlay"
SKILL_REGISTRY_MODE_ENV = "SKILL_REGISTRY_MODE"
SKILL_REGISTRY_CONTEXT_ENV = "SKILL_REGISTRY_CONTEXT"
REQUIRED_SEARCH_MODE = "legacy_substring"

REGISTRY_GROUP_COUNT = 65


def configured_mode() -> str:
    """Return the run mode. Environment override is the rollback switch."""
    return os.environ.get(SKILL_REGISTRY_MODE_ENV, DEFAULT_SKILL_REGISTRY_MODE)
