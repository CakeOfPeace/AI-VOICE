"""
Credential loading helper for agent.py entrypoint.

This module provides functions to load and resolve credentials for the voice pipeline
components. It integrates with the config_api to fetch user credentials and uses
the resolver to determine which credentials to use based on the resolution hierarchy.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional, Tuple

import requests

logger = logging.getLogger(__name__)


def fetch_user_credentials(user_id: str) -> Dict[str, Any]:
    """
    Fetch decrypted credentials and global defaults for a user from the config API.
    
    Args:
        user_id: The user ID (owner_id from agent profile)
        
    Returns:
        Dict containing 'credentials', 'global_defaults', and 'user_role'
    """
    if not user_id:
        return {}
    
    api_base = os.getenv("CONFIG_API_BASE", os.getenv("API_BASE", "http://localhost:5057"))
    internal_key = os.getenv("INTERNAL_API_KEY", "")
    
    try:
        headers = {"X-Internal-Key": internal_key} if internal_key else {}
        r = requests.get(
            f"{api_base}/internal/credentials/{user_id}",
            headers=headers,
            timeout=5
        )
        if r.ok:
            return r.json() or {}
        else:
            logger.warning(f"Failed to fetch credentials for user {user_id}: {r.status_code}")
            return {}
    except Exception as e:
        logger.warning(f"Error fetching credentials for user {user_id}: {e}")
        return {}


def fetch_global_defaults() -> Dict[str, Any]:
    """
    Fetch global defaults from the config API.
    Note: This is now legacy as fetch_user_credentials returns global defaults.
    """
    api_base = os.getenv("CONFIG_API_BASE", os.getenv("API_BASE", "http://localhost:5057"))
    internal_key = os.getenv("INTERNAL_API_KEY", "")
    
    try:
        # Use internal endpoint if possible to avoid 401 on /defaults
        if internal_key:
            # We don't have a direct /internal/defaults, but we can use any user_id
            # or just rely on the load_pipeline_credentials calling fetch_user_credentials
            pass

        # Try public defaults (might fail with 401)
        r = requests.get(f"{api_base}/defaults", timeout=2)
        if r.ok:
            return r.json() or {}
    except Exception:
        pass
    return {}


def load_pipeline_credentials(
    agent_profile: Dict[str, Any],
    global_defaults: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Dict[str, Any]], bool]:
    """
    Load and resolve credentials for the voice pipeline.
    """
    # Get owner info
    owner_id = agent_profile.get("owner_id", "")
    
    # Fetch user data (creds + defaults + role)
    user_data = {}
    if owner_id:
        user_data = fetch_user_credentials(owner_id)
    
    user_credentials = user_data.get("credentials", {})
    user_role = user_data.get("user_role", "user")
    
    # Use global defaults from user_data if not provided
    if global_defaults is None:
        global_defaults = user_data.get("global_defaults")
        if global_defaults is None:
            global_defaults = fetch_global_defaults()
    
    is_admin = user_role in ("admin", "convix")
    
    # Import resolver here to avoid circular imports
    from .resolver import resolve_all_credentials, CredentialResolutionError
    
    try:
        resolved = resolve_all_credentials(
            agent_profile=agent_profile,
            user_credentials=user_credentials,
            global_defaults=global_defaults,
            user_role=user_role,
        )
        return resolved, is_admin
    except CredentialResolutionError as e:
        logger.error(f"Credential resolution failed: {e}")
        raise


def create_pipeline_providers(
    agent_profile: Dict[str, Any],
    global_defaults: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Create all pipeline providers for an agent.
    
    This is a convenience function that combines credential loading and provider creation.
    
    Args:
        agent_profile: The agent profile configuration
        global_defaults: Optional global defaults
        
    Returns:
        Dict with provider instances: {stt: <STT>, llm: <LLM>, tts: <TTS>}
        or for realtime: {realtime: <RealtimeModel>, tts?: <TTS>}
    """
    resolved, is_admin = load_pipeline_credentials(agent_profile, global_defaults)
    
    # Import factory here to avoid circular imports
    from .factory import create_all_providers
    
    return create_all_providers(resolved)


def _get_env_key_from_defaults(global_defaults: Dict[str, Any], key_name: str) -> Optional[str]:
    """
    Get an environment key from defaults, handling both encrypted and plaintext formats.
    
    Checks for:
    1. key_name (plaintext)
    2. key_name_encrypted (encrypted, needs decryption)
    """
    env_section = global_defaults.get("env", {}) or {}
    
    # Check plaintext first
    value = env_section.get(key_name)
    if value:
        return value
    
    # Check encrypted version
    encrypted_key = f"{key_name}_encrypted"
    encrypted_value = env_section.get(encrypted_key)
    if encrypted_value:
        try:
            from encryption import decrypt_value
            decrypted = decrypt_value(encrypted_value)
            if decrypted:
                return decrypted
        except Exception as e:
            logger.warning(f"Failed to decrypt {encrypted_key}: {e}")
    
    return None


def get_legacy_credentials(
    agent_profile: Dict[str, Any],
    global_defaults: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    """
    Get credentials in legacy format for backward compatibility.
    """
    owner_id = agent_profile.get("owner_id", "")
    
    # Fetch user data (creds + defaults + role)
    user_data = {}
    if owner_id:
        user_data = fetch_user_credentials(owner_id)
    
    user_credentials = user_data.get("credentials", {})
    user_role = user_data.get("user_role", "user")
    
    # Use global defaults from user_data if not provided
    if global_defaults is None:
        global_defaults = user_data.get("global_defaults")
        if global_defaults is None:
            global_defaults = fetch_global_defaults()
            
    is_admin = user_role in ("admin", "convix")
    
    result = {}
    
    # OpenAI key and base URL
    openai_key = (
        agent_profile.get("llm", {}).get("api_key") or
        agent_profile.get("stt", {}).get("api_key") or
        user_credentials.get("openai", {}).get("api_key") or
        (global_defaults.get("openai", {}).get("api_key") if is_admin else None) or
        (_get_env_key_from_defaults(global_defaults, "OPENAI_API_KEY") if is_admin else None) or
        (os.getenv("OPENAI_API_KEY") if is_admin else None) or
        ""
    )
    if openai_key:
        result["OPENAI_API_KEY"] = openai_key
        
    openai_base_url = (
        agent_profile.get("llm", {}).get("base_url") or
        agent_profile.get("stt", {}).get("base_url") or
        user_credentials.get("openai", {}).get("base_url") or
        (global_defaults.get("openai", {}).get("base_url") if is_admin else None) or
        (_get_env_key_from_defaults(global_defaults, "OPENAI_BASE_URL") if is_admin else None) or
        (os.getenv("OPENAI_BASE_URL") if is_admin else None) or
        ""
    )
    if openai_base_url:
        result["OPENAI_BASE_URL"] = openai_base_url
    
    # Fish Audio key
    fish_key = (
        agent_profile.get("tts", {}).get("api_key") or
        user_credentials.get("fish", {}).get("api_key") or
        (_get_env_key_from_defaults(global_defaults, "FISH_AUDIO_API_KEY") if is_admin else None) or
        (os.getenv("FISH_AUDIO_API_KEY") if is_admin else None) or
        ""
    )
    if fish_key:
        result["FISH_AUDIO_API_KEY"] = fish_key
    
    # Anthropic key
    anthropic_key = (
        agent_profile.get("llm", {}).get("api_key") if agent_profile.get("llm", {}).get("provider") == "anthropic" else None or
        user_credentials.get("anthropic", {}).get("api_key") or
        (_get_env_key_from_defaults(global_defaults, "ANTHROPIC_API_KEY") if is_admin else None) or
        (os.getenv("ANTHROPIC_API_KEY") if is_admin else None) or
        ""
    )
    if anthropic_key:
        result["ANTHROPIC_API_KEY"] = anthropic_key
    
    # Deepgram key
    deepgram_key = (
        agent_profile.get("stt", {}).get("api_key") if agent_profile.get("stt", {}).get("provider") == "deepgram" else None or
        user_credentials.get("deepgram", {}).get("api_key") or
        (_get_env_key_from_defaults(global_defaults, "DEEPGRAM_API_KEY") if is_admin else None) or
        (os.getenv("DEEPGRAM_API_KEY") if is_admin else None) or
        ""
    )
    if deepgram_key:
        result["DEEPGRAM_API_KEY"] = deepgram_key
    
    # ElevenLabs key
    elevenlabs_key = (
        agent_profile.get("tts", {}).get("api_key") if agent_profile.get("tts", {}).get("provider") == "elevenlabs" else None or
        user_credentials.get("elevenlabs", {}).get("api_key") or
        (_get_env_key_from_defaults(global_defaults, "ELEVENLABS_API_KEY") if is_admin else None) or
        (os.getenv("ELEVENLABS_API_KEY") if is_admin else None) or
        ""
    )
    if elevenlabs_key:
        result["ELEVENLABS_API_KEY"] = elevenlabs_key
    
    return result
