"""
Credential resolution for voice pipeline components.

Implements the resolution hierarchy:
1. Agent-specific override (agent.llm.api_key, etc.)
2. User's default credentials for the provider
3. Global defaults (ADMIN ONLY)
4. Environment variables (ADMIN ONLY fallback)
5. Error if no credentials found
"""

from __future__ import annotations

import os
import logging
from typing import Any, Dict, Optional, Tuple

from . import get_provider_info, get_required_fields

logger = logging.getLogger(__name__)


class CredentialResolutionError(Exception):
    """Raised when credentials cannot be resolved for a required provider."""
    pass


def _get_env_key_for_provider(provider: str) -> str:
    """Map provider ID to environment variable name."""
    # SIMPLIFIED: Only OpenAI, Google/Gemini, Groq, Fish, xAI
    env_map = {
        "openai": "OPENAI_API_KEY",
        "google": "GOOGLE_API_KEY",
        "gemini": "GOOGLE_API_KEY",  # Gemini TTS uses Google API key
        "groq": "GROQ_API_KEY",
        "fish": "FISH_AUDIO_API_KEY",
        "xai": "XAI_API_KEY",
    }
    return env_map.get(provider.lower(), f"{provider.upper()}_API_KEY")


def resolve_component_credentials(
    component: str,
    agent_config: Dict[str, Any],
    user_credentials: Dict[str, Dict[str, Any]],
    global_defaults: Dict[str, Any],
    user_role: str,
) -> Dict[str, Any]:
    """
    Resolve credentials for a single pipeline component (stt, llm, or tts).
    
    Args:
        component: Component type ('stt', 'llm', 'tts')
        agent_config: The agent's config section for this component (e.g., agent['stt'])
        user_credentials: User's stored credentials {provider: {api_key_encrypted: ...}}
        global_defaults: Global default settings (admin-only fallback)
        user_role: User's role ('admin', 'subscriber', 'user', etc.)
        
    Returns:
        Dict with resolved credentials including provider, api_key, and other config
        
    Raises:
        CredentialResolutionError: If required credentials cannot be found
    """
    is_admin = user_role in ("admin", "convix")
    
    # Strict: provider MUST be explicitly set
    provider = (agent_config.get("provider") or "").strip().lower()
    if not provider:
        raise CredentialResolutionError(
            f"{component.upper()} provider is not set. "
            f"Please select a {component.upper()} provider in the agent configuration."
        )
    
    # Get provider info and required fields
    provider_info = get_provider_info(component, provider)
    if not provider_info:
        logger.warning(f"Unknown {component} provider: {provider}, using default config")
        provider_info = {"requires": ["api_key"]}
    
    required_fields = provider_info.get("requires", ["api_key"])
    
    # Build resolved config starting with agent config
    resolved = {
        "provider": provider,
        **{k: v for k, v in agent_config.items() if k not in ("api_key", "api_key_encrypted")},
    }
    
    # Resolve each required field
    for field in required_fields:
        value = _resolve_field(
            field=field,
            provider=provider,
            agent_config=agent_config,
            user_credentials=user_credentials,
            global_defaults=global_defaults,
            is_admin=is_admin,
        )
        
        if not value:
            raise CredentialResolutionError(
                f"No {field} found for {component} provider '{provider}'. "
                f"Please configure your {provider_info.get('name', provider)} credentials."
            )
        
        resolved[field] = value
    
    # Also resolve optional fields if they exist in config
    optional_fields = provider_info.get("optional", [])
    for field in optional_fields:
        value = _resolve_field(
            field=field,
            provider=provider,
            agent_config=agent_config,
            user_credentials=user_credentials,
            global_defaults=global_defaults,
            is_admin=is_admin,
        )
        if value:
            resolved[field] = value
    
    return resolved


def _resolve_field(
    field: str,
    provider: str,
    agent_config: Dict[str, Any],
    user_credentials: Dict[str, Dict[str, Any]],
    global_defaults: Dict[str, Any],
    is_admin: bool,
) -> Optional[str]:
    """
    Resolve a single configuration field using the hierarchy.
    
    Priority:
    1. Agent-specific value
    2. User's default for this provider
    3. Global defaults (admin only)
    4. Environment variable (admin only)
    """
    # 1. Agent-specific override
    value = (agent_config.get(field) or "").strip()
    if value:
        logger.debug(f"Using agent-specific {field} for {provider}")
        return value
    
    # 2. User's stored credential for this provider
    provider_creds = user_credentials.get(provider, {}) or {}
    value = (provider_creds.get(field) or "").strip()
    if value:
        logger.debug(f"Using user credential {field} for {provider}")
        return value
    
    # 3. Global defaults (admin only)
    if is_admin:
        # Check provider-specific section in defaults
        provider_defaults = global_defaults.get(provider, {}) or {}
        value = (provider_defaults.get(field) or "").strip()
        if value:
            logger.debug(f"Using global default {field} for {provider}")
            return value
        
        # Check legacy sections (openai section for OpenAI, etc.)
        if provider == "openai" and field == "api_key":
            openai_section = global_defaults.get("openai", {}) or {}
            value = (openai_section.get("api_key") or "").strip()
            if value:
                logger.debug(f"Using global openai section api_key")
                return value
        
        # Check env section in defaults (both plaintext and encrypted)
        env_section = global_defaults.get("env", {}) or {}
        env_key = _get_env_key_for_provider(provider) if field == "api_key" else f"{provider.upper()}_{field.upper()}"
        
        # Check plaintext first
        value = (env_section.get(env_key) or "").strip()
        if value:
            logger.debug(f"Using env section {env_key} from defaults")
            return value
        
        # Check encrypted version
        encrypted_key = f"{env_key}_encrypted"
        encrypted_value = env_section.get(encrypted_key)
        if encrypted_value:
            try:
                from encryption import decrypt_value
                decrypted = decrypt_value(encrypted_value)
                if decrypted:
                    logger.debug(f"Using decrypted env section {encrypted_key} from defaults")
                    return decrypted
            except Exception as e:
                logger.warning(f"Failed to decrypt {encrypted_key}: {e}")
        
        # 4. Environment variable fallback (admin only)
        env_key = _get_env_key_for_provider(provider) if field == "api_key" else f"{provider.upper()}_{field.upper()}"
        value = os.getenv(env_key, "").strip()
        if value:
            logger.debug(f"Using environment variable {env_key}")
            return value
    
    return None


def resolve_all_credentials(
    agent_profile: Dict[str, Any],
    user_credentials: Dict[str, Dict[str, Any]],
    global_defaults: Dict[str, Any],
    user_role: str,
) -> Dict[str, Dict[str, Any]]:
    """
    Resolve credentials for all pipeline components.
    
    Args:
        agent_profile: Full agent profile from storage
        user_credentials: User's stored credentials (already decrypted)
        global_defaults: Global default settings
        user_role: User's role
        
    Returns:
        Dict with resolved credentials for each component:
        {
            'stt': {provider: 'openai', api_key: '...', model: '...'},
            'llm': {provider: 'openai', api_key: '...', model: '...'},
            'tts': {provider: 'fish', api_key: '...', reference_id: '...'},
        }
    """
    result = {}
    
    # Check if using realtime mode (combined STT+LLM+TTS)
    realtime_config = agent_profile.get("realtime", {}) or {}
    use_realtime = bool(realtime_config.get("enabled", False))
    
    if use_realtime:
        # Get realtime provider - check explicit provider first, then infer from model
        realtime_provider = (realtime_config.get("provider") or "").strip().lower()
        
        # If provider not explicitly set, try to infer from model name
        if not realtime_provider:
            realtime_model = (realtime_config.get("model") or "").strip().lower()
            if realtime_model:
                # Infer provider from model name
                if "gpt" in realtime_model or "openai" in realtime_model:
                    realtime_provider = "openai"
                elif "gemini" in realtime_model or "google" in realtime_model:
                    realtime_provider = "google"
                else:
                    # Default to openai for unrecognized models
                    realtime_provider = "openai"
                logger.info(f"Inferred realtime provider '{realtime_provider}' from model '{realtime_model}'")
        
        if not realtime_provider:
            raise CredentialResolutionError(
                "Realtime provider is not set. Please select a realtime provider (OpenAI or Google) in the agent configuration, "
                "or set a model name (e.g., 'gpt-realtime' or 'gemini-2.0-flash-live')."
            )
        
        # Resolve realtime provider credentials
        # Ensure provider is set in the config for resolve_component_credentials
        realtime_config_with_provider = dict(realtime_config)
        realtime_config_with_provider["provider"] = realtime_provider
        
        realtime_resolved = resolve_component_credentials(
            component="realtime",
            agent_config=realtime_config_with_provider,
            user_credentials=user_credentials,
            global_defaults=global_defaults,
            user_role=user_role,
        )
        result["realtime"] = realtime_resolved
        
        # In realtime mode, STT/LLM are handled by realtime, but TTS can be Fish Audio
        use_separate_tts = bool(realtime_config.get("use_separate_tts", False))
        
        if use_separate_tts:
            tts_config = agent_profile.get("tts", {}) or {}
            
            # Get TTS provider - check both tts.provider AND realtime.tts_provider (UI sets the latter)
            tts_provider = (tts_config.get("provider") or "").strip().lower()
            if not tts_provider:
                tts_provider = (realtime_config.get("tts_provider") or "").strip().lower()
            
            if not tts_provider:
                raise CredentialResolutionError(
                    "Separate TTS is enabled but TTS provider is not set. "
                    "Please select a TTS provider in the agent configuration."
                )
            
            # Build config with the resolved provider
            tts_config = dict(tts_config)
            tts_config["provider"] = tts_provider
            
            result["tts"] = resolve_component_credentials(
                component="tts",
                agent_config=tts_config,
                user_credentials=user_credentials,
                global_defaults=global_defaults,
                user_role=user_role,
            )
    else:
        # Standard pipeline mode - resolve each component separately
        for component in ("stt", "llm", "tts"):
            config = agent_profile.get(component, {}) or {}
            
            # Strict: provider MUST be set
            provider = (config.get("provider") or "").strip().lower()
            if not provider:
                raise CredentialResolutionError(
                    f"{component.upper()} provider is not set. "
                    f"Please select a {component.upper()} provider in the agent configuration."
                )
            
            result[component] = resolve_component_credentials(
                component=component,
                agent_config=config,
                user_credentials=user_credentials,
                global_defaults=global_defaults,
                user_role=user_role,
            )
    
    return result


def validate_credentials_available(
    agent_profile: Dict[str, Any],
    user_credentials: Dict[str, Dict[str, Any]],
    global_defaults: Dict[str, Any],
    user_role: str,
) -> Tuple[bool, Dict[str, str]]:
    """
    Check if all required credentials are available without raising exceptions.
    Strict mode: no fallbacks, providers and keys must be explicitly set.
    
    Returns:
        Tuple of (is_valid, errors_dict)
        - is_valid: True if all credentials can be resolved
        - errors_dict: {component: error_message} for any missing credentials
    """
    errors = {}
    
    realtime_config = agent_profile.get("realtime", {}) or {}
    use_realtime = bool(realtime_config.get("enabled", False))
    
    if use_realtime:
        # Validate realtime provider is set or can be inferred from model
        realtime_provider = (realtime_config.get("provider") or "").strip().lower()
        
        # If provider not explicitly set, try to infer from model name
        if not realtime_provider:
            realtime_model = (realtime_config.get("model") or "").strip().lower()
            if realtime_model:
                if "gpt" in realtime_model or "openai" in realtime_model:
                    realtime_provider = "openai"
                elif "gemini" in realtime_model or "google" in realtime_model:
                    realtime_provider = "google"
                else:
                    realtime_provider = "openai"  # Default fallback
        
        if not realtime_provider:
            errors["realtime"] = "Realtime provider is not set. Please select a realtime provider (OpenAI or Google) or set a model name."
        else:
            realtime_config_with_provider = dict(realtime_config)
            realtime_config_with_provider["provider"] = realtime_provider
            try:
                resolve_component_credentials(
                    component="realtime",
                    agent_config=realtime_config_with_provider,
                    user_credentials=user_credentials,
                    global_defaults=global_defaults,
                    user_role=user_role,
                )
            except CredentialResolutionError as e:
                errors["realtime"] = str(e)
        
        # Check separate TTS (Fish Audio) if enabled
        use_separate_tts = bool(realtime_config.get("use_separate_tts", False))
        
        if use_separate_tts:
            tts_config = agent_profile.get("tts", {}) or {}
            tts_provider = (tts_config.get("provider") or "").strip().lower()
            if not tts_provider:
                tts_provider = (realtime_config.get("tts_provider") or "").strip().lower()
            
            if not tts_provider:
                errors["tts"] = "Separate TTS is enabled but TTS provider is not set. Please select a TTS provider."
            else:
                tts_config = dict(tts_config)
                tts_config["provider"] = tts_provider
                try:
                    resolve_component_credentials(
                        component="tts",
                        agent_config=tts_config,
                        user_credentials=user_credentials,
                        global_defaults=global_defaults,
                        user_role=user_role,
                    )
                except CredentialResolutionError as e:
                    errors["tts"] = str(e)
    else:
        # Standard mode - check all components strictly
        for component in ("stt", "llm", "tts"):
            config = agent_profile.get(component, {}) or {}
            provider = (config.get("provider") or "").strip().lower()
            
            if not provider:
                errors[component] = f"{component.upper()} provider is not set. Please select a {component.upper()} provider."
            else:
                try:
                    resolve_component_credentials(
                        component=component,
                        agent_config=config,
                        user_credentials=user_credentials,
                        global_defaults=global_defaults,
                        user_role=user_role,
                    )
                except CredentialResolutionError as e:
                    errors[component] = str(e)
    
    return (len(errors) == 0, errors)
