"""
API key validation helpers for each provider.

These functions make test API calls to verify that provided credentials are valid.

SIMPLIFIED: Only OpenAI, Google/Gemini, Groq, Fish Audio, xAI
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Optional, Tuple

import aiohttp

logger = logging.getLogger(__name__)


async def validate_openai_key(api_key: str, base_url: Optional[str] = None) -> Tuple[bool, str]:
    """
    Validate an OpenAI API key by listing models.
    
    Returns:
        Tuple of (is_valid, message)
    """
    url = (base_url or "https://api.openai.com").rstrip("/") + "/v1/models"
    headers = {"Authorization": f"Bearer {api_key}"}
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    return (True, "OpenAI API key is valid")
                elif resp.status == 401:
                    return (False, "Invalid OpenAI API key")
                elif resp.status == 429:
                    return (True, "OpenAI API key is valid (rate limited)")
                else:
                    text = await resp.text()
                    return (False, f"OpenAI API error: {resp.status} - {text[:100]}")
    except asyncio.TimeoutError:
        return (False, "OpenAI API request timed out")
    except Exception as e:
        return (False, f"OpenAI API connection error: {str(e)}")


async def validate_google_key(api_key: str) -> Tuple[bool, str]:
    """
    Validate a Google/Gemini API key.
    
    Returns:
        Tuple of (is_valid, message)
    """
    # Use Gemini models endpoint to validate
    url = f"https://generativelanguage.googleapis.com/v1beta/models?key={api_key}"
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    return (True, "Google/Gemini API key is valid")
                elif resp.status in (401, 403):
                    return (False, "Invalid Google API key or insufficient permissions")
                else:
                    text = await resp.text()
                    return (False, f"Google API error: {resp.status} - {text[:100]}")
    except asyncio.TimeoutError:
        return (False, "Google API request timed out")
    except Exception as e:
        return (False, f"Google API connection error: {str(e)}")


async def validate_groq_key(api_key: str) -> Tuple[bool, str]:
    """
    Validate a Groq API key by listing models.
    
    Returns:
        Tuple of (is_valid, message)
    """
    url = "https://api.groq.com/openai/v1/models"
    headers = {"Authorization": f"Bearer {api_key}"}
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    return (True, "Groq API key is valid")
                elif resp.status == 401:
                    return (False, "Invalid Groq API key")
                elif resp.status == 429:
                    return (True, "Groq API key is valid (rate limited)")
                else:
                    text = await resp.text()
                    return (False, f"Groq API error: {resp.status} - {text[:100]}")
    except asyncio.TimeoutError:
        return (False, "Groq API request timed out")
    except Exception as e:
        return (False, f"Groq API connection error: {str(e)}")


async def validate_fish_audio_key(api_key: str) -> Tuple[bool, str]:
    """
    Validate a Fish Audio API key.
    
    Returns:
        Tuple of (is_valid, message)
    """
    url = "https://api.fish.audio/v1/tts"
    headers = {"Authorization": f"Bearer {api_key}"}
    
    try:
        async with aiohttp.ClientSession() as session:
            # Just check if the endpoint accepts our auth
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                # Fish Audio returns 405 for GET but with valid auth
                if resp.status in (200, 405):
                    return (True, "Fish Audio API key is valid")
                elif resp.status == 401:
                    return (False, "Invalid Fish Audio API key")
                elif resp.status == 403:
                    return (False, "Fish Audio API key forbidden - check permissions")
                else:
                    return (True, "Fish Audio API key appears valid")
    except asyncio.TimeoutError:
        return (False, "Fish Audio API request timed out")
    except Exception as e:
        return (False, f"Fish Audio API connection error: {str(e)}")


async def validate_xai_key(api_key: str) -> Tuple[bool, str]:
    """
    Validate an xAI API key by listing models.
    
    Returns:
        Tuple of (is_valid, message)
    """
    url = "https://api.x.ai/v1/models"
    headers = {"Authorization": f"Bearer {api_key}"}
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    return (True, "xAI API key is valid")
                elif resp.status == 401:
                    return (False, "Invalid xAI API key")
                elif resp.status == 400:
                    # xAI returns 400 with "Incorrect API key" message for invalid keys
                    text = await resp.text()
                    if "Incorrect API key" in text:
                        return (False, "Invalid xAI API key")
                    return (False, f"xAI API error: {text[:100]}")
                elif resp.status == 429:
                    return (True, "xAI API key is valid (rate limited)")
                else:
                    text = await resp.text()
                    return (False, f"xAI API error: {resp.status} - {text[:100]}")
    except asyncio.TimeoutError:
        return (False, "xAI API request timed out")
    except Exception as e:
        return (False, f"xAI API connection error: {str(e)}")


# Registry of validators (SIMPLIFIED)
VALIDATORS = {
    "openai": validate_openai_key,
    "google": validate_google_key,
    "gemini": validate_google_key,  # Gemini uses same Google API key
    "groq": validate_groq_key,
    "fish": validate_fish_audio_key,
    "xai": validate_xai_key,
}


async def validate_credential(provider: str, api_key: str, **kwargs) -> Tuple[bool, str]:
    """
    Validate a credential for any supported provider.
    
    Args:
        provider: Provider ID (openai, google, groq, fish)
        api_key: The API key to validate
        **kwargs: Additional arguments (e.g., base_url for OpenAI)
        
    Returns:
        Tuple of (is_valid, message)
    """
    provider = provider.lower()
    
    if provider not in VALIDATORS:
        return (False, f"No validator available for provider: {provider}. Supported: openai, google, groq, fish, xai")
    
    validator = VALIDATORS[provider]
    
    # Handle special cases
    if provider == "openai":
        base_url = kwargs.get("base_url")
        return await validator(api_key, base_url)
    else:
        return await validator(api_key)


def validate_credential_sync(provider: str, api_key: str, **kwargs) -> Tuple[bool, str]:
    """
    Synchronous wrapper for validate_credential.
    """
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    return loop.run_until_complete(validate_credential(provider, api_key, **kwargs))
