"""
Provider factory for instantiating voice pipeline components.

Creates STT, LLM, and TTS provider instances based on resolved configuration.

SIMPLIFIED: Only OpenAI, Google/Gemini, Groq, and Fish Audio
"""

from __future__ import annotations

import importlib
import logging
import os
from typing import Any, Dict, Optional

from . import get_provider_info

logger = logging.getLogger(__name__)


class ProviderCreationError(Exception):
    """Raised when a provider cannot be instantiated."""
    pass


def _import_plugin_class(plugin_path: str, class_name: str):
    """
    Dynamically import a class from a plugin module.
    
    Args:
        plugin_path: Module path (e.g., 'livekit.plugins.openai')
        class_name: Class name to import (e.g., 'STT')
        
    Returns:
        The imported class
    """
    try:
        module = importlib.import_module(plugin_path)
        return getattr(module, class_name)
    except (ImportError, AttributeError) as e:
        raise ProviderCreationError(f"Failed to import {class_name} from {plugin_path}: {e}")


def create_stt(config: Dict[str, Any]):
    """
    Create an STT (Speech-to-Text) provider instance.
    
    Args:
        config: Resolved configuration with provider, api_key, model, etc.
        
    Returns:
        STT provider instance
    """
    provider = config.get("provider", "openai").lower()
    provider_info = get_provider_info("stt", provider)
    
    if not provider_info:
        raise ProviderCreationError(f"Unknown STT provider: {provider}. Supported: openai, google, groq")
    
    api_key = config.get("api_key")
    if not api_key:
        raise ProviderCreationError(f"No API key provided for STT provider {provider}")
    
    model = config.get("model")
    
    # Provider-specific instantiation
    if provider == "openai":
        from livekit.plugins import openai
        
        base_url = config.get("base_url")
        use_realtime = config.get("use_realtime", True)
        detect_language = config.get("detect_language", True)
        language = config.get("language")
        
        # Set API key in environment for the plugin
        os.environ["OPENAI_API_KEY"] = api_key
        
        if base_url:
            return openai.STT.with_base_url(
                model=model or "gpt-4o-mini-transcribe",
                base_url=base_url,
                api_key=api_key,
                use_realtime=use_realtime,
                detect_language=detect_language,
                language=language or "auto",
            )
        else:
            return openai.STT(
                model=model or "gpt-4o-mini-transcribe",
                use_realtime=use_realtime,
                detect_language=detect_language,
                language=language or "auto",
            )
    
    elif provider == "google":
        from livekit.plugins import google
        os.environ["GOOGLE_API_KEY"] = api_key
        return google.STT(
            model=model or "chirp",
            location=config.get("location"),
        )
    
    elif provider == "groq":
        from livekit.plugins import groq
        os.environ["GROQ_API_KEY"] = api_key
        return groq.STT(
            model=model or "whisper-large-v3-turbo",
            language=config.get("language"),
        )
    
    else:
        raise ProviderCreationError(f"Unsupported STT provider: {provider}. Supported: openai, google, groq")


def create_llm(config: Dict[str, Any]):
    """
    Create an LLM (Large Language Model) provider instance.
    
    Args:
        config: Resolved configuration with provider, api_key, model, etc.
        
    Returns:
        LLM provider instance
    """
    provider = config.get("provider", "openai").lower()
    provider_info = get_provider_info("llm", provider)
    
    if not provider_info:
        raise ProviderCreationError(f"Unknown LLM provider: {provider}. Supported: openai, google, groq")
    
    api_key = config.get("api_key")
    if not api_key:
        raise ProviderCreationError(f"No API key provided for LLM provider {provider}")
    
    model = config.get("model")
    
    # Provider-specific instantiation
    if provider == "openai":
        from livekit.plugins import openai
        
        os.environ["OPENAI_API_KEY"] = api_key
        
        base_url = config.get("base_url")
        temperature = config.get("temperature")
        
        kwargs = {"model": model or "gpt-4o-mini"}
        if temperature is not None:
            kwargs["temperature"] = float(temperature)
        
        if base_url:
            return openai.LLM.with_base_url(base_url=base_url, api_key=api_key, **kwargs)
        else:
            return openai.LLM(**kwargs)
    
    elif provider == "google":
        from livekit.plugins import google
        
        os.environ["GOOGLE_API_KEY"] = api_key
        
        temperature = config.get("temperature")
        kwargs = {"model": model or "gemini-2.5-flash"}
        if temperature is not None:
            kwargs["temperature"] = float(temperature)
        
        return google.LLM(**kwargs)
    
    elif provider == "groq":
        from livekit.plugins import groq
        
        os.environ["GROQ_API_KEY"] = api_key
        
        temperature = config.get("temperature")
        kwargs = {"model": model or "meta-llama/llama-3.3-70b-versatile"}
        if temperature is not None:
            kwargs["temperature"] = float(temperature)
        
        return groq.LLM(**kwargs)
    
    else:
        raise ProviderCreationError(f"Unsupported LLM provider: {provider}. Supported: openai, google, groq")


def create_tts(config: Dict[str, Any]):
    """
    Create a TTS (Text-to-Speech) provider instance.
    
    Args:
        config: Resolved configuration with provider, api_key, voice settings, etc.
        
    Returns:
        TTS provider instance
    """
    provider = config.get("provider", "fish").lower()
    provider_info = get_provider_info("tts", provider)
    
    if not provider_info:
        raise ProviderCreationError(f"Unknown TTS provider: {provider}. Supported: fish, openai, gemini, groq")
    
    api_key = config.get("api_key")
    if not api_key:
        raise ProviderCreationError(f"No API key provided for TTS provider {provider}")
    
    model = config.get("model")
    
    # Provider-specific instantiation
    if provider == "fish":
        # Custom Fish Audio TTS implementation
        from fish_tts_provider import FishSpeechTTS
        
        os.environ["FISH_AUDIO_API_KEY"] = api_key
        
        tts = FishSpeechTTS(
            api_key=api_key,
            reference_id=config.get("reference_id") or config.get("voice_id"),
            model=model or "s1",
            response_format=config.get("response_format", "wav"),
            latency=config.get("latency", "low"),
            temperature=float(config.get("temperature", 0.7)),
            top_p=float(config.get("top_p", 0.7)),
            speed=float(config.get("speed", 1.0)),
            volume_db=float(config.get("volume_db", 0.0)),
            emotion_prefix=config.get("emotion_prefix"),
            frame_size_ms=int(config.get("frame_size_ms", 20)),
        )
        tts.prewarm()
        return tts
    
    elif provider == "openai":
        from livekit.plugins import openai
        
        os.environ["OPENAI_API_KEY"] = api_key
        
        voice = config.get("voice") or config.get("voice_id") or "alloy"
        base_url = config.get("base_url")
        
        kwargs = {
            "model": model or "tts-1",
            "voice": voice,
        }
        
        if base_url:
            return openai.TTS.with_base_url(base_url=base_url, api_key=api_key, **kwargs)
        else:
            return openai.TTS(**kwargs)
    
    elif provider == "gemini":
        from livekit.plugins.google import beta as google_beta
        
        os.environ["GOOGLE_API_KEY"] = api_key
        
        voice_name = config.get("voice_name") or config.get("voice") or config.get("voice_id") or "Kore"
        instructions = config.get("instructions") or ""
        
        return google_beta.GeminiTTS(
            model=model or "gemini-2.5-flash-preview-tts",
            voice_name=voice_name,
            instructions=instructions,
        )
    
    elif provider == "groq":
        from livekit.plugins import groq
        
        os.environ["GROQ_API_KEY"] = api_key
        
        voice = config.get("voice") or config.get("voice_id") or "autumn"
        
        return groq.TTS(
            model=model or "canopylabs/orpheus-v1-english",
            voice=voice,
        )
    
    else:
        raise ProviderCreationError(f"Unsupported TTS provider: {provider}. Supported: fish, openai, gemini, groq")


def create_realtime_model(config: Dict[str, Any], use_separate_tts: bool = False):
    """
    Create a Realtime model instance.
    
    Args:
        config: Resolved configuration with provider, api_key, model, voice, etc.
        use_separate_tts: If True, configure model for text-only output (no audio)
                         so a separate TTS (Fish Audio) can handle speech synthesis.
        
    Returns:
        RealtimeModel instance
    """
    provider = config.get("provider", "openai").lower()
    
    api_key = config.get("api_key")
    if not api_key:
        raise ProviderCreationError(f"No API key provided for {provider} Realtime")
    
    if provider == "openai":
        from livekit.plugins.openai import realtime
        from livekit.plugins.openai.realtime.utils import TurnDetection
        
        os.environ["OPENAI_API_KEY"] = api_key
        
        model = config.get("model", "gpt-4o-realtime")
        voice = config.get("voice", "verse")
        
        # Build realtime model configuration
        kwargs = {
            "model": model,
            "turn_detection": TurnDetection(
                type="server_vad",
                threshold=float(config.get("vad_threshold", 0.5)),
                prefix_padding_ms=int(config.get("prefix_padding_ms", 300)),
                silence_duration_ms=int(config.get("silence_duration_ms", 500)),
                create_response=config.get("create_response", True),
            ),
        }
        
        if use_separate_tts:
            # Text-only mode: OpenAI Realtime handles STT+LLM, Fish Audio handles audio output
            kwargs["modalities"] = ["text"]
            logger.info("[realtime] Using text-only mode (modalities=['text']) for Fish Audio TTS")
        else:
            # Full audio mode: OpenAI Realtime handles STT+LLM+TTS
            kwargs["voice"] = voice
            kwargs["modalities"] = ["text", "audio"]
        
        return realtime.RealtimeModel(**kwargs)
    
    elif provider == "google":
        from livekit.plugins.google import realtime
        
        os.environ["GOOGLE_API_KEY"] = api_key
        
        model = config.get("model", "gemini-2.5-flash-native-audio-preview-12-2025")
        voice = config.get("voice", "Puck")
        
        # Check if using VertexAI
        use_vertexai = config.get("vertexai", False)
        
        kwargs = {
            "model": model,
            "voice": voice,
        }
        
        if use_vertexai:
            kwargs["vertexai"] = True
            if config.get("project"):
                kwargs["project"] = config["project"]
            if config.get("location"):
                kwargs["location"] = config["location"]
        
        return realtime.RealtimeModel(**kwargs)
    
    elif provider == "xai":
        # Use our custom xAI implementation with function call support
        # The standard LiveKit xAI plugin doesn't handle xAI's function call events
        from providers import xai_realtime
        from openai.types.beta.realtime.session import TurnDetection
        
        os.environ["XAI_API_KEY"] = api_key
        
        model = config.get("model", "grok-2-public")
        voice = config.get("voice", "ara")
        
        # Build realtime model configuration
        # Note: xAI RealtimeModel does NOT support 'modalities' parameter (unlike OpenAI)
        kwargs = {
            "voice": voice,
            "turn_detection": TurnDetection(
                type="server_vad",
                threshold=float(config.get("vad_threshold", 0.5)),
                prefix_padding_ms=int(config.get("prefix_padding_ms", 300)),
                silence_duration_ms=int(config.get("silence_duration_ms", 200)),
                create_response=config.get("create_response", True),
                interrupt_response=config.get("interrupt_response", True),
            ),
        }
        
        if use_separate_tts:
            # xAI doesn't support text-only mode via modalities parameter
            # Log a warning - separate TTS with xAI may not work as expected
            logger.warning("[realtime] xAI Grok does not support text-only mode. Separate TTS may not work correctly.")
        
        return xai_realtime.RealtimeModel(**kwargs)
    
    else:
        raise ProviderCreationError(f"Unsupported realtime provider: {provider}. Supported: openai, google, xai")


def create_all_providers(resolved_credentials: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Create all provider instances from resolved credentials.
    
    Args:
        resolved_credentials: Output from resolver.resolve_all_credentials()
        
    Returns:
        Dict with instantiated providers:
        {
            'stt': <STT instance>,
            'llm': <LLM instance>,
            'tts': <TTS instance>,
        }
        OR for realtime mode:
        {
            'realtime': <RealtimeModel instance>,
            'tts': <TTS instance> (optional, if using separate TTS)
        }
    """
    providers = {}
    
    # Check for realtime mode
    if "realtime" in resolved_credentials:
        providers["realtime"] = create_realtime_model(resolved_credentials["realtime"])
        
        # Optionally create separate TTS if configured
        if "tts" in resolved_credentials:
            providers["tts"] = create_tts(resolved_credentials["tts"])
    else:
        # Standard pipeline mode
        providers["stt"] = create_stt(resolved_credentials["stt"])
        providers["llm"] = create_llm(resolved_credentials["llm"])
        providers["tts"] = create_tts(resolved_credentials["tts"])
    
    return providers
