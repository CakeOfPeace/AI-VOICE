"""
Provider registry and configuration for voice pipeline components.

This module defines all supported STT, LLM, and TTS providers along with
their required configuration fields and model options.

SIMPLIFIED: Only OpenAI, Google/Gemini, Groq, and Fish Audio
"""

from __future__ import annotations

from typing import Any, Dict, List

# =============================================================================
# PROVIDER REGISTRY
# =============================================================================

# STT (Speech-to-Text) Providers
STT_PROVIDERS: Dict[str, Dict[str, Any]] = {
    "openai": {
        "name": "OpenAI",
        "plugin": "livekit.plugins.openai",
        "class": "STT",
        "requires": ["api_key"],
        "optional": ["base_url"],
        "models": [
            {"id": "gpt-4o-transcribe", "label": "GPT-4o Transcribe"},
            {"id": "gpt-4o-mini-transcribe", "label": "GPT-4o Mini Transcribe"},
            {"id": "whisper-1", "label": "Whisper v1"},
        ],
        "supports_realtime": True,
        "supports_language_detection": True,
    },
    "google": {
        "name": "Google Cloud",
        "plugin": "livekit.plugins.google",
        "class": "STT",
        "requires": ["api_key"],
        "optional": ["location"],
        "models": [
            {"id": "chirp", "label": "Chirp"},
            {"id": "chirp_2", "label": "Chirp 2"},
            {"id": "long", "label": "Long"},
            {"id": "short", "label": "Short"},
        ],
        "supports_realtime": True,
        "supports_language_detection": True,
    },
    "groq": {
        "name": "Groq",
        "plugin": "livekit.plugins.groq",
        "class": "STT",
        "requires": ["api_key"],
        "optional": [],
        "models": [
            {"id": "whisper-large-v3-turbo", "label": "Whisper Large v3 Turbo (Fast)"},
            {"id": "whisper-large-v3", "label": "Whisper Large v3 (Accurate)"},
        ],
        "supports_realtime": True,
        "supports_language_detection": True,
    },
}

# LLM (Large Language Model) Providers
LLM_PROVIDERS: Dict[str, Dict[str, Any]] = {
    "openai": {
        "name": "OpenAI",
        "plugin": "livekit.plugins.openai",
        "class": "LLM",
        "requires": ["api_key"],
        "optional": ["base_url"],
        "models": [
            {"id": "gpt-4.1", "label": "GPT-4.1"},
            {"id": "gpt-4.1-mini", "label": "GPT-4.1 Mini"},
            {"id": "gpt-4.1-nano", "label": "GPT-4.1 Nano"},
            {"id": "gpt-5", "label": "GPT-5"},
            {"id": "gpt-5-mini", "label": "GPT-5 Mini"},
            {"id": "gpt-5-nano", "label": "GPT-5 Nano"},
            {"id": "gpt-4o", "label": "GPT-4o"},
            {"id": "gpt-4o-mini", "label": "GPT-4o Mini"},
            {"id": "gpt-4-turbo", "label": "GPT-4 Turbo"},
        ],
        "supports_function_calling": True,
        "supports_streaming": True,
    },
    "google": {
        "name": "Google (Gemini)",
        "plugin": "livekit.plugins.google",
        "class": "LLM",
        "requires": ["api_key"],
        "optional": [],
        "models": [
            {"id": "gemini-3-flash-preview", "label": "Gemini 3 Flash (Preview)"},
            {"id": "gemini-2.5-flash", "label": "Gemini 2.5 Flash"},
            {"id": "gemini-2.5-flash-lite", "label": "Gemini 2.5 Flash Lite"},
            {"id": "gemini-2.5-pro", "label": "Gemini 2.5 Pro"},
            {"id": "gemini-2.0-flash", "label": "Gemini 2.0 Flash"},
            {"id": "gemini-2.0-flash-lite", "label": "Gemini 2.0 Flash Lite"},
        ],
        "supports_function_calling": True,
        "supports_streaming": True,
    },
    "groq": {
        "name": "Groq",
        "plugin": "livekit.plugins.groq",
        "class": "LLM",
        "requires": ["api_key"],
        "optional": [],
        "models": [
            # Production Models
            {"id": "meta-llama/llama-3.3-70b-versatile", "label": "Llama 3.3 70B Versatile"},
            {"id": "meta-llama/llama-3.1-8b-instant", "label": "Llama 3.1 8B Instant"},
            {"id": "openai/gpt-oss-120b", "label": "GPT-OSS 120B"},
            {"id": "openai/gpt-oss-20b", "label": "GPT-OSS 20B"},
            # Preview Models
            {"id": "meta-llama/llama-4-maverick-17b-128e-instruct", "label": "Llama 4 Maverick 17B"},
            {"id": "meta-llama/llama-4-scout-17b-16e-instruct", "label": "Llama 4 Scout 17B"},
            {"id": "moonshotai/kimi-k2-instruct-0905", "label": "Kimi K2 Instruct"},
            {"id": "qwen/qwen3-32b", "label": "Qwen 3 32B"},
        ],
        "supports_function_calling": True,
        "supports_streaming": True,
    },
}

# TTS (Text-to-Speech) Providers
TTS_PROVIDERS: Dict[str, Dict[str, Any]] = {
    "fish": {
        "name": "Fish Audio",
        "plugin": "fish_tts_provider",  # Custom implementation
        "class": "FishSpeechTTS",
        "requires": ["api_key"],
        "optional": ["reference_id"],
        "models": [
            {"id": "s1", "label": "Speech S1"},
        ],
        "supports_voice_cloning": True,
        "supports_emotion_tags": True,
        "voice_field": "reference_id",
    },
    "openai": {
        "name": "OpenAI TTS",
        "plugin": "livekit.plugins.openai",
        "class": "TTS",
        "requires": ["api_key"],
        "optional": ["base_url"],
        "models": [
            {"id": "gpt-4o-mini-tts", "label": "GPT-4o Mini TTS"},
            {"id": "tts-1", "label": "TTS-1"},
            {"id": "tts-1-hd", "label": "TTS-1 HD"},
        ],
        "voices": [
            {"id": "alloy", "label": "Alloy (Neutral)", "sample_url": "https://cdn.openai.com/API/docs/audio/alloy.wav"},
            {"id": "ash", "label": "Ash (Soft)", "sample_url": "https://cdn.openai.com/API/docs/audio/ash.wav"},
            {"id": "ballad", "label": "Ballad (Warm)", "sample_url": "https://cdn.openai.com/API/docs/audio/ballad.wav"},
            {"id": "coral", "label": "Coral (Clear)", "sample_url": "https://cdn.openai.com/API/docs/audio/coral.wav"},
            {"id": "echo", "label": "Echo (Deep)", "sample_url": "https://cdn.openai.com/API/docs/audio/echo.wav"},
            {"id": "fable", "label": "Fable (Expressive)", "sample_url": "https://cdn.openai.com/API/docs/audio/fable.wav"},
            {"id": "onyx", "label": "Onyx (Deep)", "sample_url": "https://cdn.openai.com/API/docs/audio/onyx.wav"},
            {"id": "nova", "label": "Nova (Friendly)", "sample_url": "https://cdn.openai.com/API/docs/audio/nova.wav"},
            {"id": "shimmer", "label": "Shimmer (Bright)", "sample_url": "https://cdn.openai.com/API/docs/audio/shimmer.wav"},
            {"id": "sage", "label": "Sage (Wise)", "sample_url": "https://cdn.openai.com/API/docs/audio/sage.wav"},
            {"id": "verse", "label": "Verse (Versatile)", "sample_url": "https://cdn.openai.com/API/docs/audio/verse.wav"},
        ],
        "supports_voice_cloning": False,
        "supports_emotion_tags": False,
        "voice_field": "voice",
    },
    "gemini": {
        "name": "Gemini TTS (Native)",
        "plugin": "livekit.plugins.google.beta",
        "class": "GeminiTTS",
        "requires": ["api_key"],
        "optional": ["instructions"],
        "models": [
            {"id": "gemini-2.5-flash-preview-tts", "label": "Gemini 2.5 Flash TTS"},
            {"id": "gemini-2.5-pro-preview-tts", "label": "Gemini 2.5 Pro TTS"},
        ],
        "voices": [
            {"id": "Zephyr", "label": "Zephyr (Bright)"},
            {"id": "Puck", "label": "Puck (Upbeat)"},
            {"id": "Charon", "label": "Charon (Informative)"},
            {"id": "Kore", "label": "Kore (Firm)"},
            {"id": "Fenrir", "label": "Fenrir (Excitable)"},
            {"id": "Leda", "label": "Leda (Youthful)"},
            {"id": "Orus", "label": "Orus (Firm)"},
            {"id": "Aoede", "label": "Aoede (Breezy)"},
            {"id": "Callirrhoe", "label": "Callirrhoe (Easy-going)"},
            {"id": "Autonoe", "label": "Autonoe (Bright)"},
            {"id": "Enceladus", "label": "Enceladus (Breathy)"},
            {"id": "Iapetus", "label": "Iapetus (Clear)"},
            {"id": "Umbriel", "label": "Umbriel (Easy-going)"},
            {"id": "Algieba", "label": "Algieba (Smooth)"},
            {"id": "Despina", "label": "Despina (Smooth)"},
            {"id": "Erinome", "label": "Erinome (Clear)"},
            {"id": "Algenib", "label": "Algenib (Gravelly)"},
            {"id": "Rasalgethi", "label": "Rasalgethi (Informative)"},
            {"id": "Laomedeia", "label": "Laomedeia (Upbeat)"},
            {"id": "Achernar", "label": "Achernar (Soft)"},
            {"id": "Alnilam", "label": "Alnilam (Firm)"},
            {"id": "Schedar", "label": "Schedar (Even)"},
            {"id": "Gacrux", "label": "Gacrux (Mature)"},
            {"id": "Pulcherrima", "label": "Pulcherrima (Forward)"},
            {"id": "Achird", "label": "Achird (Friendly)"},
            {"id": "Zubenelgenubi", "label": "Zubenelgenubi (Casual)"},
            {"id": "Vindemiatrix", "label": "Vindemiatrix (Gentle)"},
            {"id": "Sadachbia", "label": "Sadachbia (Lively)"},
            {"id": "Sadaltager", "label": "Sadaltager (Knowledgeable)"},
            {"id": "Sulafat", "label": "Sulafat (Warm)"},
        ],
        "supports_voice_cloning": False,
        "supports_emotion_tags": True,  # Supports style instructions
        "voice_field": "voice_name",
    },
    "groq": {
        "name": "Groq TTS (Orpheus)",
        "plugin": "livekit.plugins.groq",
        "class": "TTS",
        "requires": ["api_key"],
        "optional": [],
        "models": [
            {"id": "canopylabs/orpheus-v1-english", "label": "Orpheus English"},
            {"id": "canopylabs/orpheus-arabic-saudi", "label": "Orpheus Arabic (Saudi)"},
        ],
        "voices": [
            # English voices (for orpheus-v1-english)
            {"id": "autumn", "label": "Autumn (Female)", "model": "canopylabs/orpheus-v1-english"},
            {"id": "diana", "label": "Diana (Female)", "model": "canopylabs/orpheus-v1-english"},
            {"id": "hannah", "label": "Hannah (Female)", "model": "canopylabs/orpheus-v1-english"},
            {"id": "austin", "label": "Austin (Male)", "model": "canopylabs/orpheus-v1-english"},
            {"id": "daniel", "label": "Daniel (Male)", "model": "canopylabs/orpheus-v1-english"},
            {"id": "troy", "label": "Troy (Male)", "model": "canopylabs/orpheus-v1-english"},
            # Arabic voices (for orpheus-arabic-saudi)
            {"id": "fahad", "label": "Fahad (Arabic Male)", "model": "canopylabs/orpheus-arabic-saudi"},
            {"id": "sultan", "label": "Sultan (Arabic Male)", "model": "canopylabs/orpheus-arabic-saudi"},
            {"id": "lulwa", "label": "Lulwa (Arabic Female)", "model": "canopylabs/orpheus-arabic-saudi"},
            {"id": "noura", "label": "Noura (Arabic Female)", "model": "canopylabs/orpheus-arabic-saudi"},
        ],
        "supports_voice_cloning": False,
        "supports_emotion_tags": True,  # Orpheus supports vocal directions like [cheerful]
        "voice_field": "voice",
    },
}

# Realtime (Combined STT+LLM+TTS) Providers
REALTIME_PROVIDERS: Dict[str, Dict[str, Any]] = {
    "openai": {
        "name": "OpenAI Realtime",
        "plugin": "livekit.plugins.openai.realtime",
        "class": "RealtimeModel",
        "requires": ["api_key"],
        "optional": ["base_url"],
        "models": [
            {"id": "gpt-4o-realtime", "label": "GPT-4o Realtime"},
            {"id": "gpt-4o-mini-realtime", "label": "GPT-4o Mini Realtime"},
            {"id": "gpt-realtime", "label": "GPT Realtime"},
        ],
        "voices": [
            {"id": "alloy", "label": "Alloy (Neutral)", "sample_url": "https://cdn.openai.com/API/docs/audio/alloy.wav"},
            {"id": "ash", "label": "Ash (Soft)", "sample_url": "https://cdn.openai.com/API/docs/audio/ash.wav"},
            {"id": "ballad", "label": "Ballad (Warm)", "sample_url": "https://cdn.openai.com/API/docs/audio/ballad.wav"},
            {"id": "coral", "label": "Coral (Clear)", "sample_url": "https://cdn.openai.com/API/docs/audio/coral.wav"},
            {"id": "echo", "label": "Echo (Deep)", "sample_url": "https://cdn.openai.com/API/docs/audio/echo.wav"},
            {"id": "sage", "label": "Sage (Wise)", "sample_url": "https://cdn.openai.com/API/docs/audio/sage.wav"},
            {"id": "shimmer", "label": "Shimmer (Bright)", "sample_url": "https://cdn.openai.com/API/docs/audio/shimmer.wav"},
            {"id": "verse", "label": "Verse (Versatile)", "sample_url": "https://cdn.openai.com/API/docs/audio/verse.wav"},
        ],
        "supports_tools": True,
    },
    "google": {
        "name": "Google Gemini Live",
        "plugin": "livekit.plugins.google.realtime",
        "class": "RealtimeModel",
        "requires": ["api_key"],
        "optional": ["vertexai", "project", "location", "instructions"],
        "models": [
            # Native Audio models (speech-to-speech)
            {"id": "gemini-2.5-flash-native-audio-preview-09-2025", "label": "Gemini 2.5 Flash Native (Thinking)"},
            {"id": "gemini-2.5-flash-native-audio-preview-12-2025", "label": "Gemini 2.5 Flash Native Audio"},
            {"id": "gemini-2.5-flash", "label": "Gemini 2.5 Flash (Default)"},
            {"id": "gemini-2.0-flash-live-001", "label": "Gemini 2.0 Flash Live"},
            # VertexAI models
            {"id": "gemini-live-2.5-flash-native-audio", "label": "Gemini Live 2.5 (VertexAI)"},
        ],
        "voices": [
            # Same 30 voices as Gemini TTS
            {"id": "Puck", "label": "Puck (Upbeat)"},
            {"id": "Charon", "label": "Charon (Informative)"},
            {"id": "Kore", "label": "Kore (Firm)"},
            {"id": "Fenrir", "label": "Fenrir (Excitable)"},
            {"id": "Aoede", "label": "Aoede (Breezy)"},
            {"id": "Leda", "label": "Leda (Youthful)"},
            {"id": "Orus", "label": "Orus (Firm)"},
            {"id": "Zephyr", "label": "Zephyr (Bright)"},
            {"id": "Callirrhoe", "label": "Callirrhoe (Easy-going)"},
            {"id": "Autonoe", "label": "Autonoe (Bright)"},
            {"id": "Enceladus", "label": "Enceladus (Breathy)"},
            {"id": "Iapetus", "label": "Iapetus (Clear)"},
            {"id": "Umbriel", "label": "Umbriel (Easy-going)"},
            {"id": "Algieba", "label": "Algieba (Smooth)"},
            {"id": "Despina", "label": "Despina (Smooth)"},
            {"id": "Erinome", "label": "Erinome (Clear)"},
            {"id": "Algenib", "label": "Algenib (Gravelly)"},
            {"id": "Rasalgethi", "label": "Rasalgethi (Informative)"},
            {"id": "Laomedeia", "label": "Laomedeia (Upbeat)"},
            {"id": "Achernar", "label": "Achernar (Soft)"},
            {"id": "Alnilam", "label": "Alnilam (Firm)"},
            {"id": "Schedar", "label": "Schedar (Even)"},
            {"id": "Gacrux", "label": "Gacrux (Mature)"},
            {"id": "Pulcherrima", "label": "Pulcherrima (Forward)"},
            {"id": "Achird", "label": "Achird (Friendly)"},
            {"id": "Zubenelgenubi", "label": "Zubenelgenubi (Casual)"},
            {"id": "Vindemiatrix", "label": "Vindemiatrix (Gentle)"},
            {"id": "Sadachbia", "label": "Sadachbia (Lively)"},
            {"id": "Sadaltager", "label": "Sadaltager (Knowledgeable)"},
            {"id": "Sulafat", "label": "Sulafat (Warm)"},
        ],
        "supports_tools": True,
        "supports_thinking": True,  # gemini-2.5-flash-native-audio-preview-09-2025
        "supports_affective_dialog": True,
        "supports_proactivity": True,
    },
    "xai": {
        "name": "xAI Grok",
        "plugin": "livekit.plugins.xai.realtime",
        "class": "RealtimeModel",
        "requires": ["api_key"],
        "optional": [],
        "models": [
            {"id": "grok-2-public", "label": "Grok 2 (Public)"},
            {"id": "grok-3-fast", "label": "Grok 3 Fast"},
        ],
        "voices": [
            {"id": "ara", "label": "Ara (Warm, Friendly)"},
            {"id": "rex", "label": "Rex (Confident, Clear)"},
            {"id": "sal", "label": "Sal (Smooth, Balanced)"},
            {"id": "eve", "label": "Eve (Energetic, Upbeat)"},
            {"id": "leo", "label": "Leo (Authoritative, Strong)"},
        ],
        "supports_tools": True,
        "supports_x_search": True,  # XSearch, WebSearch, FileSearch tools
    },
}

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_provider_info(component: str, provider_id: str) -> Dict[str, Any] | None:
    """Get provider configuration by component type and provider ID."""
    registries = {
        "stt": STT_PROVIDERS,
        "llm": LLM_PROVIDERS,
        "tts": TTS_PROVIDERS,
        "realtime": REALTIME_PROVIDERS,
    }
    registry = registries.get(component.lower())
    if not registry:
        return None
    return registry.get(provider_id.lower())


def list_providers(component: str) -> List[Dict[str, str]]:
    """List all available providers for a component type."""
    registries = {
        "stt": STT_PROVIDERS,
        "llm": LLM_PROVIDERS,
        "tts": TTS_PROVIDERS,
        "realtime": REALTIME_PROVIDERS,
    }
    registry = registries.get(component.lower(), {})
    return [{"id": k, "name": v.get("name", k)} for k, v in registry.items()]


def get_default_provider(component: str) -> str | None:
    """Get the default provider for a component type."""
    defaults = {
        "stt": "openai",
        "llm": "openai",
        "tts": "fish",
        "realtime": "openai",
    }
    return defaults.get(component.lower())


def get_required_fields(component: str, provider_id: str) -> List[str]:
    """Get required configuration fields for a provider."""
    info = get_provider_info(component, provider_id)
    if not info:
        return []
    return info.get("requires", [])


def get_models_for_provider(component: str, provider_id: str) -> List[Dict[str, str]]:
    """Get available models for a provider."""
    info = get_provider_info(component, provider_id)
    if not info:
        return []
    return info.get("models", [])


def get_voices_for_provider(provider_id: str) -> List[Dict[str, str]]:
    """Get available voices for a TTS provider."""
    info = get_provider_info("tts", provider_id)
    if not info:
        return []
    return info.get("voices", [])


def get_voices_for_realtime_provider(provider_id: str) -> List[Dict[str, str]]:
    """Get available voices for a Realtime provider."""
    info = get_provider_info("realtime", provider_id)
    if not info:
        return []
    return info.get("voices", [])


# Export all for easy access
__all__ = [
    "STT_PROVIDERS",
    "LLM_PROVIDERS",
    "TTS_PROVIDERS",
    "REALTIME_PROVIDERS",
    "get_provider_info",
    "list_providers",
    "get_default_provider",
    "get_required_fields",
    "get_models_for_provider",
    "get_voices_for_provider",
    "get_voices_for_realtime_provider",
]
