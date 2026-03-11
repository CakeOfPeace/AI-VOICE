"""
Encryption service for API key storage.

Uses Fernet symmetric encryption (AES-128-CBC with HMAC-SHA256).
The encryption key is stored in CREDENTIALS_ENCRYPTION_KEY environment variable.
If not set, a key is auto-generated and logged on first run.
"""

from __future__ import annotations

import base64
import hashlib
import os
import sys
from typing import Optional

from cryptography.fernet import Fernet, InvalidToken


_fernet_instance: Optional[Fernet] = None


def _get_or_create_key() -> bytes:
    """
    Get the encryption key from environment or generate one.
    
    The key must be a 32-byte URL-safe base64-encoded string.
    If CREDENTIALS_ENCRYPTION_KEY is set, it's used directly.
    If not, we derive a key from CREDENTIALS_SECRET (if set) or auto-generate.
    """
    # Option 1: Direct Fernet key (must be 32 bytes base64-encoded)
    env_key = os.getenv("CREDENTIALS_ENCRYPTION_KEY", "").strip()
    if env_key:
        try:
            # Validate it's a proper Fernet key
            decoded = base64.urlsafe_b64decode(env_key)
            if len(decoded) == 32:
                return env_key.encode()
        except Exception:
            pass
        # If not valid Fernet format, derive from it
        derived = hashlib.sha256(env_key.encode()).digest()
        return base64.urlsafe_b64encode(derived)
    
    # Option 2: Derive from a secret passphrase
    secret = os.getenv("CREDENTIALS_SECRET", "").strip()
    if secret:
        derived = hashlib.sha256(secret.encode()).digest()
        return base64.urlsafe_b64encode(derived)
    
    # Option 3: Auto-generate (only for development - prints warning)
    print(
        "[WARNING] No CREDENTIALS_ENCRYPTION_KEY or CREDENTIALS_SECRET set. "
        "Generating a temporary key. Set one of these environment variables "
        "for production use to persist encrypted credentials across restarts.",
        file=sys.stderr,
        flush=True,
    )
    new_key = Fernet.generate_key()
    print(f"[INFO] Generated key (save this): CREDENTIALS_ENCRYPTION_KEY={new_key.decode()}", file=sys.stderr, flush=True)
    return new_key


def _get_fernet() -> Fernet:
    """Get or create the Fernet instance (singleton)."""
    global _fernet_instance
    if _fernet_instance is None:
        key = _get_or_create_key()
        _fernet_instance = Fernet(key)
    return _fernet_instance


def encrypt_value(plaintext: str) -> str:
    """
    Encrypt a plaintext string and return base64-encoded ciphertext.
    
    Args:
        plaintext: The string to encrypt (e.g., an API key)
        
    Returns:
        Base64-encoded encrypted string
    """
    if not plaintext:
        return ""
    fernet = _get_fernet()
    encrypted = fernet.encrypt(plaintext.encode("utf-8"))
    return encrypted.decode("utf-8")


def decrypt_value(ciphertext: str) -> str:
    """
    Decrypt a base64-encoded ciphertext string.
    
    Args:
        ciphertext: The encrypted string from encrypt_value()
        
    Returns:
        The original plaintext string
        
    Raises:
        ValueError: If decryption fails (invalid key or corrupted data)
    """
    if not ciphertext:
        return ""
    fernet = _get_fernet()
    try:
        decrypted = fernet.decrypt(ciphertext.encode("utf-8"))
        return decrypted.decode("utf-8")
    except InvalidToken as e:
        raise ValueError(f"Failed to decrypt value: invalid key or corrupted data") from e


def mask_api_key(api_key: str, visible_chars: int = 4) -> str:
    """
    Mask an API key for display, showing only the last N characters.
    
    Args:
        api_key: The full API key
        visible_chars: Number of characters to show at the end (default: 4)
        
    Returns:
        Masked string like "sk-...abc1234" or "••••abc1234"
    """
    if not api_key:
        return ""
    if len(api_key) <= visible_chars:
        return "•" * len(api_key)
    
    # Detect common prefixes to preserve
    prefixes = ["sk-", "sk-proj-", "gsk_", "key-", "pk_", "rk_"]
    prefix = ""
    for p in prefixes:
        if api_key.startswith(p):
            prefix = p
            break
    
    masked_len = len(api_key) - len(prefix) - visible_chars
    if masked_len <= 0:
        return api_key[:len(prefix)] + "•" * (len(api_key) - len(prefix))
    
    return f"{prefix}{'•' * min(masked_len, 20)}...{api_key[-visible_chars:]}"


def is_encrypted(value: str) -> bool:
    """
    Check if a value appears to be Fernet-encrypted.
    
    Fernet tokens start with 'gAAAAA' (base64-encoded version byte + timestamp).
    """
    if not value:
        return False
    return value.startswith("gAAAAA") and len(value) > 50


def encrypt_dict_keys(data: dict, keys_to_encrypt: list[str]) -> dict:
    """
    Encrypt specified keys in a dictionary.
    
    Args:
        data: Dictionary containing values to encrypt
        keys_to_encrypt: List of key names to encrypt (e.g., ["api_key"])
        
    Returns:
        New dictionary with encrypted values (original keys renamed to *_encrypted)
    """
    result = {}
    for k, v in data.items():
        if k in keys_to_encrypt and v and isinstance(v, str) and not is_encrypted(v):
            result[f"{k}_encrypted"] = encrypt_value(v)
        else:
            result[k] = v
    return result


def decrypt_dict_keys(data: dict, keys_to_decrypt: list[str]) -> dict:
    """
    Decrypt specified keys in a dictionary.
    
    Args:
        data: Dictionary containing encrypted values (with *_encrypted suffix)
        keys_to_decrypt: List of original key names (e.g., ["api_key"])
        
    Returns:
        New dictionary with decrypted values (renamed back to original keys)
    """
    result = {}
    for k, v in data.items():
        original_key = k.replace("_encrypted", "") if k.endswith("_encrypted") else k
        if original_key in keys_to_decrypt and k.endswith("_encrypted") and v:
            try:
                result[original_key] = decrypt_value(v)
            except ValueError:
                # Keep encrypted if decryption fails
                result[k] = v
        else:
            result[k] = v
    return result


# Convenience function for credential dictionaries
def encrypt_credentials(creds: dict) -> dict:
    """Encrypt api_key fields in a credentials dictionary."""
    return encrypt_dict_keys(creds, ["api_key"])


def decrypt_credentials(creds: dict) -> dict:
    """Decrypt api_key fields in a credentials dictionary."""
    return decrypt_dict_keys(creds, ["api_key"])
