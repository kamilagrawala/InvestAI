import os
from cryptography.fernet import Fernet
import sys
from dotenv import load_dotenv

# Load local environment
load_dotenv(override=True)

KEY_FILE = "master.key"

def load_or_generate_key(env_name="MASTER_KEY"):
    """Load the existing master key or generate a new one if missing."""
    # Priority 1: Environment variable (for Docker/Production)
    env_key = os.getenv(env_name)
    if env_key:
        return env_key.encode()

    # Priority 2: Key file (for local development)
    if os.path.exists(KEY_FILE):
        with open(KEY_FILE, "rb") as f:
            return f.read()
    else:
        # Generate new key
        key = Fernet.generate_key()
        with open(KEY_FILE, "wb") as f:
            f.write(key)
        # Ensure it's not world-readable
        os.chmod(KEY_FILE, 0o600)
        return key

def get_cipher(env_name="MASTER_KEY"):
    """Initialize the cipher using the specified master key."""
    key = load_or_generate_key(env_name)
    return Fernet(key)

def encrypt_string(plain_text: str, env_name="MASTER_KEY") -> str:
    """Encrypt a string and return the base64 encoded result."""
    cipher = get_cipher(env_name)
    encrypted_bytes = cipher.encrypt(plain_text.encode())
    return encrypted_bytes.decode()

def decrypt_string(encrypted_text: str, env_name="MASTER_KEY") -> str:
    """Decrypt a base64 encoded string."""
    cipher = get_cipher(env_name)
    decrypted_bytes = cipher.decrypt(encrypted_text.encode())
    return decrypted_bytes.decode()

if __name__ == "__main__":
    # CLI mode for easy encryption
    if len(sys.argv) < 2:
        print("Usage: python crypto_utils.py <string_to_encrypt>")
        sys.exit(1)
    
    target = sys.argv[1]
    encrypted = encrypt_string(target)
    print(f"Original: {target}")
    print(f"Encrypted: {encrypted}")
    print("\nIMPORTANT: Keep your 'master.key' file safe and never commit it to git.")
