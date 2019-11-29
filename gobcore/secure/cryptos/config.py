import os
import base64

from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes


class DecryptionError(Exception):
    pass


def _getenv():
    """
    Read password(s) and salt(s) from the environment

    :return:
    """
    env = {
        "salt": os.getenv("SECURE_SALT"),
        "password": os.getenv("SECURE_PASSWORD")
    }
    # Assert that all environment variables are set, do not accept default values
    assert None not in env.values(), "missing SECURE_SALT and/or SECURE_PASSWORD environment variables"
    return env


def _get_key(password: str, salt: str) -> str:
    """
    Generate a secure key value given a password and a salt

    :param password:
    :param salt:
    :return:
    """
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=100000,
        backend=default_backend()
    )
    return base64.urlsafe_b64encode(kdf.derive(password))


def get_keys():
    """
    Get secure and safe keys on the basis of password-salt combinations

    :return:
    """
    return [_get_key(**{key: value.encode() for key, value in _getenv().items()})]
