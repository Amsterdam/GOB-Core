"""GOB cryptograhic functions

"""
import random

from gobcore.secure.fernet import Crypto


_safe_storage = {}

_KEY_INDEX = "i"
_CONFIDENCE_LEVEL = "l"
_VALUE = "v"


def is_encrypted(value):
    """
    Tells if a value is an encrypted value

    :param value: any value
    :return: True when the value is an encrypted value
    """
    keys = [_KEY_INDEX, _CONFIDENCE_LEVEL, _VALUE]
    return isinstance(value, dict) and \
        all([key in value for key in keys]) and \
        len(value.keys()) == len(keys)


def confidence_level(encrypted_value):
    """
    Tells the confidence level of an encrypted value

    :param encrypted_value: any encrypted value
    :return: the required confidence level to have access to the value
    """
    return encrypted_value[_CONFIDENCE_LEVEL]


def encrypt(value, confidence_level):
    """
    Encrypt a value with the encryption for the given confidence level

    :param value:
    :param confidence_level:
    :return:
    """
    key_index, encrypted_value = Crypto().encrypt(value, confidence_level)
    return {
        _KEY_INDEX: key_index,                # Allows for key rotation
        _CONFIDENCE_LEVEL: confidence_level,  # Some data is more confident that other data
        _VALUE: encrypted_value               # The encrypted data
    }


def decrypt(value):
    """
    Decrypt an encrypted value

    :param value:
    :return:
    """
    key_index = value[_KEY_INDEX]
    confidence_level = value[_CONFIDENCE_LEVEL]
    return Crypto().decrypt(value[_VALUE], confidence_level, key_index)


def read_protect(value, save_local=True):
    """
    Protect sensitive data by storing it locally or encrypt its value

    :param value: any sensitive data
    :param save_local: store it locally if true, else encrypt
    :return: the key to the sensitive data or the encrypted value
    """
    if save_local:
        key = random.random()
        _safe_storage[key] = value
        return key
    else:
        _, encrypted_value = Crypto().encrypt(value)
        return encrypted_value


def read_unprotect(value):
    """
    Unprotect a previously read protected data

    :param value: the key to the sensitive data or the encrypted value
    :return: the unprotected value
    """
    if value in _safe_storage:
        saved_value = _safe_storage[value]
        del _safe_storage[value]
        return saved_value
    else:
        return Crypto().decrypt(value)
