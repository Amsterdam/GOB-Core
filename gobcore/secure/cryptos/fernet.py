"""
Fernet encryption

Conceptually, fernet takes
- a user-provided message (an arbitrary sequence of bytes)
- a key (256 bits)
- and the current time
and produces a token, which contains the message in a form that can't be read or altered without the key.

Encryption is done with AES 128 in CBC mode.

CBC stands for Cipher Block Chaining.
In CBC mode, each block of plaintext is XORed with the previous ciphertext block before being encrypted.
This way, each ciphertext block depends on all plaintext blocks processed up to that point.

CBC is one of the most commonly used mode of operation.
Its main drawback is that encryption is sequential (i.e., it cannot be parallelized).

All base 64 encoding is done with the "URL and Filename Safe" variant, defined in RFC 4648 as "base64url".
"""
from cryptography.fernet import Fernet, MultiFernet, InvalidToken

from gobcore.secure.cryptos.config import DecryptionError, get_keys


class FernetCrypto:

    _fernet = None

    def __init__(self):
        """
        Initializes the Fernet crypto class

        https://github.com/fernet/spec/blob/master/Spec.md

        Basically Fernet is AES 128
        It is chosen because it is resource efficient and offers a reasonable security margin

        MultiFernet is chosen because it can handle key rotation
        """
        if not self._fernet:
            FernetCrypto._fernet = MultiFernet([Fernet(key) for key in get_keys()])

    def encrypt(self, value: str, confidence_level=None) -> str:
        """
        Encrypt a string value

        :param value: any string value
        :param confidence_level: not used
        :return: (key index (unused in multiFernet), the encrypted value)
        """
        return 0, self._fernet.encrypt(value.encode()).decode()

    def decrypt(self, value: str, confidence_level=None, key_index=None) -> str:
        """
        Decrypt a previously encrypted value

        :param value: any encrypted value
        :param confidence_level: not used
        :param key_index: not used
        :return:
        """
        try:
            return self._fernet.decrypt(value.encode()).decode()
        except InvalidToken:
            raise DecryptionError
