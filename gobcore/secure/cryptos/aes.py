"""
Plain AES in ECB mode

ECB stands for Electronic Codebook (ECB) mode.
The message is divided into blocks, and each block is encrypted separately.

The disadvantage of this method is a lack of diffusion.
ECB encrypts identical plaintext blocks into identical ciphertext blocks.

AES in ECB mode encryption can be used to encrypt values that relate to values in other collections,
because the encrypted value will be identical for identical values.
"""
import base64

from Crypto.Cipher import AES
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import algorithms

from gobcore.secure.cryptos.config import DecryptionError, get_keys


class AESCrypto():

    _ciphers = None

    def __init__(self):
        if not self._ciphers:
            AESCrypto._ciphers = [AES.new(base64.urlsafe_b64decode(key), AES.MODE_ECB) for key in get_keys()]

    def encrypt(self, value: str, confidence_level=None) -> str:
        """
        Encrypt a string value

        :param value: any string value
        :param confidence_level: not used
        :return: key index
        """
        key_index = 0  # encrypt with the first key
        msg = value.encode()
        padder = padding.PKCS7(algorithms.AES.block_size).padder()
        padded_data = padder.update(msg) + padder.finalize()
        msg_enc = self._ciphers[key_index].encrypt(padded_data)
        return key_index, base64.urlsafe_b64encode(msg_enc).decode()

    def decrypt(self, value: str, confidence_level=None, key_index=None) -> str:
        """
        Decrypt a previously encrypted value

        :param value: any encrypted value
        :param confidence_level: not used
        :param key_index: not used
        :return:
        """
        msg_enc = base64.urlsafe_b64decode(value)
        for cipher in self._ciphers:
            # Try each cipher, starting from the most actual (first) cipher
            try:
                msg_dec = cipher.decrypt(msg_enc)
                unpadder = padding.PKCS7(algorithms.AES.block_size).unpadder()
                unpadded = unpadder.update(msg_dec)
                unpadded += unpadder.finalize()
                return unpadded.decode()
            except Exception:
                pass

        raise DecryptionError
