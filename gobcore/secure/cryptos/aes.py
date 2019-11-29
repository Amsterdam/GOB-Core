import base64

from Crypto.Cipher import AES
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import algorithms

from gobcore.secure.cryptos.config import DecryptionError, get_keys


class AESCrypto():

    _aes = None

    def __init__(self):
        if not self._aes:
            self._keys = get_keys()
            self._ciphers = [AES.new(base64.urlsafe_b64decode(key), AES.MODE_ECB) for key in self._keys]

    def encrypt(self, value: str, confidence_level=None) -> str:
        """
        Encrypt a string value

        :param value: any string value
        :param confidence_level: not used
        :return: key index
        """
        key_index = 0  # encrypt with the first key
        msg_dec = value.encode()
        padder = padding.PKCS7(algorithms.AES.block_size).padder()
        padded_data = padder.update(msg_dec) + padder.finalize()
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
            try:
                msg_dec = cipher.decrypt(msg_enc)
                unpadder = padding.PKCS7(algorithms.AES.block_size).unpadder()
                unpadded = unpadder.update(msg_dec)
                unpadded += unpadder.finalize()
                return unpadded.decode()
            except Exception:
                pass

        raise DecryptionError
