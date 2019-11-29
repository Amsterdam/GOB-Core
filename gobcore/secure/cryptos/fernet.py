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
