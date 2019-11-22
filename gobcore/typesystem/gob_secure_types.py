from gobcore.typesystem.gob_types import String, Decimal, DateTime
from gobcore.secure.crypto import is_encrypted, encrypt, decrypt, read_unprotect


class Secure(String):
    """
    Secure types are derived from the String type
    """
    name = "Secure"
    is_secure = True

    def __init__(self, value, level=None):
        """
        Initialize a secure value.

        If the value is not yet secured, secure it

        :param value: the secure value
        :param level: the level to encrypt the value if it is still unencrypted
        """
        if not is_encrypted(value):
            assert level is not None, "Missing level to encrypt the given value"
            value = encrypt(str(value), confidence_level=level)
        super().__init__(value)

    @classmethod
    def from_value(cls, value, **kwargs):
        """
        Initialize a secure value from a read value (import or database)

        If the date is read via an import, a confidence level should be supplied
        to encrypt the data. Also the data should first be unprotected, as all
        data that is read from any external source is protected upon retrieval.

        :param value: a value read via an import or from the database
        :param kwargs:
        :return: an instance of a Secure datatype
        """
        if "level" in kwargs:
            level = kwargs["level"]
            del kwargs["level"]
            value = read_unprotect(value)
        else:
            level = None

        if not is_encrypted(value):
            value = cls.BaseType.from_value(value, **kwargs)

        return cls(value, level)

    def get_value(self, user=None):
        """
        Get the value from a secure datatype.

        Access to the value is protected by checking the user authorization levels.
        Only if the authorization level meets the requirements for the given value,
        its value will be returned.

        Otherwise a no-access value will be returned
        :param user:
        :return:
        """
        if user is None:
            has_access = False
        else:
            value = self._string
            has_access = user.has_access_to(value)

        if has_access:
            value = decrypt(value)
            return None if value is None else self.get_typed_value(value)
        else:
            return "*" * 10


class SecureString(Secure):
    name = "SecureString"
    BaseType = String

    def get_typed_value(self, value):
        return value


class SecureDecimal(Secure):
    name = "SecureDecimal"
    BaseType = Decimal

    def get_typed_value(self, value):
        return float(value)


class SecureDateTime(Secure):
    name = "SecureDateTime"
    BaseType = DateTime

    def get_typed_value(self, value):
        # Behave like its BaseType...
        return DateTime.from_value(value).to_value
