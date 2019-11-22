import os

from gobcore.secure.config import ROLES

MIN_USER_LEVEL = 10


class User:

    def __init__(self, request):
        """
        A user is instantiated by the parameters in the HTTPS request

        :param request:
        """
        # roles = request.headers.get("X-Auth-Roles", "")
        roles = os.getenv("ROLES", "")
        self._roles = roles.split(",")

    def has_access_to(self, encrypted_value):
        """
        Tells if the user has access to the given encrypted value

        :param encrypted_value:
        :return:
        """
        return any(ROLES.get(user_level, -1) >= MIN_USER_LEVEL for user_level in self._roles)
