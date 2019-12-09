from gobcore.secure.config import REQUEST_ROLES, GOB_ADMIN


class User:

    def __init__(self, request):
        """
        A user is instantiated by the parameters in the HTTPS request

        :param request:
        """
        roles = request.headers.get(REQUEST_ROLES, "")
        self._roles = roles.split(",")

    def has_access_to(self, encrypted_value):
        """
        Tells if the user has access to the given encrypted value

        :param encrypted_value:
        :return:
        """

        # For now only gob_adm has access to secure values
        return GOB_ADMIN in self._roles
