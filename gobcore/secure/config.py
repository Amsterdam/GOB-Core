"""
User authorization levels and roles

"""

# Keycloak header attributes
AUTH_PATTERN = '^X-Auth-'
REQUEST_USER = 'X-Auth-Userid'
REQUEST_ROLES = 'X-Auth-Roles'

# Keycloak roles
GOB_ADMIN = "gob_adm"
GOB_DUMP = "gob_dump"
GOB_SECURE_ATTRS = "gob_secure_attrs"
BRK_DATA_TOTAAL = "brk_data_totaal"
BRK_DATA_BEPERKT = "brk_data_beperkt"
