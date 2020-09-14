class QA_LEVEL:
    """
    Constants to denote the level or severity of a quality issue
    """
    FATAL = "fatal"
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class QA_CHECK:
    """
    Defintion of quality checks
    """

    # Dutch border coordinates
    _NL_X_MIN = 110000
    _NL_X_MAX = 136000
    _NL_Y_MIN = 474000
    _NL_Y_MAX = 502000

    # Basic requirements
    Attribute_exists = {
        "msg": "attribute should exist"
    }
    Sourcevalue_exists = {
        "msg": "source value should exist"
    }
    Reference_exists = {
        "msg": "referenced entity should exist"
    }
    Unique_destination = {
        "msg": "destination is ambiguous"
    }

    # Format requirements
    Format_N8 = {
        "pattern": "^\d{8}$",
        "msg": "value should consist of 8 numeric characters",
    }
    Format_numeric = {
        "pattern": "^\d+$",
        "msg": "value should be a valid positive integer",
    }
    Format_alphabetic = {
        "pattern": "^[^0-9]+$",
        "msg": "value should only contain characters",
    }
    Format_ANN = {
        "pattern": "^[a-zA-Z]{1}\d{2}$",
        "msg": "value should be 1 character and 2 digits",
    }
    Format_AANN = {
        "pattern": "^[a-zA-Z]{2}\d{2}$",
        "msg": "value should be 2 characters and 2 digits",
    }
    Format_AAN_AANN = {
        "pattern": "(.*)/([a-zA-Z]{2}\d{1,2}).(.*)",
        "msg": "value should be 2 characters and 1 or 2 digits",
    }
    Format_4_2_2_2_6_HEX = {
        "pattern": "^{[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}}$",
        "msg": "value should be a 4-2-2-2-6 bytes hexidecimal value",
    }

    # Value requirements
    Value_1_2_3 = {
        "pattern": "^[1,2,3]$",
        "msg": "value should be one of [1,2,3]",
    }
    Value_wind_direction_NOZW = {
        "pattern": "^(N|NO|O|ZO|Z|ZW|W|NW)$",
        "msg": "value should be one of [N,NO,O,ZO,Z,ZW,W,NW]",
    }
    Value_1_0 = {
        "pattern": "^[1,0]$",
        "msg": "value should be one of 1 or 0",
    }
    Value_J_N = {
        "pattern": "^[J,N]$",
        "msg": "value should be one of J or N",
    }
    Value_height_6_15 = {
        "between": [-6, 15],
        "msg": "value should be a numeric value between -6 and 15",
    }
    Value_geometry_in_NL = {
        "geometry": {
            'x': {
                'min': _NL_X_MIN,
                'max': _NL_X_MAX,
            },
            'y': {
                'min': _NL_Y_MIN,
                'max': _NL_Y_MAX,
            }
        },
        "msg": f"value should be between {_NL_X_MIN}-{_NL_X_MAX} and {_NL_Y_MIN}-{_NL_Y_MAX}",
    }
    Value_not_empty = {
        "pattern": "^.+$",
        "msg": "value should not be empty",
    }
    Value_brondocument_coding = {
        "pattern": "^[a-zA-Z]{2}[0-9]{8}_[a-zA-Z]{2}[0-9]{2}[a-zA-Z]{2}\.[a-zA-Z]{3}",
        "msg": "brondocument does not match required coding",
    }
    Value_not_in_future = {
        "msg": "value should not be in the future",
    }
    Value_not_after = {
        "msg": "value should not be after"
    }
    Value_unique = {
        "msg": 'value should be unique within the collection'
    }
    Value_empty_once = {
        "msg": 'empty value should occur max once per entity'
    }
    Value_should_match = {
        "msg": 'value should match'
    }
    Value_1_1_reference = {
        "msg": 'value should be a one to one reference'
    }


# Initialisation of QA Checks
for check_name in [item for item in dir(QA_CHECK) if not item.startswith("__")]:
    # For each Quality Check
    check = getattr(QA_CHECK, check_name)
    if isinstance(check, dict):
        # Set the id attribute to the name of the check
        check['id'] = check_name
