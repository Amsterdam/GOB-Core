"""GOB Types.

Each possible data type in GOB is defined in this module.
The definition includes:
    name - the name of the data type, e.g. string
    sql_type - the corresponding storage type, e.g. sqlalchemy.String
    is_pk - whether the attribute that has this type is the primary key of the entity

todo:
    is_pk tells something about the attribute, not about the type.
    should is_pk stay a type property?

todo:
    think about the tight coupling with SQLAlchemy, is that what we want?
"""


import datetime
import decimal
import json
import numbers
import re
from abc import ABCMeta, abstractmethod
from math import isnan
from typing import Any, Optional

import sqlalchemy

from gobcore.exceptions import GOBTypeException
from gobcore.model.metadata import FIELD


def get_kwargs_from_type_info(type_info: dict[str, Any]) -> dict[str, Any]:
    """Return kwargs dictionary from GOB Model field type info."""
    # Collect special keys like 'precision'.
    type_kwargs = {
        key: value for key, value in type_info.items() if key not in ["type", "gob_type", "description", "ref"]
    }
    return type_kwargs


class GOBType(metaclass=ABCMeta):
    """Abstract Base Class for GOB Types.

    Use as follows:

        # instantiates an instance of a GOB Type (for instance String)
        #   Instantiating can only be done with a string, because the internal representation of a value is string
        gob_type = String(string)

        # generates an instance of GOB Type (for instance String) when you have other values:
        gob_type1 = String.from_value(int)
        gob_type2 = String.from_value(bool)

        # gob type store internal representation as a string, the `str()` method exposes that
        $ str(False) == str(Boolean.from_value('N', format='YN'))
        True

        # use comparison like this:
        gob_type1 == gob_type2  # True if both are None or str(gob_type1) == str(gob_type2)

    """
    is_pk = False
    is_composite = False
    is_secure = False
    name = "type"
    sql_type = sqlalchemy.Column

    @abstractmethod
    def __init__(self, value):
        """Initialisation of GOBType with a string value

        :param value: the string value for internal representation
        :return: GOBType
        """
        if value is not None and not isinstance(value, str):
            raise GOBTypeException("GOBType can only be instantiated with string, "
                                   "use `GOBType.from_value(value)` instead")
        self._string = value if value is None else str(value)

    def __str__(self):
        """Internal representation is string, that is what we return

        :return: String representation of the GOBType instance
        """
        return str(self._string)

    def __eq__(self, other):
        """Internal representation is string, that is what we compare

        :param other: other GOB Type to compare with
        :return: True or False
        """
        # todo: same type?
        # When string is a NoneType, compare it as a string to other
        if self._string is None and isinstance(other, GOBType):
            return self.json == other.json

        return self._string == str(other) if isinstance(other, GOBType) else self._string == other

    @classmethod
    def from_value_secure(cls, value, type_info, **kwargs):
        """Mapper around cls.from_value to handle (secure) GOBType values.

        The type information is used to protect the value with the right confidence level.

        :param value: the value of the GOBType instance
        :param type_info: the GOB Model field type information for the given value
        :param kwargs:
        :return: GOBType
        """
        if cls.is_secure:
            # Secure types require a confidence level
            kwargs["level"] = type_info["level"]
        # GOB.JSON
        if isinstance(value, dict):
            # Attributes are either defined in the 'secure' dict or the 'attributes' dict. Pass either.
            if type_info.get("secure"):
                kwargs["secure"] = type_info["secure"]
            elif type_info.get("attributes"):
                kwargs["attributes"] = type_info["attributes"]
        else:
            # Update kwargs with GOB Model field type info dict.
            kwargs = get_kwargs_from_type_info(type_info) | kwargs
        return cls.from_value(value, **kwargs)

    @classmethod
    @abstractmethod
    def from_value(cls, value, **kwargs):
        """Classmethod GOBType constructor, able to ingest multiple types of values

        :param value: the value of the GOBType instance
        :return: GOBType
        """
        pass  # pragma: no cover

    @property
    @abstractmethod
    def json(self):
        """JSON string representation of the GOBType instance for ContentsWriter.

        :return: JSON String
        """
        pass  # pragma: no cover

    @property
    @abstractmethod
    def to_db(self):
        """DB storable representation of the GOBType instance

        :return: DB Storable object
        """
        pass  # pragma: no cover

    @property
    @abstractmethod
    def to_value(self):
        """Python object of the GOBType instance

        :return: Python object
        """
        pass  # pragma: no cover

    @classmethod
    def get_column_definition(cls, column_name, **kwargs):
        """Returns the SQL Alchemy column definition for the type """
        return sqlalchemy.Column(column_name, cls.sql_type, primary_key=cls.is_pk, autoincrement=cls.is_pk)


class String(GOBType):
    """GOBType class for GOB.String."""

    name = "String"
    sql_type = sqlalchemy.String

    def __init__(self, value):
        super().__init__(value)

    @classmethod
    def from_value(cls, value, **kwargs) -> GOBType:
        if isinstance(value, numbers.Number) and isnan(value):
            value = None
        return cls(str(value)) if value is not None else cls(value)

    @property
    def json(self):
        return json.dumps(self._string)

    @property
    def to_db(self):
        return self._string

    @property
    def to_value(self):
        return self._string


class Character(String):
    """GOBType class for GOB.Character."""

    name = "Character"
    sql_type = sqlalchemy.CHAR

    @classmethod
    def from_value(cls, value, **kwargs) -> GOBType:
        """
        Returns GOBType as a String containing a single character value if input has a string representation with
        len > 0, else a GOBType(None)

        :param value:
        :return: the string that holds the value in single character format or None
        """
        if value is None:
            return cls(None)
        string_value = str(value)
        if len(string_value) != 1:
            raise GOBTypeException(f"value '{string_value}' has more than one character")
        return cls(string_value[0]) if len(string_value) > 0 else cls(None)


class Integer(String):
    """GOBType class for GOB.Integer."""

    name = "Integer"
    sql_type = sqlalchemy.Integer

    def __init__(self, value):
        if value == 'nan':
            value = None
        if value is not None:
            try:
                value = str(int(value))
            except ValueError:
                raise GOBTypeException(f"value '{value}' cannot be interpreted as Integer")
        super().__init__(value)

    @property
    def json(self):
        return json.dumps(int(self._string)) if self._string is not None else json.dumps(None)

    @property
    def to_db(self):
        if self._string is None:
            return None
        return int(self._string)

    @property
    def to_value(self):
        return int(self._string) if self._string else None


class BigInteger(Integer):
    """GOBType class for GOB.BigInteger."""

    name = "BigInteger"
    sql_type = sqlalchemy.BigInteger


class PKInteger(Integer):
    """GOBType class for GOB.PKInteger."""

    name = "PKInteger"
    is_pk = True


class Decimal(GOBType):
    """GOBType class for GOB.Decimal."""

    name = "Decimal"
    sql_type = sqlalchemy.DECIMAL

    def __init__(self, value: Optional[str], **kwargs) -> None:  # noqa: C901
        if value == 'nan':
            value = None
        if value is not None:
            try:
                if "precision" in kwargs:
                    # Set Decimal precision
                    fmt = f".{kwargs['precision']}f"
                    value = format(decimal.Decimal(value), fmt)
                else:
                    # Preserve Decimal precision
                    value = str(decimal.Decimal(value))
            except (ValueError, decimal.InvalidOperation) as exc:
                raise GOBTypeException(f"value '{value}' cannot be interpreted as Decimal: {exc}")
        super().__init__(value)

    @classmethod
    def from_value(cls, value, **kwargs):
        """Create a Decimal GOB Type from value and kwargs.

            Decimal.from_value("123.0", decimal_separator='.', **get_kwargs_from_type_info(type_info))

        For now decimal separator is optional - setting it would make sense in import,
        but not in transfer and comparison
        """
        decimal_separator_internal = '.'
        input_decimal_separator = kwargs['decimal_separator'] if 'decimal_separator' in kwargs else '.'

        if value is None:
            return cls(None)
        string_value = str(value).strip().replace(input_decimal_separator, decimal_separator_internal)
        return cls(string_value, **kwargs)

    @property
    def json(self):
        return json.dumps(self._string) if self._string is not None else json.dumps(None)

    @property
    def to_db(self):
        if self._string is None:
            return None
        return decimal.Decimal(self._string)

    @property
    def to_value(self):
        return self._string

    @classmethod
    def get_column_definition(cls, column_name, **kwargs):
        """Returns the SQL Alchemy column definition for the type """
        type_kwargs = {}
        if "precision" in kwargs:
            type_kwargs |= {
                "scale": kwargs["precision"],
                "precision": kwargs.get("length", 10),
            }
        return sqlalchemy.Column(
            column_name,
            cls.sql_type(**type_kwargs),
            primary_key=cls.is_pk,
            autoincrement=cls.is_pk
        )


class Boolean(GOBType):
    """GOBType class for GOB.Boolean."""

    name = "Boolean"
    sql_type = sqlalchemy.Boolean

    def __init__(self, value):
        if value is not None:
            if value.lower() not in ['true', 'false']:
                raise GOBTypeException("Boolean should be False, True or None")
        super().__init__(value)

    @classmethod
    def from_value(cls, value, **kwargs):
        """Create a Boolean GOB Type from value and kwargs.

            Boolean.from_value("N", format="YN")

        Formatting might be required to enable interpretation of the value during import. And can be one of:

            - 'YN', 'Y' --> True, 'N' --> False, Other/None --> None
            - 'JN', 'J' --> True, 'N' --> False, Other/None --> None
            - '10', '1' --> True, '0' --> False, Other/None --> None
        """
        known_formats = ['YN', 'JN', '10']

        if 'format' in kwargs:
            if kwargs['format'] not in known_formats:
                raise GOBTypeException(f"Unknown boolean formatting: '{kwargs['format']}'")
            value = cls._bool_or_none(value, kwargs['format'])

        return cls(str(value)) if value is not None else cls(None)

    @classmethod
    def _bool_or_none(cls, value, format):
        if value is None or str(value) not in format:
            return None
        if str(value) == format[0]:
            return True
        if str(value) == format[1]:
            return False
        return None

    @property
    def json(self):
        return json.dumps(self._bool()) if self._string is not None else json.dumps(None)

    @property
    def to_db(self):
        return self._bool()

    @property
    def to_value(self):
        return self._bool()

    def _bool(self):
        if self._string is not None:
            if self._string.lower() == str(True).lower():
                return True
            elif self._string.lower() == str(False).lower():
                return False
        return None


class Date(String):
    """GOBType class for GOB.Date."""

    name = "Date"
    sql_type = sqlalchemy.Date
    internal_format = "%Y-%m-%d"

    @classmethod
    def from_value(cls, value, **kwargs):
        """ Create a Date GOB type as a string containing a date value in ISO 8601 format:

            Date.from_value("20160504", format="%Y%m%d")

        In which `format` is the datetime formatting string of the input - usually used on import
        """
        input_format = kwargs['format'] if 'format' in kwargs else cls.internal_format

        if value is not None:
            try:
                value = datetime.datetime.strptime(str(value), input_format).date()
                # Transform to internal string format and work around issue: https://bugs.python.org/issue13305
                value = f"{value.year:04d}-" + value.strftime("%m-%d")
            except ValueError as v:
                raise GOBTypeException(v)

        return cls(str(value)) if value is not None else cls(None)

    @property
    def to_db(self):
        if self._string is None:
            return None
        return datetime.datetime.strptime(self._string, self.internal_format)

    @property
    def to_value(self):
        if self._string is None:
            return None
        return datetime.datetime.strptime(self._string, self.internal_format).date()


class DateTime(Date):
    """GOBType class for GOB.DateTime."""

    name = "DateTime"
    sql_type = sqlalchemy.DateTime
    internal_format = "%Y-%m-%dT%H:%M:%S.%f"

    def __init__(self, value):
        super().__init__(value)

    @classmethod
    def from_value(cls, value, **kwargs):
        input_format = kwargs['format'] if 'format' in kwargs else cls.internal_format

        if value is not None:
            try:
                if not isinstance(value, datetime.datetime):
                    if isinstance(value, str) and '.%f' in input_format and len(value) == len('YYYY-MM-DDTHH:MM:SS'):
                        # Add missing microseconds if needed
                        value += '.000000'
                    value = datetime.datetime.strptime(str(value), input_format)
                # Transform to internal string format and work around issue: https://bugs.python.org/issue13305
                value = f"{value.year:04d}-" + value.strftime("%m-%dT%H:%M:%S.%f")
            except ValueError as v:
                raise GOBTypeException(v)

        return cls(str(value)) if value is not None else cls(None)

    @property
    def to_db(self):
        if self._string is None:
            return None
        return datetime.datetime.strptime(self._string, self.internal_format)

    @property
    def to_value(self):
        if self._string is None:
            return None
        return datetime.datetime.strptime(self._string, self.internal_format)


class JSON(GOBType):
    """GOBType class for GOB.JSON."""

    name = "JSON"
    sql_type = sqlalchemy.dialects.postgresql.JSONB

    def __init__(self, value, spec=None):
        if value is not None:
            try:
                # force sort keys to have order
                value = json.dumps(json.loads(value), sort_keys=True)
                self._spec = spec
            except ValueError:
                raise GOBTypeException(f"value '{value}' cannot be interpreted as JSON")
        super().__init__(value)

    def _process_get_dict_value(self, value, user):
        for attr, attr_value in value.items():
            if isinstance(attr_value, dict):
                self._process_get_dict_value(attr_value, user)
            elif self._spec and self._spec.get(attr):
                # Resolve secure attributes
                level = self._spec[attr].get('level')
                value[attr] = self._spec[attr]['gob_type'](attr_value, level=level).get_value(user)

    def _process_get_value(self, value, user):
        if isinstance(value, dict):
            self._process_get_dict_value(value, user)
        elif isinstance(value, list):
            for item in value:
                self._process_get_value(item, user)

    def get_value(self, user=None):
        if self._string is None:
            return None

        value = json.loads(self._string)
        self._process_get_value(value, user)
        return value

    @classmethod
    def _process_from_value(cls, value, attributes):
        """Recurse into dict (value) to add field keys with values.

        :param value:
        :param attributes: Either the 'secure' dict or 'attributes' dict from the field definition
        :return:
        """
        for attr, attr_value in value.items():
            if isinstance(attr_value, dict):
                cls._process_from_value(attr_value, attributes)
            elif attributes.get(attr):
                type_info = attributes[attr]
                type_kwargs = get_kwargs_from_type_info(type_info)
                gob_type = type_info['gob_type']
                value[attr] = gob_type.from_value_secure(attr_value, type_info, **type_kwargs).to_value

    @classmethod
    def from_value(cls, value, **kwargs):
        """ Create a JSON GOB type as a string:

            if a dict of list is submitted, it gets dumped to json
            if a json string is submitted its keys get reorded for comparison
        """
        if value is None:
            return cls(None)

        # Take the secure attributes, or use the attributes dict when no secure attributes present.
        attributes = kwargs.get('secure', kwargs.get('attributes'))
        if isinstance(value, dict) and attributes:
            cls._process_from_value(value, attributes)

        if isinstance(value, dict) or isinstance(value, list):
            return cls(json.dumps(value), spec=attributes)

        return cls(str(value))

    @property
    def to_db(self):
        if self._string is None:
            return None
        return json.loads(self._string)

    @property
    def json(self):
        return self._string if self._string is not None else json.dumps(None)

    @property
    def to_value(self):
        if self._string is None:
            return None
        return json.loads(self._string)


class Reference(JSON):
    """GOBType class for GOB.Reference."""

    name = "Reference"
    exclude_keys = (FIELD.REFERENCE_ID, FIELD.SEQNR)  # Legacy. Old way of storing relations.

    def __eq__(self, other):
        """Internal representation is string, that is what we compare

        :param other: other GOB Type to compare with
        :return: True or False
        """
        cleaned_self = self._filter_reference(self._string)
        cleaned_other = self._filter_reference(other._string)

        return cleaned_self == cleaned_other

    def _filter_reference(self, value):
        if value is None:
            return value
        item = json.loads(value) if isinstance(value, str) else value
        return {k: v for k, v in item.items() if k not in self.exclude_keys}


class ManyReference(Reference):
    """GOBType class for GOB.ManyReference."""

    name = "ManyReference"

    def __eq__(self, other, exclude_keys=(FIELD.REFERENCE_ID, FIELD.SEQNR)):
        """Internal representation is string, that is what we compare

        :param other: other GOB Type to compare with
        :return: True or False
        """
        cleaned_self = self._filter_references(self._string)
        cleaned_other = self._filter_references(other._string)

        return cleaned_self == cleaned_other

    def _filter_references(self, value):
        if value is None:
            return value
        return [self._filter_reference(item) for item in json.loads(str(value))]


class VeryManyReference(ManyReference):
    """GOBType class for GOB.VeryManyReference."""

    name = "VeryManyReference"


class IncompleteDate(JSON):
    """GOBType class for GOB.IncompleteDate."""

    name = "IncompleteDate"
    pattern = r"^(\d{4})-(\d{2})-(\d{2})$"

    def __init__(self, value):
        """Value can be of type str or dict.
        If value is str, value is expected to be of the form yyyy-mm-dd, where the yyyy, mm and dd parts can consist of
        only 0's, meaning that part is unknown.
        If value is of type dict, keys 'year', 'month' and 'day' are expected to be present (but may be null).
        Dict may also be encoded as a JSON string.

        For example:
        2021-03-22 (complete date)
        2021-03-00 (day unknown)
        2021-00-00 (month and day unknown)

        :param value:
        """
        if value is not None:
            if isinstance(value, str) and re.match(self.pattern, value):
                self.year, self.month, self.day = self.__init_from_str(value)
            else:
                if isinstance(value, str):
                    try:
                        value = json.loads(value)
                    except json.JSONDecodeError:
                        raise GOBTypeException(f"Could not decode value '{value}'")

                    assert isinstance(value, dict), "Value should be of type dict"
                self.year, self.month, self.day = self.__init_from_dict(value)

        super().__init__(json.dumps({
            'year': self.year,
            'month': self.month,
            'day': self.day,
            'formatted': self._formatted,
        }) if value is not None else value)

    def __init_from_dict(self, value) -> tuple[Optional[int], Optional[int], Optional[int]]:
        if not all([v in value for v in ['year', 'month', 'day']]):
            raise GOBTypeException(f"Cannot interpret value '{json.dumps(value)}' as IncompleteDate. "
                                   f"Expecting keys 'year', 'month' and 'day'.")

        return value.get('year'), value.get('month'), value.get('day')

    def __init_from_str(self, value: str) -> tuple[int, int, int]:
        m = re.match(self.pattern, value)
        assert m, f"Value '{value}' cannot be interpreted as IncompleteDate"  # Already checked in constructor.

        return (int(m.group(1)) if m.group(1) != '0000' else None,
                int(m.group(2)) if m.group(2) != '00' else None,
                int(m.group(3)) if m.group(3) != '00' else None)

    @classmethod
    def from_value(cls, value, **kwargs):
        return cls(value)

    @property
    def _formatted(self):
        return f"{self.year or 0:04}-{self.month or 0:02}-{self.day or 0:02}"
