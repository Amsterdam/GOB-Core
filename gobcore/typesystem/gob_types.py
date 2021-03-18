"""GOB Types

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
from abc import ABCMeta, abstractmethod
import datetime
import json
import numbers
from math import isnan

import sqlalchemy

from gobcore.exceptions import GOBTypeException
from gobcore.model.metadata import FIELD


class GOBType(metaclass=ABCMeta):
    """Abstract baseclass for the GOB Types, use like follows:

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
    def from_value_secure(cls, value, typeinfo, **kwargs):
        """
        A mapper around from_value to handle secure data.

        The type information is used to protect the data with the right confidence level.

        :param value: the value of the GOBType instance
        :param typeinfo: the GOB Model type information for the given value
        :param kwargs:
        :return: GOBType
        """
        if cls.is_secure:
            # Secure types require a confidence level
            kwargs["level"] = typeinfo["level"]
        if isinstance(value, dict):
            # Attributes are either defined in the 'secure' dict or the 'attributes' dict. Pass either.
            if typeinfo.get("secure"):
                kwargs["secure"] = typeinfo["secure"]
            elif typeinfo.get("attributes"):
                kwargs["attributes"] = typeinfo["attributes"]
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
        """Json string representation of the GOBType instance

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
    def get_column_definition(cls, column_name):
        """Returns the SQL Alchemy column definition for the type """
        return sqlalchemy.Column(column_name, cls.sql_type, primary_key=cls.is_pk, autoincrement=cls.is_pk)


class String(GOBType):
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


class PKInteger(Integer):
    name = "PKInteger"
    is_pk = True


class Decimal(GOBType):
    name = "Decimal"
    sql_type = sqlalchemy.DECIMAL

    def __init__(self, value, **kwargs):  # noqa: C901
        if value == 'nan':
            value = None
        if value is not None:
            try:
                if 'precision' in kwargs:
                    fmt = f".{kwargs['precision']}f"
                    value = format(float(value), fmt)
                else:
                    value = str(float(value))
            except ValueError:
                raise GOBTypeException(f"value '{value}' cannot be interpreted as Decimal")
        super().__init__(value)

    @classmethod
    def from_value(cls, value, **kwargs):
        """ Create a Decimal GOB Type from value and kwargs:

            Decimal.from_value("123.0", decimal_separator='.')

        For now decemial separator is optional - setting it would make sense in import,
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
        return json.dumps(float(self._string)) if self._string is not None else json.dumps(None)

    @property
    def to_db(self):
        if self._string is None:
            return None
        return float(self._string)

    @property
    def to_value(self):
        return self._string


class Boolean(GOBType):
    name = "Boolean"
    sql_type = sqlalchemy.Boolean

    def __init__(self, value):
        if value is not None:
            if value.lower() not in ['true', 'false']:
                raise GOBTypeException("Boolean should be False, True or None")
        super().__init__(value)

    @classmethod
    def from_value(cls, value, **kwargs):
        """ Create a Decimal GOB Type from value and kwargs:

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
        """

        :param value:
        :param attributes: Either the 'secure' dict or 'attributes' dict from the field definition
        :return:
        """
        for attr, attr_value in value.items():
            if isinstance(attr_value, dict):
                cls._process_from_value(attr_value, attributes)
            elif attributes.get(attr):
                attribute = attributes[attr]
                gob_type = attribute['gob_type']
                type_kwargs = {k: v for k, v in attribute.items()
                               if k not in ['type', 'gob_type', 'source_mapping', 'filters']}
                value[attr] = gob_type.from_value_secure(attr_value, attribute, **type_kwargs).to_value

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
    name = "Reference"
    exclude_keys = (FIELD.REFERENCE_ID, FIELD.SEQNR)

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
    name = "VeryManyReference"
