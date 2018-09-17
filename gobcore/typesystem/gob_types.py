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
    GOBType is an abstract base class, why not subclass from ABC?

todo:
    think about the tight coupling with SQLAlchemy, is that what we want?

"""
import sqlalchemy
import datetime

from pandas import pandas


class GOBType():
    is_pk = False
    is_composite = False
    name = "type"
    sql_type = sqlalchemy.Column

    @classmethod
    def equals(cls, one, other):
        return cls.to_db(one) == cls.to_db(other)

    @classmethod
    def to_db(cls, value):
        return value

    @classmethod
    def to_str_or_none(cls, value):
        """
        Returns value as a string
        :param value:
        :return: the string that holds the value or None
        """
        return str(value) if not pandas.isnull(value) else None

    @classmethod
    def get_column_definition(cls, column_name):
        return sqlalchemy.Column(column_name, cls.sql_type, primary_key=cls.is_pk, autoincrement=cls.is_pk)


class String(GOBType):
    name = "string"
    sql_type = sqlalchemy.String


class Character(GOBType):
    name = "character"
    sql_type = sqlalchemy.CHAR

    @classmethod
    def to_str_or_none(cls, value):
        """
        Returns value as a string containing a single character value
        :param value:
        :return: the string that holds the value in single character format or None
        """
        to_str = super().to_str_or_none(value)
        return to_str[0] if to_str is not None else None


class Integer(GOBType):
    name = "integer"
    sql_type = sqlalchemy.Integer


class PKInteger(Integer):
    is_pk = True


class Decimal(GOBType):
    name = "decimal"
    sql_type = sqlalchemy.DECIMAL

    @classmethod
    def to_str_or_none(cls, value):
        """
        Returns value as a string containing a decimal value
        :param value:
        :return: the string that holds the value in decimal format or None
        """
        to_str = super().to_str_or_none(value)
        return to_str.strip().replace(",", ".") if to_str is not None else None


class Number(GOBType):
    name = "number"
    sql_type = sqlalchemy.NUMERIC

    @classmethod
    def to_str_or_none(cls, value):
        """
        Returns value as a string containing an integer value
        :param value:
        :return: the string that holds the value in integer format or None
        """
        decimal_str = Decimal.to_str_or_none(value)
        return str.split(decimal_str, ".")[0] if decimal_str is not None else None


class Date(GOBType):
    name = "date"
    sql_type = sqlalchemy.Date

    @classmethod
    def to_str_or_none(cls, value):
        """
        Returns value as a string containing a date value in ISO 8601 format
        :param value:
        :return: the string that holds the value in data or None
        """
        to_str = super().to_str_or_none(value)
        return datetime.datetime.strptime(to_str, "%Y%m%d").date().strftime("%Y-%m-%d") \
            if to_str is not None else None


class DateTime(GOBType):
    name = "datetime"
    sql_type = sqlalchemy.DateTime


class JSON(GOBType):
    name = "json"
    sql_type = sqlalchemy.JSON


class Boolean(GOBType):
    name = "boolean"
    sql_type = sqlalchemy.Boolean

    @classmethod
    def to_db(cls, value):
        return value == str(True)

    @classmethod
    def to_str_or_none(cls, value):
        """
        Returns value as a string containing a boolean value, default "True"

        Todo:
            Determine if the default value True is OK

        :param value:
        :return: the string that holds the boolean value
        """
        return str(False) if value == 'N' else str(True)
