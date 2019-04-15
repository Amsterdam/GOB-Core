"""GOB metadata

Definition of the attributes that are automatically added to each GOB entity.

The auto-included attributes have the purpose of
    registering the last application of a specific event, e.g. date_confirmed
    registering the source of the data; source and source_id
    provide for a internal id and universal id reference; gobid and id

"""


# Field names
class FIELD:
    START_VALIDITY = "begin_geldigheid"
    END_VALIDITY = "eind_geldigheid"
    VERSION = "_version"
    DATE_CREATED = "_date_created"
    DATE_CONFIRMED = "_date_confirmed"
    DATE_MODIFIED = "_date_modified"
    DATE_DELETED = "_date_deleted"
    SOURCE = "_source"
    ID = "_id"
    APPLICATION = "_application"
    SOURCE_ID = "_source_id"
    LAST_EVENT = "_last_event"
    GOBID = "_gobid"
    SEQNR = "volgnummer"
    REGISTRATION_DATE = "registratiedatum"
    HASH = "_hash"
    EXPIRATION_DATE = "_expiration_date"


"""Description of all fields that are automatically added to each entity"""
DESCRIPTION = {
    FIELD.VERSION: "",
    FIELD.DATE_CREATED: "Timestamp telling when the entity has last been created in GOB.",
    FIELD.DATE_CONFIRMED: "Timestamp telling when the entity has last been confirmed in GOB.",
    FIELD.DATE_MODIFIED: "Timestamp telling when the entity has last been modified in GOB.",
    FIELD.DATE_DELETED: "Timestamp telling when the entity has last been deleted in GOB.",
    FIELD.SOURCE: "Functional source for the specific entity, eg the name of a department.",
    FIELD.ID: "Functional source id, a generic (independent from Stelselpedia) id field for every entity.",
    FIELD.APPLICATION: "Technical source for the specific entity, eg the name of a database.",
    FIELD.SOURCE_ID: "Technical source id, id of the entity in the above mentioned source application.",
    FIELD.LAST_EVENT: "Id of the last event that has been applied to this entity.",
    FIELD.GOBID: "The internal GOB id of the entity.",
    FIELD.SEQNR: "Uniek volgnummer van de toestand van het object.",
    FIELD.REGISTRATION_DATE: "De datum waarop de toestand is geregistreerd.",
    FIELD.HASH: "A hash of the values of all public fields for comparison",
    FIELD.EXPIRATION_DATE: "Timestamp telling when the entity will be or is expired.",
}

"""Meta data that is registered for every entity"""
METADATA_COLUMNS = {
    # Public meta information about each row in a table
    "public": {
        FIELD.VERSION: "GOB.String",
        FIELD.DATE_CREATED: "GOB.DateTime",
        FIELD.DATE_CONFIRMED: "GOB.DateTime",
        FIELD.DATE_MODIFIED: "GOB.DateTime",
        FIELD.DATE_DELETED: "GOB.DateTime",
        FIELD.EXPIRATION_DATE: "GOB.DateTime",
    },

    # These properties will not be made public by the API
    # Note:
    # the _last_event property is a unique identification of the "state" of the entity
    # This property is used when deriving events out of a full update
    # The _last_event property tells with which state of the entity the comparison has been made
    # On applying the event this state is compared with the current state
    # Only when these two match the event will be applied, otherwise the event will be rejected.
    "private": {
        FIELD.SOURCE: "GOB.String",
        FIELD.APPLICATION: "GOB.String",
        FIELD.SOURCE_ID: "GOB.String",
        FIELD.LAST_EVENT: "GOB.Integer",
        FIELD.HASH: "GOB.String",
    }
}

"""Columns that are at the start of each entity"""
FIXED_COLUMNS = {
    FIELD.GOBID: "GOB.PKInteger",
    FIELD.ID: "GOB.String"
}

"""Columns that are part of entities that have states"""
STATE_COLUMNS = {
    FIELD.SEQNR: "GOB.String",
    FIELD.REGISTRATION_DATE: "GOB.DateTime"
}


def columns_to_fields(columns, descriptions):
    return {
        field_name: {
            "type": field_type,
            "description": descriptions[field_name]
        } for field_name, field_type in columns.items()
    }


PRIVATE_META_FIELDS = columns_to_fields(METADATA_COLUMNS["private"], DESCRIPTION)
PUBLIC_META_FIELDS = columns_to_fields(METADATA_COLUMNS["public"], DESCRIPTION)
FIXED_FIELDS = columns_to_fields(FIXED_COLUMNS, DESCRIPTION)
STATE_FIELDS = columns_to_fields(STATE_COLUMNS, DESCRIPTION)
