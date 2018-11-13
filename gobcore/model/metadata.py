"""GOB metadata

Definition of the attributes that are automatically added to each GOB entity.

The auto-included attributes have the purpose of
    registering the last application of a specific event, e.g. date_confirmed
    registering the source of the data; source and source_id
    provide for a internal id and universal id reference; gobid and id

"""

"""Meta data that is registered for every entity"""
METADATA_COLUMNS = {
    # Public meta information about each row in a table
    "public": {
        "_version": "GOB.String",
        "_date_created": "GOB.DateTime",
        "_date_confirmed": "GOB.DateTime",
        "_date_modified": "GOB.DateTime",
        "_date_deleted": "GOB.DateTime",
    },

    # These properties will not be made public by the API
    # Note:
    # the _last_event property is a unique identification of the "state" of the entity
    # This property is used when deriving events out of a full update
    # The _last_event property tells with which state of the entity the comparison has been made
    # On applying the event this state is compared with the current state
    # Only when these two match the event will be applied, otherwise the event will be rejected.
    "private": {
        "_source": "GOB.String",  # Source for the specific entity, eg the name of a database
        "_source_id": "GOB.String",  # Id of the entity in the above mentioned source
        "_last_event": "GOB.String"  # Id of the last event that has been applied to this entity
    }
}

"""Columns that are at the start of each entity"""
FIXED_COLUMNS = {
    "_gobid": "GOB.PKInteger",  # The internal (GOB) id of the entity
    "_id": "GOB.String"         # Provide for a generic (independent from Stelselpedia) id field for every entity
}

PRIVATE_META_FIELDS = {
        field_name: {
            "type": field_type,
            "description": ""
        } for field_name, field_type in METADATA_COLUMNS["private"].items()
    }

PUBLIC_META_FIELDS = {
        field_name: {
            "type": field_type,
            "description": ""
        } for field_name, field_type in METADATA_COLUMNS["public"].items()
    }
