EVENTS_DESCRIPTION = {
    "eventid": "Unique identification of the event, numbered sequentially",
    "timestamp": "Datetime when the event as created",
    "catalogue": "The catalogue in which the entity resides",
    "entity": "The entity to which the event need to be applied",
    "tid": "The tid (combination _id and volgnummer, if applicable) of the entity this event refers to",
    "version": "The version of the entity model",
    "action": "Add, change, delete or confirm",
    "source": "The functional source of the entity, e.g. AMSBI",  # Deprecated. Remove later
    "application": "The technical source of the entity, e.g. DIVA",
    "source_id": "The id of the entity in the source",  # Deprecated. Remove later
    "contents": "A json object that holds the contents for the action, the full entity for an Add",
}

EVENTS = {
    "eventid": "GOB.PKInteger",
    "timestamp": "GOB.DateTime",
    "catalogue": "GOB.String",
    "entity": "GOB.String",
    "tid": "GOB.String",
    "version": "GOB.String",
    "action": "GOB.String",
    "source": "GOB.String",  # Deprecated. Remove later
    "application": "GOB.String",
    "source_id": "GOB.String",  # Deprecated. Remove later
    "contents": "GOB.JSON",
}
