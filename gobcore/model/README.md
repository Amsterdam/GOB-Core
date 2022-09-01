# ID’s within GOB

## Different id’s exist within GOB:
- source
- application
- source_id
- entity_id, _id, identificatie
- entity_source_id

### Source (\_source)
The **functional source** for the given entity. Usually the value is the name of a department, company or organisation.
Examples are AMSBI, CBS, …

### Application (\_application)
The **technical source** (application) within a source that has delivered the contents of the entity.
Within a source different applications may exist that deliver data, and applications.
These values can change during the lifetime of an entity.
Examples are DIVA, DGDialog, …

### Source ID (\_source\_id)
The **technical source id**.
The application that delivers data may use ID’s that are different from the functional ID’s in the GOB Model.
The combination of application and source_id uniquely identify the provenance of the contents of an entity.

### Entity ID, ID, identificatie (\_id)
The **functional source id**.
For every entity in the GOB Model an attribute is defined as its functional identification.
The name of this attribute in the GOB Model definition is “_entity_id”.
This name is quite often identical to “identification” but this is not required.
This is just a convention within the collection/entities in GOB Model.

In order to refer to entities by their functional ID a universal attribute “_id” is available.
For every entity in GOB the `_id` attribute functionally identifies the entity.

### Entity\_source\_id (\_entity\_source\_id)
Events in GOB are applied to entities.
Over time the technical source for an entity may change.
The entity however remains the same entity.
This means that the source for an entity is constant during its lifetime but the application and source_id may change.

Upon compare the entity to update is searched on its **functional id** (source and entity_id).
When an existing entity is found its **technical id** (source\_id) is stored in the event as **entity\_source\_id**

Upon application of the event the source and entity\_source\_id are used to locate the entity to update.

## States
States can be regarded as explicitly stored entity history.
Instead of storing entity modifications in events, entities are duplicated on each modification.
Each new duplication is identified by a volgnummer and a registration date.

Within GOB entity with states are guaranteed to have a volgnummer and registratiedatum.
Normally they also have a begin and end validity dates (begin\_geldigheid and eind\_geldigheid).

The actual value of an entity with state is **not** necessarily the entity with the highest volgnummer.
Volgnummers may refer to already closed time spans.
The actual value is the entity with **eind\_geldigheid equal to null**.

Entity with states are marked in the GOB Model definition by the attribute “has_states”.
When this value is set to true the processing of entities slightly changes.

Source, application and entity id remain unchanged.

To uniquely identify the entity within the source application the volgnummer is added to the source\_id.
