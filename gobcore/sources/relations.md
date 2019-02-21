# GOB Relations

Relations in GOB are modeled by means of source values.

Example:

  * **Referentiepunt** is related to **Bouwblok** by means of the bouwblok code (Grondslag import)

During the **Relate** process the bouwblok collection is searched for the specific bouwblok code and its identification (_id) is registered within the Referentiepunt.

This looks as follows:

## Import:

    referentiepunt = {
      ...
      _id: 1,
      ligt_in_bouwblok: {
        bronwaarde: 'AA01'
      },
      ...
    }

## Relate:

    referentiepunt = {
      ...
      _id: 1,
      ligt_in_bouwblok: {
        bronwaarde: 'AA01',
        identificatie: '03630012096976'
      },
      ...
    }

Finally both the original value (bronwaarde) and the _id (identificatie) are registered in the relation.

## Destination has states

However, Bouwblok is an entity with state. Entities with state have a registration date, volgnummer and begin- and einddatum. Relations to entities with state are therefore by nature relations with a begin- and enddate

The relation should be:

      ...
      _id: 1,
      ligt_in_bouwblok: {
        bronwaarde: 'AA01',
        identificaties: [
          {
            identificatie: '03630012096976',
            begin_geldigheid: t0,
            eind_geldigheid: t1
          },
          ...
        ]
      },
      ...
    }

Note that multiple identificaties may exist for an entity. When relations are sorted on most recent begin_geldigheid then identificaties[0]identificatie is the most recent value for the relation.

## Source has states

When an entity itself has a begin (t<sub>0</sub>) and eind_geldigheid (t<sub>1</sub>) then only the entities in the other collection that have a begin_geldigheid <= t<sub>1</sub> and einddatum >= t<sub>0</sub> should be considered.

The most recent value of every timespan should be used.

## Database

The storage of entity relations within the entity itself is not optimal.

Efficient updates require answers to questions like:

  * Given an entity, get all entities that link to it

    When the entity is updated all relations to it should be recalculated

  * Given a begin and end date, get all relations within the timespan

    Mainly used for export products

The following database structure is therefor proposed (with demo data from the example):

    {
        src_catalog: 'meetbouten'
        src_collection: 'referentiepunten'
        src_attribute: 'ligt_in_bouwblok'
        src_id: 1
        dst_catalog: 'gebieden'
        dst_collection: 'bouwblokken'
        dst_id: '03630012096976'
        begin_geldigheid: t0
        eind_geldigheid: t1
    }

The entity can remain simple:


    {
      ...
      ligt_in_bouwblok: {
        bronwaarde: 'AA01'
      },
      ...
    }

To get the complete data of an entity it has to be joined with its relations.

Relations become like regular entities within GOB. They are updated using events and can be tracked over time. The regular entity attributes are part of each relation.

    gobid             GOB unique internal id
    _id               catalog.collection.id.attribute, eg gebieden.wijken.036XXX.ligt_in_stadsdeel
    _version          0.1
    _date_created
    _date_confirmed
    _date_modified
    _date_deleted
    _source           
    _application      
    _source_id        _id.volgnummer        
    _last_event       last event that has "touched" the relation
    _hash             hash of the relation to allow for fast compares

Relations can be a regular part of GOB by defining its structure in GOB Model. It's catalog can be set to gob and the collection to relations. The database table name becomes gob_relations.

GOB Relations can be retrieved via the API like any other GOB entity.

## Example

BAG-Adressen:

    {
        _id: 1
        ligt_in_wijk: {
            bronwaarde: 'W1'
        },
        volgnummer: 1,
        begin_geldigheid: 1-1-2001,
        eind_geldigheid: 1-1-2007
    }
	
    {
        _id: 1
        ligt_in_wijk: {
            bronwaarde: 'W3'
        },
        volgnummer: 2,
        begin_geldigheid: 1-1-2007,
        eind_geldigheid: NULL
    }

Gebieden-Wijken:

    {
        _id: 1,
        code: 'W1',
        volgnummer: 1,
        begin_geldigheid: 1-1-2000,
        eind_geldigheid: 1-1-2003
    }
	
    {
        _id: 2,
        code: 'W1',
        volgnummer: 1,
        begin_geldigheid: 1-1-2003,
        eind_geldigheid: 1-1-2006
    }
	
    {
        _id: 1,
        code: 'W1',
        volgnummer: 2,
        begin_geldigheid: 1-1-2006,
        eind_geldigheid: NULL
    }
	
    {
        _id: 3,
        code: 'W3',
        volgnummer: 1,
        begin_geldigheid: 1-1-2000,
        eind_geldigheid: 1-1-2009
    }
	

GOB-Relations:

    {
        _id: 'bag.adressen.1.ligt_in_wijk'
        src_catalog: 'bag'
        src_collection: 'adressen'
        src_attribute: 'ligt_in_wijk'
        src_id: 1
        dst_catalog: 'gebieden'
        dst_collection: 'wijken'
        dst_id: 1
        volgnummer: int(1-1-2001)
        registratiedatum: now
        begin_geldigheid: 1-1-2001
        eind_geldigheid: 1-1-2003
    }

    {
        _id: 'bag.adressen.1.ligt_in_wijk'
        src_catalog: 'bag'
        src_collection: 'adressen'
        src_attribute: 'ligt_in_wijk'
        src_id: 1
        dst_catalog: 'gebieden'
        dst_collection: 'wijken'
        dst_id: 2
        volgnummer: int(1-1-2003)
        registratiedatum: now
        begin_geldigheid: 1-1-2003
        eind_geldigheid: 1-1-2006
    }

    {
        _id: 'bag.adressen.1.ligt_in_wijk'
        src_catalog: 'bag'
        src_collection: 'adressen'
        src_attribute: 'ligt_in_wijk'
        src_id: 1
        dst_catalog: 'gebieden'
        dst_collection: 'wijken'
        dst_id: 1
        volgnummer: int(1-1-2006)
        registratiedatum: now
        begin_geldigheid: 1-1-2006
        eind_geldigheid: 1-1-2007
    }

    {
        _id: 'bag.adressen.1.ligt_in_wijk'
        src_catalog: 'bag'
        src_collection: 'adressen'
        src_attribute: 'ligt_in_wijk'
        src_id: 1
        dst_catalog: 'gebieden'
        dst_collection: 'wijken'
        dst_id: 3
        volgnummer: int(1-1-2007)
        registratiedatum: now
        begin_geldigheid: 1-1-2007
        eind_geldigheid: 1-1-2009
    }

    {
        _id: 'bag.adressen.1.ligt_in_wijk'
        src_catalog: 'bag'
        src_collection: 'adressen'
        src_attribute: 'ligt_in_wijk'
        src_id: 1
        dst_catalog: 'gebieden'
        dst_collection: 'wijken'
        dst_id: NULL
        volgnummer: int(1-1-2009)
        registratiedatum: now
        begin_geldigheid: 1-1-2009
        eind_geldigheid: NULL
    }

