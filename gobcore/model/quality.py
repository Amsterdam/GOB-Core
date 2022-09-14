QUALITY_CATALOG = "qa"


def _get_qa(catalog, collection):
    return {
        "version": "0.1",
        "abbreviation": _get_qa_abbreviation(catalog, collection),
        "entity_id": "meldingnummer",
        "attributes": {
            "meldingnummer": {
                "type": "GOB.String",
                "description": "Unieke ID van de bevinding"
            },
            "code": {
                "type": "GOB.String",
                "description": "Foutcode van de bevinding"
            },
            "proces": {
                "type": "GOB.String",
                "description": "het proces waarin de bevinding is geconstateerd"
            },
            "attribuut": {
                "type": "GOB.String",
                "description": "Het attribuut waarop de melding van toepassing is"
            },
            "identificatie": {
                "type": "GOB.String",
                "description": "Unieke identificatie van het object waarop de melding van toepassing op is"
            },
            "volgnummer": {
                "type": "GOB.Integer",
                "description": "Volgnummer van het object waarop de melding van toepassing is"
            },
            "begin_geldigheid": {
                "type": "GOB.DateTime",
                "description": "De datum waarop het object waarop de melding van toepassing is is gecreëerd."
            },
            "eind_geldigheid": {
                "type": "GOB.DateTime",
                "description": "De datum waarop het object waarop de melding van toepassing is is komen te vervallen."
            },
            "betwijfelde_waarde": {
                "type": "GOB.String",
                "description": "De waarde van het attribuut waar aan wordt getwijfeld"
            },
            "onderbouwing": {
                "type": "GOB.String",
                "description": "Een onderbouwing waarom de waarde van het attribuut niet correct is"
            },
            "voorgestelde_waarde": {
                "type": "GOB.String",
                "description": "De voorgestelde nieuwe waarde van het attribuut"
            }
        }
    }


def get_entity_name(catalog_name, collection_name):
    return f"{catalog_name}_{collection_name}"


def _get_qa_abbreviation(catalog, collection):
    return f"{catalog['abbreviation']}_{collection['abbreviation']}"


def get_quality_assurances(model) -> dict:
    """Used during GOBModel initialisation."""
    quality_assurance = {
        "description": "Kwaliteits gegevens over GOB data.",
        "version": "0.1",
        "abbreviation": "QA",
        "collections": {}
    }
    for catalog_name, catalog in model.items():
        for collection_name, collection in catalog['collections'].items():
            entity = get_entity_name(catalog_name, collection_name)
            qa = _get_qa(catalog, collection)
            quality_assurance['collections'][entity] = qa
    return quality_assurance
