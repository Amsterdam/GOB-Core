from enum import Enum


class ImportMode(Enum):
    """Import mode definition."""

    FULL = "full"
    RECENT = "recent"
    MUTATIONS = "mutations"
    SINGLE_OBJECT = "single_object"
    DELETE = "delete"
    UPDATE = "update"
