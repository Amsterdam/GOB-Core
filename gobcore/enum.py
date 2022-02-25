from enum import Enum


class ImportMode(Enum):
    FULL = "full"
    RECENT = "recent"
    MUTATIONS = "mutations"
    SINGLE_OBJECT = "single_object"
    DELETE = "delete"
