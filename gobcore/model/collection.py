"""GOB collection module."""


from collections import UserDict
from typing import Any

class GOBCollection(UserDict[str, Any]):
    """GOB Collection."""

    def __init__(self, collection_name, collection, catalog_name) -> None:
        """Initialise GOBCollection."""
        self.name = collection_name
        self.catalog_name = catalog_name
        self.abbreviation = collection["abbreviation"].upper()
        self.data = collection

    @property
    def has_states(self) -> bool:
        """Tell if a collection has states."""
        return self.get("has_states") is True

    @property
    def reference(self) -> str:
        """Return catalog_name:collection_name reference."""
        return f"{self.catalog_name}:{self.name}"

    @property
    def table_name(self) -> str:
        """Return collection table name."""
        return f"{self.catalog_name}_{self.name}".lower()

    @property
    def is_relation(self) -> bool:
        """Tell if I'm the relation collection."""
        return self.catalog_name == "rel"

    def matches_abbreviation(self, abbr: str) -> bool:
        """Return True if uppercased `abbr' matches collection abbreviation."""
        return abbr.upper() == self.abbreviation
