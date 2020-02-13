class NameCompressor():
    _CONVERSIONS = {
        "aantekening": "ant",
        "kadastraal": "kad",
        "appartements": "app",
        "splitsing": "spl",
        "ontstaan_uit": "ont",
        "is_bron_voor": "brn",
        "betrokken_bij": "btr",
        "reference": "ref"
    }

    @classmethod
    def _compressed_value(cls, value):
        return f"_{value}_"

    @classmethod
    def compress_name(cls, name):
        for src, dst in cls._CONVERSIONS.items():
            name = name.replace(src, cls._compressed_value(dst))
        return name

    @classmethod
    def uncompress_name(cls, name):
        for src, dst in cls._CONVERSIONS.items():
            name = name.replace(cls._compressed_value(dst), src)
        return name
