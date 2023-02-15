"""Shorten table names."""


_CONVERSIONS = {
    "is_bron_voor_aantekening_kadastraal_object": "bron_kad_obj",
    "ontstaan_uit_appartementsrechtsplitsing_vve": "ontst_apprechtsplit_vve",
    "betrokken_bij_appartementsrechtsplitsing_vve": "betr_apprechtsplit_vve",
    "heeft_een_relatie_met": "hft_rel_mt",
    "heeft_betrekking_op": "hft_btrk_p",
    "is_onderdeel_van": "ondrdl_vn",
    "aangeduid_door": "angdd_dr",
    "wordt_uitgeoefend_in": "uitgoef_in",
    "commerciele_vestiging": "comm_vstgng",
    "maatschappelijke_activiteit": "maatsch_act",
    "heeft_sbi_activiteiten": "heeft_sbi_act",
    "refereert_aan_meetbouten_referentiepunten": "meetbouten_refpnt_rft_aan",
    "betrokken_bij_brk_zakelijke_rechten": "betrokken_bij_brk_zrt",
    "is_beperkt_tot_brk_tenaamstellingen": "is_beperkt_tot_brk_tng",
    "ontstaan_uit_brk_zakelijke_rechten": "ontstaan_uit_brk_zrt",
    "betrokken_samenwerkingsverband_brk_subject": "betr_samenwerkverband_brk_sjt",
    "betrokken_gorzen_en_aanwassen_brk_subject": "betr_gorzen_aanwassen_brk_sjt",
    "is_bron_voor_brk_aantekening_kadastraal_object": "is_bron_voor_brk_akt",
    "_hft_btrk_p__brk_kadastraal_object": "hft_btrk_op_brk_kot",
    "is_bron_voor_brk_aantekening_recht": "is_bron_voor_brk_art",
    "__ondrdl_vn__brk_kadastrale_gemeente": "onderdeel_van_brk_kge",
}


class NameCompressor:
    """Use the name compressor for all table names that are too long.

    For PostgreSQL the maximimum length = 63 but simply testing for this length is not enough.

    The name is also used with prefixes: 'mv_' for materialized views, 'rel_' for relations.
    And postfixes: '_tmp' for temporary tables.
    """

    # Warn if name exceeds this length
    LONG_NAME_LENGTH = 55

    @classmethod
    def _compressed_value(cls, value):
        return f"_{value}_"

    @classmethod
    def compress_name(cls, name):
        """Compress (shorten) table name."""
        for src, dst in _CONVERSIONS.items():
            name = name.replace(src, cls._compressed_value(dst))

        if len(name) > cls.LONG_NAME_LENGTH:
            print(f"WARNING: LONG NAME: {name} ({len(name)})")

        return name

    @classmethod
    def uncompress_name(cls, name):
        """Uncompress table name."""
        for src, dst in _CONVERSIONS.items():
            name = name.replace(cls._compressed_value(dst), src)
        return name
