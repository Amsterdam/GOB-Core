SELECT sjt._gobid,
       sjt.identificatie,
       sjt.voornamen                AS "brk:sjt:voornamen",
       sjt.voorvoegsels             AS "brk:sjt:voorvoegsels",
       sjt.geslacht,
       sjt.geslachtsnaam            AS "brk:sjt:geslachtsnaam",
       sjt.geboortedatum            AS "brk:sjt:geboortedatum",
       sjt.geboorteplaats,
       sjt.geboorteland,
       sjt.datum_overlijden,
       sjt.geslachtsnaam_partner    AS "brk:sjt:geslachtsnaam_partner",
       sjt.voornamen_partner        AS "brk:sjt:voornamen_partner",
       sjt.voorvoegsels_partner     AS "brk:sjt:voorvoegsels_partner",
       sjt.naam_gebruik,
       sjt.land_waarnaar_vertrokken,
       sjt.indicatie_overleden,
       sjt.rechtsvorm,
       sjt.statutaire_naam,
       sjt.statutaire_zetel,
       sjt.heeft_rsin_voor,
       sjt.heeft_kvknummer_voor,
       sjt.heeft_bsn_voor           AS "brk:sjt:heeft_bsn_voor",
       sjt.woonadres,
       sjt.postadres,
       sjt.woonadres_buitenland,
       sjt.postadres_postbus,
       sjt.postadres_buitenland,
       sjt.beschikkingsbevoegdheid,
       sjt.type_subject
FROM brk_kadastralesubjecten sjt
         JOIN (
    SELECT sjt.identificatie
    FROM brk_kadastralesubjecten sjt
             JOIN mv_brk_tng_brk_sjt_van_kadastraalsubject rel ON rel.dst_id = sjt._id
             JOIN brk_tenaamstellingen tng ON tng._id = rel.src_id
    WHERE (tng._expiration_date IS NULL OR tng._expiration_date > NOW())
    GROUP BY sjt.identificatie
    UNION
    SELECT sjt.identificatie
    FROM brk_kadastralesubjecten sjt
             JOIN mv_brk_zrt_brk_sjt_betrokken_bij_appartementsrechtsplitsing_vve rel ON rel.dst_id = sjt._id
             JOIN brk_zakelijkerechten zrt ON zrt._id = rel.src_id AND zrt.volgnummer = rel.src_volgnummer
    GROUP BY sjt.identificatie
    UNION
    SELECT sjt.identificatie
    FROM brk_kadastralesubjecten sjt
             JOIN mv_brk_zrt_brk_sjt_ontstaan_uit_appartementsrechtsplitsing_vve rel ON rel.dst_id = sjt._id
             JOIN brk_zakelijkerechten zrt ON zrt._id = rel.src_id AND zrt.volgnummer = rel.src_volgnummer
    GROUP BY sjt.identificatie
    UNION
    SELECT sjt.identificatie
    FROM brk_kadastralesubjecten sjt
             JOIN mv_brk_akt_brk_sjt_heeft_betrokken_persoon rel ON rel.dst_id = sjt._id
             JOIN brk_aantekeningenkadastraleobjecten akt
                  ON akt._id = rel.src_id and akt.volgnummer = rel.src_volgnummer
    WHERE (akt._expiration_date IS NULL OR akt._expiration_date > NOW())
    GROUP BY sjt.identificatie
    UNION
    SELECT sjt.identificatie
    FROM brk_kadastralesubjecten sjt
             JOIN mv_brk_art_brk_sjt_heeft_betrokken_persoon rel ON rel.dst_id = sjt._id
             JOIN brk_aantekeningenrechten art ON art._id = rel.src_id
    WHERE (art._expiration_date IS NULL OR art._expiration_date > NOW())
    GROUP BY sjt.identificatie
) sjt_ids ON sjt_ids.identificatie = sjt.identificatie
WHERE (sjt._expiration_date IS NULL OR sjt._expiration_date > NOW())