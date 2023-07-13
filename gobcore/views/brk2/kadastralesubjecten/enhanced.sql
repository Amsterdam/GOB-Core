SELECT sjt._gobid,
       sjt.identificatie,
       sjt.voornamen,
       sjt.voorvoegsels,
       sjt.geslacht,
       sjt.geslachtsnaam,
       sjt.geboortedatum,
       sjt.geboorteplaats,
       sjt.geboorteland,
       sjt.datum_overlijden,
       sjt.geslachtsnaam_partner,
       sjt.voornamen_partner,
       sjt.voorvoegsels_partner,
       sjt.naam_gebruik,
       sjt.land_waarnaar_vertrokken,
       sjt.indicatie_overleden,
       sjt.rechtsvorm,
       sjt.statutaire_naam,
       sjt.statutaire_zetel,
       JSONB_BUILD_OBJECT(
         'bronwaarde', sjt.heeft_rsin_voor_hr_niet_natuurlijkepersoon
       )                                                        AS heeft_rsin_voor,
       JSONB_BUILD_OBJECT(
         'bronwaarde', sjt.heeft_kvknummer_voor_hr_maatschappelijkeactiviteit
       )                                                        AS heeft_kvknummer_voor,
       JSONB_BUILD_OBJECT(
         'bronwaarde', sjt.heeft_bsn_voor_brp_persoon
       )                                                        AS heeft_bsn_voor,
       sjt.woonadres,
       sjt.postadres,
       JSONB_BUILD_OBJECT(
         'code', woonadres->'buitenland_land_code',
         'naam', woonadres->'buitenland_land_omschrijving',
         'adres', woonadres->'buitenland_adres',
         'regio', woonadres->'buitenland_regio',
         'woonplaats', woonadres->'buitenland_woonplaats',
         'omschrijving', NULL
       )                                                        AS woonadres_buitenland,
       JSONB_BUILD_OBJECT(
         'nummer', postadres->'postbus_nummer',
         'postcode', postadres->'postbus_postcode',
         'woonplaatsnaam', postadres->'postbus_woonplaatsnaam'
       )                                                        AS postadres_postbus,
       JSONB_BUILD_OBJECT(
         'code', postadres->'buitenland_land_code',
         'naam', postadres->'buitenland_land_omschrijving',
         'adres', postadres->'buitenland_adres',
         'regio', postadres->'buitenland_regio',
         'woonplaats', postadres->'buitenland_woonplaats',
         'omschrijving', NULL
       )                                                        AS postadres_buitenland,
       sjt.beschikkingsbevoegdheid,
       sjt.type_subject
FROM legacy.brk2_kadastralesubjecten sjt
         JOIN (
    SELECT sjt.identificatie
    FROM legacy.brk2_kadastralesubjecten sjt
             JOIN legacy.mv_brk2_tng_brk2_sjt_van_brk_kadastraalsubject rel ON rel.dst_id = sjt._id
             JOIN legacy.brk2_tenaamstellingen tng ON tng._id = rel.src_id
    WHERE (tng._expiration_date IS NULL OR tng._expiration_date > NOW())
    GROUP BY sjt.identificatie
    UNION
    SELECT sjt.identificatie
    FROM legacy.brk2_kadastralesubjecten sjt
             JOIN legacy.mv_brk2_zrt_brk2_sjt_vve_identificatie_betrokken_bij rel ON rel.dst_id = sjt._id
             JOIN legacy.brk2_zakelijkerechten zrt ON zrt._id = rel.src_id AND zrt.volgnummer = rel.src_volgnummer
    GROUP BY sjt.identificatie
    UNION
    SELECT sjt.identificatie
    FROM legacy.brk2_kadastralesubjecten sjt
             JOIN legacy.mv_brk2_zrt_brk2_sjt_vve_identificatie_ontstaan_uit rel ON rel.dst_id = sjt._id
             JOIN legacy.brk2_zakelijkerechten zrt ON zrt._id = rel.src_id AND zrt.volgnummer = rel.src_volgnummer
    GROUP BY sjt.identificatie
    UNION
    SELECT sjt.identificatie
    FROM legacy.brk2_kadastralesubjecten sjt
             JOIN legacy.mv_brk2_akt_brk2_sjt_heeft_brk_betrokken_persoon rel ON rel.dst_id = sjt._id
             JOIN legacy.brk2_aantekeningenkadastraleobjecten akt
                  ON akt._id = rel.src_id and akt.volgnummer = rel.src_volgnummer
    WHERE (akt._expiration_date IS NULL OR akt._expiration_date > NOW())
    GROUP BY sjt.identificatie
    UNION
    SELECT sjt.identificatie
    FROM legacy.brk2_kadastralesubjecten sjt
             JOIN legacy.mv_brk2_art_brk2_sjt_heeft_brk_betrokken_persoon rel ON rel.dst_id = sjt._id
             JOIN legacy.brk2_aantekeningenrechten art ON art._id = rel.src_id
    WHERE (art._expiration_date IS NULL OR art._expiration_date > NOW())
    GROUP BY sjt.identificatie
) sjt_ids ON sjt_ids.identificatie = sjt.identificatie
WHERE (sjt._expiration_date IS NULL OR sjt._expiration_date > NOW())
