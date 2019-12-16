SELECT
    vot_0.amsterdamse_sleutel,
    vot_0.documentdatum,
    vot_0.documentnummer,
    vot_0.begin_geldigheid,
    vot_0.eind_geldigheid,
    begin_geldigheid_object.begin_geldigheid as begin_geldigheid_object,
    eind_geldigheid_object.eind_geldigheid as eind_geldigheid_object,
    vot_0.status,
    ST_AsText(vot_0.geometrie) geometrie,
    vot_0.gebruiksdoel,
    vot_0.verdieping_toegang,
    vot_0.aantal_eenheden_complex,
    vot_0.aantal_bouwlagen,
    vot_0.cbs_nummer,
    vot_0.indicatie_woningvoorraad,
    vot_0.feitelijk_gebruik,
    vot_0.financieringscode,
    vot_0.eigendomsverhouding,
    vot_0.toegang,
    json_build_object(
        'amsterdamse_sleutel', nag_0.amsterdamse_sleutel,
        'postcode', nag_0.postcode,
        'huisnummer', nag_0.huisnummer,
        'huisletter', nag_0.huisletter,
        'huisnummertoevoeging', nag_0.huisnummertoevoeging) heeft_hoofdadres,
    json_build_object(
        'straatcode', ore_0.straatcode,
        'straatnaam_ptt', ore_0.straatnaam_ptt,
        'naam', ore_0.naam,
        'naam_nen', ore_0.naam_nen) ligt_aan_openbareruimte,
    json_build_object(
        'identificatie', wps_0.identificatie,
        'naam', wps_0.naam) ligt_in_woonplaats,
    json_build_object(
        'code', brt_0.code,
        'naam', brt_0.naam) ligt_in_buurt,
    json_build_object(
        'code', sdl_0.code,
        'naam', sdl_0.naam) ligt_in_stadsdeel,
    json_build_object(
        'type_woonobject', pnd_0.type_woonobject,
        'ligging', pnd_0.ligging,
        'oorspronkelijk_bouwjaar', pnd_0.oorspronkelijk_bouwjaar) ligt_in_panden,
    json_build_object(
        'code', bbk_0.code) ligt_in_bouwblok
FROM (
    SELECT *
    FROM bag_verblijfsobjecten
    WHERE (_expiration_date IS NULL OR _expiration_date > NOW()) AND _date_deleted IS NULL
    ORDER BY _gobid
) vot_0
LEFT JOIN mv_bag_vot_bag_nag_heeft_hoofdadres rel_0
    ON rel_0.src_id = vot_0._id AND rel_0.src_volgnummer = vot_0.volgnummer
LEFT JOIN bag_nummeraanduidingen nag_0
    ON rel_0.dst_id = nag_0._id AND rel_0.dst_volgnummer = nag_0.volgnummer
LEFT JOIN mv_bag_nag_bag_ore_ligt_aan_openbareruimte rel_1
    ON rel_1.src_id = nag_0._id AND rel_1.src_volgnummer = nag_0.volgnummer
LEFT JOIN bag_openbareruimtes ore_0
    ON rel_1.dst_id = ore_0._id AND rel_1.dst_volgnummer = ore_0.volgnummer
LEFT JOIN mv_bag_ore_bag_wps_ligt_in_woonplaats rel_2
    ON rel_2.src_id = ore_0._id AND rel_2.src_volgnummer = ore_0.volgnummer
LEFT JOIN bag_woonplaatsen wps_0
    ON rel_2.dst_id = wps_0._id AND rel_2.dst_volgnummer = wps_0.volgnummer
LEFT JOIN mv_bag_vot_gbd_brt_ligt_in_buurt rel_3
    ON rel_3.src_id = vot_0._id AND rel_3.src_volgnummer = vot_0.volgnummer
LEFT JOIN gebieden_buurten brt_0
    ON rel_3.dst_id = brt_0._id AND rel_3.dst_volgnummer = brt_0.volgnummer
LEFT JOIN mv_gbd_brt_gbd_wijk_ligt_in_wijk rel_4
    ON rel_4.src_id = brt_0._id AND rel_4.src_volgnummer = brt_0.volgnummer
LEFT JOIN gebieden_wijken wijk_0
    ON rel_4.dst_id = wijk_0._id AND rel_4.dst_volgnummer = wijk_0.volgnummer
LEFT JOIN mv_gbd_wijk_gbd_sdl_ligt_in_stadsdeel rel_5
    ON rel_5.src_id = wijk_0._id AND rel_5.src_volgnummer = wijk_0.volgnummer
LEFT JOIN gebieden_stadsdelen sdl_0
    ON rel_5.dst_id = sdl_0._id AND rel_5.dst_volgnummer = sdl_0.volgnummer
LEFT JOIN LATERAL (
    SELECT DISTINCT ON (src_id)
       src_id, dst_id, src_volgnummer, dst_volgnummer
    FROM mv_bag_vot_bag_pnd_ligt_in_panden
    ORDER BY src_id, src_volgnummer, dst_id
) AS rel_6
    ON rel_6.src_id = vot_0._id AND rel_6.src_volgnummer = vot_0.volgnummer
LEFT JOIN bag_panden pnd_0
    ON rel_6.dst_id = pnd_0._id AND rel_6.dst_volgnummer = pnd_0.volgnummer
LEFT JOIN mv_bag_pnd_gbd_bbk_ligt_in_bouwblok rel_7
    ON rel_7.src_id = pnd_0._id AND rel_7.src_volgnummer = pnd_0.volgnummer
LEFT JOIN gebieden_bouwblokken bbk_0
    ON rel_7.dst_id = bbk_0._id AND rel_7.dst_volgnummer = bbk_0.volgnummer
JOIN LATERAL (
    SELECT DISTINCT ON (amsterdamse_sleutel)
       amsterdamse_sleutel, begin_geldigheid
    FROM bag_verblijfsobjecten
    ORDER BY amsterdamse_sleutel, volgnummer::INTEGER
) AS begin_geldigheid_object
ON begin_geldigheid_object.amsterdamse_sleutel = vot_0.amsterdamse_sleutel
JOIN LATERAL (
    SELECT DISTINCT ON (amsterdamse_sleutel)
       amsterdamse_sleutel, eind_geldigheid
    FROM bag_verblijfsobjecten
    ORDER BY amsterdamse_sleutel, volgnummer::INTEGER DESC
) AS eind_geldigheid_object
ON eind_geldigheid_object.amsterdamse_sleutel = vot_0.amsterdamse_sleutel
WHERE (nag_0._expiration_date IS NULL OR nag_0._expiration_date > NOW())
    AND nag_0._date_deleted IS NULL
    AND (ore_0._expiration_date IS NULL OR ore_0._expiration_date > NOW())
    AND ore_0._date_deleted IS NULL
    AND (wps_0._expiration_date IS NULL OR wps_0._expiration_date > NOW())
    AND wps_0._date_deleted IS NULL
    AND (brt_0._expiration_date IS NULL OR brt_0._expiration_date > NOW())
    AND brt_0._date_deleted IS NULL
    AND (wijk_0._expiration_date IS NULL OR wijk_0._expiration_date > NOW())
    AND wijk_0._date_deleted IS NULL
    AND (sdl_0._expiration_date IS NULL OR sdl_0._expiration_date > NOW())
    AND sdl_0._date_deleted IS NULL
    AND (pnd_0._expiration_date IS NULL OR pnd_0._expiration_date > NOW())
    AND pnd_0._date_deleted IS NULL
    AND (bbk_0._expiration_date IS NULL OR bbk_0._expiration_date > NOW())
    AND bbk_0._date_deleted IS NULL
ORDER BY
    vot_0._gobid
