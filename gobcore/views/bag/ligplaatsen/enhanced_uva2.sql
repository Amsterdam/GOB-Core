SELECT
    lps_0._gobid,
    lps_0.amsterdamse_sleutel,
    lps_0.begin_geldigheid,
    lps_0.eind_geldigheid,
    begin_geldigheid_object.begin_geldigheid as begin_geldigheid_object,
    eind_geldigheid_object.eind_geldigheid as eind_geldigheid_object,
    lps_0.documentdatum,
    lps_0.documentnummer,
    lps_0.status,
    ST_AsText(lps_0.geometrie) geometrie,
    'bag' AS _catalog,
    'ligplaatsen' AS _collection,
    json_build_object(
        '_gobid', nag_0._gobid,
        'identificatie', nag_0.identificatie,
        'postcode', nag_0.postcode,
        'huisnummer', nag_0.huisnummer,
        'huisletter', nag_0.huisletter,
        'huisnummertoevoeging', nag_0.huisnummertoevoeging,
        '_catalog', 'bag', '_collection', 'nummeraanduidingen') _heeft_hoofdadres,
    json_build_object(
        '_gobid', ore_0._gobid,
        'identificatie', ore_0.identificatie,
        'straatcode', ore_0.straatcode,
        'straatnaam_ptt', ore_0.straatnaam_ptt,
        'naam', ore_0.naam,
        'naam_nen', ore_0.naam_nen,
        '_catalog', 'bag', '_collection', 'openbareruimtes') _ligt_aan_openbareruimte,
    json_build_object(
        '_gobid', wps_0._gobid,
        'identificatie',wps_0.identificatie,
        'naam', wps_0.naam,
        '_catalog', 'bag', '_collection', 'woonplaatsen') _ligt_in_woonplaats,
    json_build_object(
        '_gobid', brt_0._gobid,
        'code', brt_0.code,
        'naam', brt_0.naam,
        '_catalog', 'gebieden', '_collection', 'buurten') _ligt_in_buurt,
    json_build_object(
        '_gobid', wijk_0._gobid,
        '_catalog', 'gebieden', '_collection', 'wijken') _ligt_in_wijk,
    json_build_object(
        '_gobid', sdl_0._gobid,
        'code', sdl_0.code,
        'naam', sdl_0.naam,
        '_catalog', 'gebieden', '_collection', 'stadsdelen') _ligt_in_stadsdeel
FROM (
    SELECT *
    FROM bag_ligplaatsen
    WHERE (_expiration_date IS NULL OR _expiration_date > NOW()) AND _date_deleted IS NULL
    ORDER BY _gobid
    
) lps_0
LEFT JOIN mv_bag_lps_bag_nag_heeft_hoofdadres rel_0
    ON rel_0.src_id = lps_0._id AND rel_0.src_volgnummer = lps_0.volgnummer
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
LEFT JOIN mv_bag_lps_gbd_brt_ligt_in_buurt rel_3
    ON rel_3.src_id = lps_0._id AND rel_3.src_volgnummer = lps_0.volgnummer
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
JOIN LATERAL (
	SELECT DISTINCT ON (amsterdamse_sleutel)
       amsterdamse_sleutel, begin_geldigheid
	FROM bag_ligplaatsen
	ORDER BY amsterdamse_sleutel, volgnummer::INTEGER
) AS begin_geldigheid_object
ON begin_geldigheid_object.amsterdamse_sleutel = lps_0.amsterdamse_sleutel
JOIN LATERAL (
	SELECT DISTINCT ON (amsterdamse_sleutel)
       amsterdamse_sleutel, eind_geldigheid
	FROM bag_ligplaatsen
	ORDER BY amsterdamse_sleutel, volgnummer::INTEGER DESC
) AS eind_geldigheid_object
ON eind_geldigheid_object.amsterdamse_sleutel = lps_0.amsterdamse_sleutel
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
    AND sdl_0._date_deleted IS null
ORDER BY
    lps_0._gobid;
