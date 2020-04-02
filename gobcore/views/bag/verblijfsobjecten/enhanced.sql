    SELECT
      vot.identificatie,
      vot.aanduiding_in_onderzoek,
      vot.geconstateerd,
      json_build_object(
        'identificatie', nag_0.identificatie,
        'huisnummer', nag_0.huisnummer,
        'huisletter', nag_0.huisletter,
        'huisnummertoevoeging', nag_0.huisnummertoevoeging,
        'postcode', nag_0.postcode
      ) AS _ref_heeft_hoofdadres_bag_nag,
      json_build_object(
        'identificatie', ore_0.identificatie,
        'naam', ore_0.naam
      ) AS _ref_ligt_aan_openbareruimte_bag_ore,
      json_build_object(
        'identificatie', wps_0.identificatie,
        'naam', wps_0.naam
      ) AS _ref_ligt_in_woonplaats_bag_wps,
      json_build_object(
        'identificatie', gme_0.identificatie,
        'naam', gme_0.naam
      ) AS _ref_ligt_in_gemeente_brk_gme,
      _mref_heeft_nevenadres_bag_nag,
      vot.gebruiksdoel,
      vot.gebruiksdoel_woonfunctie,
      vot.gebruiksdoel_gezondheidszorgfunctie,
      vot.aantal_eenheden_complex,
      vot.feitelijk_gebruik,
      vot.oppervlakte,
      vot.status,
      vot.begin_geldigheid,
      vot.eind_geldigheid,
      vot.documentdatum,
      vot.documentnummer,
      vot.verdieping_toegang,
      vot.toegang,
      vot.aantal_bouwlagen,
      vot.hoogste_bouwlaag,
      vot.laagste_bouwlaag,
      vot.aantal_kamers,
      vot.eigendomsverhouding,
      vot.redenopvoer,
      vot.redenafvoer,
      pnd_0._mref_ligt_in_panden_bag_pnd,
      json_build_object(
        'identificatie', bbk_0.identificatie,
        'code', bbk_0.code
      ) AS _ref_ligt_in_bouwblok_gbd_bbk,
      json_build_object(
        'identificatie', brt_0.identificatie,
        'code', brt_0.code,
        'naam', brt_0.naam
      ) AS _ref_ligt_in_buurt_gbd_brt,
      json_build_object(
        'identificatie', wijk_0.identificatie,
        'code', wijk_0.code,
        'naam', wijk_0.naam
      ) AS _ref_ligt_in_wijk_gbd_wijk,
      json_build_object(
        'identificatie', ggw_0.identificatie,
        'code', ggw_0.code,
        'naam', ggw_0.naam
      ) AS _ref_ligt_in_ggwgebied_gbd_ggw,
      json_build_object(
        'identificatie', sdl_0.identificatie,
        'code', sdl_0.code,
        'naam', sdl_0.naam
      ) AS _ref_ligt_in_stadsdeel_gbd_sdl,
      json_build_object(
        'identificatie', ggp_0.identificatie,
        'code', ggp_0.code,
        'naam', ggp_0.naam
      ) AS _ref_ligt_in_ggpgebied_gbd_ggp,
      vot.geometrie
    FROM
      bag_verblijfsobjecten AS vot
    -- SELECT heeft_hoofdadres
    LEFT JOIN mv_bag_vot_bag_nag_heeft_hoofdadres rel_0
        ON rel_0.src_id = vot._id AND rel_0.src_volgnummer = vot.volgnummer
    LEFT JOIN bag_nummeraanduidingen nag_0
        ON rel_0.dst_id = nag_0._id AND rel_0.dst_volgnummer = nag_0.volgnummer AND COALESCE(nag_0._expiration_date, '9999-12-31'::timestamp without time zone) > NOW()
    -- SELECT ligt_aan_openbareruimte
    LEFT JOIN mv_bag_nag_bag_ore_ligt_aan_openbareruimte rel_1
        ON rel_1.src_id = nag_0._id AND rel_1.src_volgnummer = nag_0.volgnummer
    LEFT JOIN bag_openbareruimtes ore_0
        ON rel_1.dst_id = ore_0._id AND rel_1.dst_volgnummer = ore_0.volgnummer AND COALESCE(ore_0._expiration_date, '9999-12-31'::timestamp without time zone) > NOW()
    -- SELECT ligt_in_woonplaats
    LEFT JOIN mv_bag_ore_bag_wps_ligt_in_woonplaats rel_2
        ON rel_2.src_id = ore_0._id AND rel_2.src_volgnummer = ore_0.volgnummer
    LEFT JOIN bag_woonplaatsen wps_0
        ON rel_2.dst_id = wps_0._id AND rel_2.dst_volgnummer = wps_0.volgnummer AND COALESCE(wps_0._expiration_date, '9999-12-31'::timestamp without time zone) > NOW()
    -- SELECT ligt_in_gemeente
    LEFT JOIN mv_bag_wps_brk_gme_ligt_in_gemeente rel_3
        ON rel_3.src_id = wps_0._id AND rel_3.src_volgnummer = wps_0.volgnummer
    LEFT JOIN brk_gemeentes gme_0
        ON rel_3.dst_id = gme_0._id AND COALESCE(gme_0._expiration_date, '9999-12-31'::timestamp without time zone) > NOW()
    -- SELECT heeft_nevenadres
    LEFT JOIN (
      SELECT
        src_id,
        src_volgnummer,
        json_agg(
          json_build_object(
            'identificatie', dst_id
          )
        ) AS _mref_heeft_nevenadres_bag_nag
      FROM
        mv_bag_vot_bag_nag_heeft_nevenadres mrel_0
      WHERE
          COALESCE(mrel_0.eind_geldigheid, '9999-12-31'::timestamp without time zone) > NOW()
          AND dst_id IS NOT NULL
      GROUP BY src_id, src_volgnummer
    ) AS nvn_0
    ON nvn_0.src_id = vot._id AND nvn_0.src_volgnummer = vot.volgnummer
    -- SELECT ligt_in_panden
    LEFT JOIN (
      SELECT
        src_id,
        src_volgnummer,
        json_agg(
          json_build_object(
            'identificatie', dst_id
          )
        ) AS _mref_ligt_in_panden_bag_pnd
      FROM
        mv_bag_vot_bag_pnd_ligt_in_panden mrel_1
      WHERE
        COALESCE(mrel_1.eind_geldigheid, '9999-12-31'::timestamp without time zone) > NOW()
        AND dst_id IS NOT NULL
      GROUP BY src_id, src_volgnummer
    ) AS pnd_0
    ON pnd_0.src_id = vot._id AND pnd_0.src_volgnummer = vot.volgnummer
    -- SELECT first ligt_in_bouwblok
    LEFT JOIN LATERAL (
        SELECT DISTINCT ON (pnd_1.src_id)
           pnd_1.src_id, pnd_1.dst_id, pnd_1.src_volgnummer, pnd_1.dst_volgnummer
        FROM mv_bag_vot_bag_pnd_ligt_in_panden pnd_1
        INNER JOIN (SELECT src_id, MAX(src_volgnummer::INTEGER) src_volgnummer
          FROM mv_bag_vot_bag_pnd_ligt_in_panden
          GROUP BY src_id) max_pnd ON pnd_1.src_id = max_pnd.src_id AND pnd_1.src_volgnummer::INTEGER = max_pnd.src_volgnummer
          ORDER BY pnd_1.src_id, pnd_1.src_volgnummer, pnd_1.dst_id
    ) AS rel_4
        ON rel_4.src_id = vot._id AND rel_4.src_volgnummer = vot.volgnummer
    LEFT JOIN bag_panden pnd_2
        ON rel_4.dst_id = pnd_2._id AND rel_4.dst_volgnummer = pnd_2.volgnummer AND COALESCE(pnd_2._expiration_date, '9999-12-31'::timestamp without time zone) > NOW()
    LEFT JOIN mv_bag_pnd_gbd_bbk_ligt_in_bouwblok rel_5
        ON rel_5.src_id = pnd_2._id AND rel_5.src_volgnummer = pnd_2.volgnummer
    LEFT JOIN gebieden_bouwblokken bbk_0
        ON rel_5.dst_id = bbk_0._id AND rel_5.dst_volgnummer = bbk_0.volgnummer AND COALESCE(bbk_0._expiration_date, '9999-12-31'::timestamp without time zone) > NOW()
    -- SELECT ligt_in_buurt
    LEFT JOIN mv_bag_vot_gbd_brt_ligt_in_buurt rel_6
        ON rel_6.src_id = vot._id AND rel_6.src_volgnummer = vot.volgnummer
    LEFT JOIN gebieden_buurten brt_0
        ON rel_6.dst_id = brt_0._id AND rel_6.dst_volgnummer = brt_0.volgnummer AND COALESCE(brt_0._expiration_date, '9999-12-31'::timestamp without time zone) > NOW()
    -- SELECT ligt_in_wijk
    LEFT JOIN mv_gbd_brt_gbd_wijk_ligt_in_wijk rel_7
        ON rel_7.src_id = brt_0._id AND rel_7.src_volgnummer = brt_0.volgnummer
    LEFT JOIN gebieden_wijken wijk_0
        ON rel_7.dst_id = wijk_0._id AND rel_7.dst_volgnummer = wijk_0.volgnummer AND COALESCE(brt_0._expiration_date, '9999-12-31'::timestamp without time zone) > NOW()
    -- SELECT ligt_in_stadsdeel
    LEFT JOIN mv_gbd_wijk_gbd_sdl_ligt_in_stadsdeel rel_8
        ON rel_8.src_id = wijk_0._id AND rel_8.src_volgnummer = wijk_0.volgnummer
    LEFT JOIN gebieden_stadsdelen sdl_0
        ON rel_8.dst_id = sdl_0._id AND rel_8.dst_volgnummer = sdl_0.volgnummer AND COALESCE(sdl_0._expiration_date, '9999-12-31'::timestamp without time zone) > NOW()
    -- SELECT _ligt_in_ggwgebied
    LEFT JOIN mv_gbd_brt_gbd_ggw__ligt_in_ggwgebied rel_9
        ON rel_9.src_id = brt_0._id AND rel_9.src_volgnummer = brt_0.volgnummer
    LEFT JOIN gebieden_ggwgebieden ggw_0
        ON rel_9.dst_id = ggw_0._id AND rel_9.dst_volgnummer = ggw_0.volgnummer AND COALESCE(ggw_0._expiration_date, '9999-12-31'::timestamp without time zone) > NOW()
    -- SELECT _ligt_in_ggpgebied
    LEFT JOIN mv_gbd_brt_gbd_ggp__ligt_in_ggpgebied rel_10
        ON rel_10.src_id = brt_0._id AND rel_10.src_volgnummer = brt_0.volgnummer
    LEFT JOIN gebieden_ggpgebieden ggp_0
        ON rel_10.dst_id = ggp_0._id AND rel_10.dst_volgnummer = ggp_0.volgnummer AND COALESCE(ggp_0._expiration_date, '9999-12-31'::timestamp without time zone) > NOW()
    WHERE
      COALESCE(vot._expiration_date, '9999-12-31'::timestamp without time zone) > NOW()
      AND vot._date_deleted IS NULL
