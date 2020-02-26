    SELECT
      vot.identificatie,
      vot.volgnummer,
      vot.aanduiding_in_onderzoek,
      vot.registratiedatum,
      vot.geconstateerd,
      json_build_object(
        'identificatie', heeft_hoofdadres.identificatie,
        'volgnummer', heeft_hoofdadres.volgnummer,
        'huisnummer', heeft_hoofdadres.huisnummer,
        'huisletter', heeft_hoofdadres.huisletter,
        'huisnummertoevoeging', heeft_hoofdadres.huisnummertoevoeging,
        'postcode', heeft_hoofdadres.postcode
      ) AS _ref_heeft_hoofdadres_bag_nag,
      json_build_object(
        'identificatie', ligt_aan_openbareruimte.identificatie,
        'volgnummer', ligt_aan_openbareruimte.volgnummer,
        'naam', ligt_aan_openbareruimte.naam
      ) AS _ref_ligt_aan_openbareruimte_bag_ore,
      json_build_object(
        'identificatie', ligt_in_woonplaats.identificatie,
        'volgnummer', ligt_in_woonplaats.volgnummer,
        'naam', ligt_in_woonplaats.naam
      ) AS _ref_ligt_in_woonplaats_bag_wps,
      json_build_object(
        'identificatie', ligt_in_gemeente.identificatie,
        'naam', ligt_in_gemeente.naam
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
      _mref_ligt_in_panden_bag_pnd,
      json_build_object(
        'identificatie', ligt_in_bouwblok.identificatie,
        'volgnummer', ligt_in_bouwblok.volgnummer,
        'code', ligt_in_bouwblok.code
      ) AS _ref_ligt_in_bouwblok_gbd_bbk,
      json_build_object(
        'identificatie', ligt_in_buurt.identificatie,
        'volgnummer', ligt_in_buurt.volgnummer,
        'code', ligt_in_buurt.code,
        'naam', ligt_in_buurt.naam
      ) AS _ref_ligt_in_buurt_gbd_brt,
      json_build_object(
        'identificatie', ligt_in_wijk.identificatie,
        'volgnummer', ligt_in_wijk.volgnummer,
        'code', ligt_in_wijk.code,
        'naam', ligt_in_wijk.naam
      ) AS _ref_ligt_in_wijk_gbd_wijk,
      json_build_object(
        'identificatie', ligt_in_ggwgebied.identificatie,
        'volgnummer', ligt_in_ggwgebied.volgnummer,
        'code', ligt_in_ggwgebied.code,
        'naam', ligt_in_ggwgebied.naam
      ) AS _ref_ligt_in_ggwgebied_gbd_ggw,
      json_build_object(
        'identificatie', ligt_in_stadsdeel.identificatie,
        'volgnummer', ligt_in_stadsdeel.volgnummer,
        'code', ligt_in_stadsdeel.code,
        'naam', ligt_in_stadsdeel.naam
      ) AS _ref_ligt_in_stadsdeel_gbd_sdl,
      json_build_object(
        'identificatie', ligt_in_ggpgebied.identificatie,
        'volgnummer', ligt_in_ggpgebied.volgnummer,
        'code', ligt_in_ggpgebied.code,
        'naam', ligt_in_ggpgebied.naam
      ) AS _ref_ligt_in_ggpgebied_gbd_ggp,
      vot.geometrie
    FROM
      bag_verblijfsobjecten AS vot
    -- SELECT heeft_hoofdadres
    LEFT JOIN
      bag_nummeraanduidingen AS heeft_hoofdadres
    ON
      vot.heeft_hoofdadres->>'id' = heeft_hoofdadres._id AND
      vot.heeft_hoofdadres->>'volgnummer' = heeft_hoofdadres.volgnummer
    -- SELECT ligt_aan_openbareruimte
    LEFT JOIN
      bag_openbareruimtes AS ligt_aan_openbareruimte
    ON
      heeft_hoofdadres.ligt_aan_openbareruimte->>'id' = ligt_aan_openbareruimte._id AND
      heeft_hoofdadres.ligt_aan_openbareruimte->>'volgnummer' = ligt_aan_openbareruimte.volgnummer
    -- SELECT ligt_in_woonplaats
    LEFT JOIN
      bag_woonplaatsen AS ligt_in_woonplaats
    ON
      heeft_hoofdadres.ligt_in_woonplaats->>'id' = ligt_in_woonplaats._id AND
      heeft_hoofdadres.ligt_in_woonplaats->>'volgnummer' = ligt_in_woonplaats.volgnummer
    -- SELECT ligt_in_gemeente
    LEFT JOIN
      brk_gemeentes AS ligt_in_gemeente
    ON
      ligt_in_woonplaats.ligt_in_gemeente->>'id' = ligt_in_gemeente._id
    -- SELECT heeft_nevenadres
    CROSS JOIN LATERAL (
      SELECT
        json_agg(
          json_build_object(
            'identificatie', nevenadres.id,
            'volgnummer', nevenadres.volgnummer
          )
        ) AS _mref_heeft_nevenadres_bag_nag
      FROM
        jsonb_to_recordset(vot.heeft_nevenadres) AS nevenadres(id text, volgnummer text)
    ) AS heeft_nevenadres
    -- SELECT ligt_in_panden
    CROSS JOIN LATERAL (
      SELECT
        json_agg(
          json_build_object(
            'identificatie', pand.id,
            'volgnummer', pand.volgnummer
          )
        ) AS _mref_ligt_in_panden_bag_pnd
      FROM
        jsonb_to_recordset(vot.ligt_in_panden) AS pand(id text, volgnummer text)
    ) AS ligt_in_panden
    -- SELECT ligt_in_bouwblok
    LEFT JOIN
      bag_panden AS pand_bouwblok
    ON
      vot.ligt_in_panden#>>'{0,id}' = pand_bouwblok.identificatie AND
      vot.ligt_in_panden#>>'{0,volgnummer}' = pand_bouwblok.volgnummer
    LEFT JOIN
      gebieden_bouwblokken AS ligt_in_bouwblok
    ON
      pand_bouwblok.ligt_in_bouwblok->>'id' = ligt_in_bouwblok.identificatie AND
      pand_bouwblok.ligt_in_bouwblok->>'volgnummer' = ligt_in_bouwblok.volgnummer
    -- SELECT ligt_in_buurt
    LEFT JOIN
      gebieden_buurten AS ligt_in_buurt
    ON
      vot.ligt_in_buurt->>'id' = ligt_in_buurt.identificatie AND
      vot.ligt_in_buurt->>'volgnummer' = ligt_in_buurt.volgnummer
    -- SELECT ligt_in_wijk
    LEFT JOIN
      gebieden_wijken AS ligt_in_wijk
    ON
      ligt_in_buurt.ligt_in_wijk->>'id' = ligt_in_wijk.identificatie AND
      ligt_in_buurt.ligt_in_wijk->>'volgnummer' = ligt_in_wijk.volgnummer
    -- SELECT ligt_in_stadsdeel
    LEFT JOIN
      gebieden_stadsdelen AS ligt_in_stadsdeel
    ON
      ligt_in_wijk.ligt_in_stadsdeel->>'id' = ligt_in_stadsdeel.identificatie AND
      ligt_in_wijk.ligt_in_stadsdeel->>'volgnummer' = ligt_in_stadsdeel.volgnummer
    -- SELECT _ligt_in_ggwgebied
    LEFT JOIN
      gebieden_ggwgebieden AS ligt_in_ggwgebied
    ON
      ligt_in_buurt._ligt_in_ggwgebied->>'id' = ligt_in_ggwgebied.identificatie AND
      ligt_in_buurt._ligt_in_ggwgebied->>'volgnummer' = ligt_in_ggwgebied.volgnummer
    -- SELECT _ligt_in_ggpgebied
    LEFT JOIN
      gebieden_ggpgebieden AS ligt_in_ggpgebied
    ON
      ligt_in_buurt._ligt_in_ggpgebied->>'id' = ligt_in_ggpgebied.identificatie AND
      ligt_in_buurt._ligt_in_ggpgebied->>'volgnummer' = ligt_in_ggpgebied.volgnummer
    WHERE vot._date_deleted IS NULL
