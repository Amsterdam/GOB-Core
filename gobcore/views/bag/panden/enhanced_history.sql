    SELECT
      pnd.identificatie,
      pnd.volgnummer,
      pnd.registratiedatum,
      pnd.aanduiding_in_onderzoek,
      pnd.geconstateerd,
      pnd.oorspronkelijk_bouwjaar,
      pnd.status,
      pnd.begin_geldigheid,
      pnd.eind_geldigheid,
      pnd.documentnummer,
      pnd.documentdatum,
      pnd.naam,
      pnd.ligging,
      pnd.aantal_bouwlagen,
      pnd.hoogste_bouwlaag,
      pnd.laagste_bouwlaag,
      pnd.type_woonobject,
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
      pnd.geometrie
    FROM
      bag_panden AS pnd
    -- SELECT ligt_in_bouwblok
    LEFT JOIN
      gebieden_bouwblokken AS ligt_in_bouwblok
    ON
      pnd.ligt_in_bouwblok->>'id' = ligt_in_bouwblok.identificatie AND
      pnd.ligt_in_bouwblok->>'volgnummer' = ligt_in_bouwblok.volgnummer
    -- SELECT ligt_in_buurt
    LEFT JOIN
      gebieden_buurten AS ligt_in_buurt
    ON
      ligt_in_bouwblok.ligt_in_buurt->>'id' = ligt_in_buurt.identificatie AND
      ligt_in_bouwblok.ligt_in_buurt->>'volgnummer' = ligt_in_buurt.volgnummer
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
    WHERE pnd._date_deleted IS NULL
