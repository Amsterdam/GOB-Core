SELECT
  pnd.identificatie,
  pnd.volgnummer,
  pnd.registratiedatum,
  CASE WHEN ozk.src_id IS NOT NULL THEN 'J' ELSE 'N' END AS aanduiding_in_onderzoek,
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
    'identificatie', bbk_0.identificatie,
    'volgnummer', bbk_0.volgnummer,
    'code', bbk_0.code
  ) AS _ref_ligt_in_bouwblok_gbd_bbk,
  json_build_object(
    'identificatie', brt_0.identificatie,
    'volgnummer', brt_0.volgnummer,
    'code', brt_0.code,
    'naam', brt_0.naam
  ) AS _ref_ligt_in_buurt_gbd_brt,
  json_build_object(
    'identificatie', wijk_0.identificatie,
    'volgnummer', wijk_0.volgnummer,
    'code', wijk_0.code,
    'naam', wijk_0.naam
  ) AS _ref_ligt_in_wijk_gbd_wijk,
  json_build_object(
    'identificatie', ggw_0.identificatie,
    'volgnummer', ggw_0.volgnummer,
    'code', ggw_0.code,
    'naam', ggw_0.naam
  ) AS _ref_ligt_in_ggwgebied_gbd_ggw,
  json_build_object(
    'identificatie', sdl_0.identificatie,
    'volgnummer', sdl_0.volgnummer,
    'code', sdl_0.code,
    'naam', sdl_0.naam
  ) AS _ref_ligt_in_stadsdeel_gbd_sdl,
  json_build_object(
    'identificatie', ggp_0.identificatie,
    'volgnummer', ggp_0.volgnummer,
    'code', ggp_0.code,
    'naam', ggp_0.naam
  ) AS _ref_ligt_in_ggpgebied_gbd_ggp,
  pnd.geometrie,
  pnd.bagproces
FROM
  legacy.bag_panden AS pnd
-- SELECT ligt_in_bouwblok
LEFT JOIN legacy.mv_bag_pnd_gbd_bbk_ligt_in_bouwblok rel_0
    ON rel_0.src_id = pnd._id AND rel_0.src_volgnummer = pnd.volgnummer
LEFT JOIN legacy.gebieden_bouwblokken bbk_0
    ON rel_0.dst_id = bbk_0._id AND rel_0.dst_volgnummer = bbk_0.volgnummer
-- SELECT ligt_in_buurt
LEFT JOIN legacy.mv_bag_pnd_gbd_brt_ligt_in_buurt rel_1
    ON rel_1.src_id = pnd._id AND rel_1.src_volgnummer = pnd.volgnummer
LEFT JOIN legacy.gebieden_buurten brt_0
    ON rel_1.dst_id = brt_0._id AND rel_1.dst_volgnummer = brt_0.volgnummer
-- SELECT ligt_in_wijk
LEFT JOIN legacy.mv_gbd_brt_gbd_wijk_ligt_in_wijk rel_2
    ON rel_2.src_id = brt_0._id AND rel_2.src_volgnummer = brt_0.volgnummer
LEFT JOIN legacy.gebieden_wijken wijk_0
    ON rel_2.dst_id = wijk_0._id AND rel_2.dst_volgnummer = wijk_0.volgnummer
-- SELECT ligt_in_stadsdeel
LEFT JOIN legacy.mv_gbd_wijk_gbd_sdl_ligt_in_stadsdeel rel_3
    ON rel_3.src_id = wijk_0._id AND rel_3.src_volgnummer = wijk_0.volgnummer
LEFT JOIN legacy.gebieden_stadsdelen sdl_0
    ON rel_3.dst_id = sdl_0._id AND rel_3.dst_volgnummer = sdl_0.volgnummer
-- SELECT _ligt_in_ggwgebied
LEFT JOIN legacy.mv_gbd_brt_gbd_ggw_ligt_in_ggwgebied rel_4
    ON rel_4.src_id = brt_0._id AND rel_4.src_volgnummer = brt_0.volgnummer
LEFT JOIN legacy.gebieden_ggwgebieden ggw_0
    ON rel_4.dst_id = ggw_0._id AND rel_4.dst_volgnummer = ggw_0.volgnummer
-- SELECT _ligt_in_ggpgebied
LEFT JOIN legacy.mv_gbd_brt_gbd_ggp_ligt_in_ggpgebied rel_5
    ON rel_5.src_id = brt_0._id AND rel_5.src_volgnummer = brt_0.volgnummer
LEFT JOIN legacy.gebieden_ggpgebieden ggp_0
    ON rel_5.dst_id = ggp_0._id AND rel_5.dst_volgnummer = ggp_0.volgnummer
LEFT JOIN (
      SELECT
          src_id, src_volgnummer
      FROM legacy.mv_bag_pnd_bag_ozk_heeft_onderzoeken rel
               INNER JOIN legacy.bag_onderzoeken ozk
                          ON rel.dst_id = ozk._id
                              AND rel.dst_volgnummer = ozk.volgnummer
                              AND ozk.in_onderzoek = 'J'
      GROUP BY rel.src_id, rel.src_volgnummer
  ) ozk ON pnd._id = ozk.src_id AND pnd.volgnummer = ozk.src_volgnummer
WHERE
    pnd._date_deleted IS NULL
