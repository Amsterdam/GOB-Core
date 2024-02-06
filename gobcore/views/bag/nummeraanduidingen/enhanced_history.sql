    SELECT
      nag.identificatie,
      nag.volgnummer,
      nag.registratiedatum,
      CASE WHEN ozk.src_id IS NOT NULL THEN 'J' ELSE 'N' END AS aanduiding_in_onderzoek,
      nag.geconstateerd,
      nag.huisnummer,
      nag.huisletter,
      nag.huisnummertoevoeging,
      nag.postcode,
      json_build_object(
        'identificatie', ore_0.identificatie,
        'volgnummer', ore_0.volgnummer,
        'naam', ore_0.naam
      ) AS _ref_ligt_aan_openbareruimte_bag_ore,
      json_build_object(
        'identificatie', wps_0.identificatie,
        'volgnummer', wps_0.volgnummer,
        'naam', wps_0.naam
      ) AS _ref_ligt_in_woonplaats_bag_wps,
      nag.begin_geldigheid,
      nag.eind_geldigheid,
      nag.type_adresseerbaar_object,
      nag.type_adres,
      nag.documentdatum,
      nag.documentnummer,
      nag.status,
      json_build_object(
        'identificatie', adresseert_verblijfsobject.identificatie,
        'volgnummer', adresseert_verblijfsobject.volgnummer
      ) AS _ref_adresseert_verblijfsobject_bag_vot,
      json_build_object(
        'identificatie', adresseert_ligplaats.identificatie,
        'volgnummer', adresseert_ligplaats.volgnummer
      ) AS _ref_adresseert_ligplaats_bag_lps,
      json_build_object(
        'identificatie', adresseert_standplaats.identificatie,
        'volgnummer', adresseert_standplaats.volgnummer
      ) AS _ref_adresseert_standplaats_bag_sps,
      nag.bagproces
    FROM
      bag_nummeraanduidingen AS nag
    -- SELECT ligt_aan_openbareruimte
    LEFT JOIN legacy.mv_bag_nag_bag_ore_ligt_aan_openbareruimte rel_0
        ON rel_0.src_id = nag._id AND rel_0.src_volgnummer = nag.volgnummer
    LEFT JOIN legacy.bag_openbareruimtes ore_0
        ON rel_0.dst_id = ore_0._id AND rel_0.dst_volgnummer = ore_0.volgnummer
    -- SELECT ligt_in_woonplaats
    LEFT JOIN legacy.mv_bag_ore_bag_wps_ligt_in_woonplaats rel_1
        ON rel_1.src_id = ore_0._id AND rel_1.src_volgnummer = ore_0.volgnummer
    LEFT JOIN legacy.bag_woonplaatsen wps_0
        ON rel_1.dst_id = wps_0._id AND rel_1.dst_volgnummer = wps_0.volgnummer
    -- SELECT adresseert_verblijfsobject
    LEFT JOIN (
        SELECT
            dst_id,
            dst_volgnummer,
            src_id,
            max(src_volgnummer) as src_volgnummer
        FROM (
                 SELECT *
                 FROM legacy.mv_bag_vot_bag_nag_heeft_hoofdadres
                 UNION
                 SELECT *
                 FROM legacy.mv_bag_vot_bag_nag_heeft_nevenadres
             ) q GROUP BY dst_id, dst_volgnummer, src_id
    ) vot_adressen ON vot_adressen.dst_id = nag._id AND vot_adressen.dst_volgnummer = nag.volgnummer
    LEFT JOIN legacy.bag_verblijfsobjecten adresseert_verblijfsobject ON vot_adressen.src_id = adresseert_verblijfsobject._id and vot_adressen.src_volgnummer = adresseert_verblijfsobject.volgnummer
    -- SELECT adresseert_ligplaats
    LEFT JOIN (
          SELECT
              dst_id,
              dst_volgnummer,
              src_id,
              max(src_volgnummer) as src_volgnummer
          FROM (
                   SELECT *
                   FROM legacy.mv_bag_lps_bag_nag_heeft_hoofdadres
                   UNION
                   SELECT *
                   FROM legacy.mv_bag_lps_bag_nag_heeft_nevenadres
               ) q GROUP BY dst_id, dst_volgnummer, src_id
      ) lps_adressen ON lps_adressen.dst_id = nag._id AND lps_adressen.dst_volgnummer = nag.volgnummer
    LEFT JOIN legacy.bag_ligplaatsen adresseert_ligplaats ON lps_adressen.src_id = adresseert_ligplaats._id and lps_adressen.src_volgnummer = adresseert_ligplaats.volgnummer
    -- SELECT adresseert_standplaats
    LEFT JOIN (
          SELECT
              dst_id,
              dst_volgnummer,
              src_id,
              max(src_volgnummer) as src_volgnummer
          FROM (
                   SELECT *
                   FROM legacy.mv_bag_sps_bag_nag_heeft_hoofdadres
                   UNION
                   SELECT *
                   FROM legacy.mv_bag_sps_bag_nag_heeft_nevenadres
               ) q GROUP BY dst_id, dst_volgnummer, src_id
      ) sps_adressen ON sps_adressen.dst_id = nag._id AND sps_adressen.dst_volgnummer = nag.volgnummer
    LEFT JOIN legacy.bag_standplaatsen adresseert_standplaats ON sps_adressen.src_id = adresseert_standplaats._id and sps_adressen.src_volgnummer = adresseert_standplaats.volgnummer
    -- SELECT in_onderzoek
    LEFT JOIN (
          SELECT
              src_id, src_volgnummer
          FROM legacy.mv_bag_nag_bag_ozk_heeft_onderzoeken rel
                   INNER JOIN legacy.bag_onderzoeken ozk
                              ON rel.dst_id = ozk._id
                                  AND rel.dst_volgnummer = ozk.volgnummer
                                  AND ozk.in_onderzoek = 'J'
          GROUP BY rel.src_id, rel.src_volgnummer
      ) ozk ON nag._id = ozk.src_id AND nag.volgnummer = ozk.src_volgnummer
    WHERE nag._date_deleted IS NULL
