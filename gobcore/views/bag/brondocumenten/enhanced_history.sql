SELECT
  bag.dossier,
  dsr.heeft_brondocument AS documentnummer,
  date(bdt.registratiedatum) AS registratiedatum,
  bdt.bronleverancier->>'omschrijving' AS bronleverancier,
  bdt.type_dossier->>'omschrijving' AS type_dossier,
  bdt.type_brondocument->>'omschrijving' AS type_brondocument
FROM (
  SELECT lps_dsr.dst_id AS dossier
  FROM mv_bag_lps_bag_dsr_heeft_dossier lps_dsr
  INNER JOIN bag_ligplaatsen lig
    ON lps_dsr.src_id = lig.identificatie
    AND lps_dsr.src_volgnummer = lig.volgnummer
    AND lig._date_deleted IS NULL
  UNION
  SELECT nag_dsr.dst_id AS dossier
  FROM mv_bag_nag_bag_dsr_heeft_dossier nag_dsr
  INNER JOIN bag_nummeraanduidingen nag
    ON nag_dsr.src_id = nag.identificatie
    AND nag_dsr.src_volgnummer = nag.volgnummer
    AND nag._date_deleted IS NULL
  UNION
  SELECT ore_dsr.dst_id AS dossier
  FROM mv_bag_ore_bag_dsr_heeft_dossier ore_dsr
  INNER JOIN bag_openbareruimtes ore
    ON ore_dsr.src_id = ore.identificatie
    AND ore_dsr.src_volgnummer = ore.volgnummer
    AND ore._date_deleted IS NULL
  UNION
  SELECT pnd_dsr.dst_id AS dossier
  FROM mv_bag_pnd_bag_dsr_heeft_dossier pnd_dsr
  INNER JOIN bag_panden pnd
    ON pnd_dsr.src_id = pnd.identificatie
    AND pnd_dsr.src_volgnummer = pnd.volgnummer
    AND pnd._date_deleted IS NULL
  UNION
  SELECT sps_dsr.dst_id AS dossier
  FROM mv_bag_sps_bag_dsr_heeft_dossier sps_dsr
  INNER JOIN bag_standplaatsen sps
    ON sps_dsr.src_id = sps.identificatie
    AND sps_dsr.src_volgnummer = sps.volgnummer
    AND sps._date_deleted IS NULL
  UNION
  SELECT vot_dsr.dst_id AS dossier
  FROM mv_bag_vot_bag_dsr_heeft_dossier vot_dsr
  INNER JOIN bag_verblijfsobjecten vot
    ON vot_dsr.src_id = vot.identificatie
    AND vot_dsr.src_volgnummer = vot.volgnummer
    AND vot._date_deleted IS NULL
  UNION
  SELECT wps_dsr.dst_id AS dossier
  FROM mv_bag_wps_bag_dsr_heeft_dossier wps_dsr
  INNER JOIN bag_openbareruimtes wps
    ON wps_dsr.src_id = wps.identificatie
    AND wps_dsr.src_volgnummer = wps.volgnummer
    AND wps._date_deleted IS NULL
) bag
LEFT JOIN (
  SELECT dossier, dsr_bdt.dst_id AS heeft_brondocument
  FROM bag_dossiers dsr
  INNER JOIN mv_bag_dsr_bag_bdt_heeft_brondocumenten dsr_bdt
    ON dsr."_source_id" = dsr_bdt.src_id
    AND dsr._date_deleted IS NULL
) dsr ON dsr.dossier = bag.dossier
LEFT JOIN bag_brondocumenten bdt ON dsr.heeft_brondocument = bdt.documentnummer
WHERE dsr.heeft_brondocument IS NOT NULL
