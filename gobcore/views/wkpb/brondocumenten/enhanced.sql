SELECT bpg.documentnummer,
    bpg.heeft_dossier as dossier,
    bpg.identificatie,
    bpg.persoonsgegevens_afschermen,
    bpg.orgaan,
    bpg.aard
FROM  
    legacy.wkpb_beperkingen bpg
WHERE bpg.aard -> 'code' <> '3'
      AND COALESCE(_expiration_date, '9999-12-31'::timestamp without time zone) > NOW()
      AND _date_deleted IS NULL
ORDER BY 
    bpg.identificatie
