SELECT bpg.documentnummer,
    bpg.heeft_dossier as dossier,
    bpg.identificatie,
    bpg.persoonsgegevens_afschermen,
    bpg.orgaan,
    bpg.aard
FROM  
    wkpb_beperkingen bpg
WHERE bpg.status -> 'code' = '3'
      AND bpg.aard -> 'code' <> '3'
      AND (bpg._expiration_date IS NULL 
        OR bpg._expiration_date > now())
      AND COALESCE(_expiration_date, '9999-12-31'::timestamp without time zone) > NOW()
ORDER BY 
    bpg.identificatie