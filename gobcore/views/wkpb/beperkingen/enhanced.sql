SELECT identificatie
     , beperking
     , documentnummer
     , heeft_dossier as dossier
     , persoonsgegevens_afschermen
     , orgaan
     , aard
     , begin_geldigheid
     , eind_geldigheid
FROM 
    wkpb_beperkingen bpg 
WHERE "status" -> 'code' = '3' 
      AND aard -> 'code' <> '3'
      AND (bpg._expiration_date IS NULL 
        OR bpg._expiration_date > NOW()) 
      AND COALESCE(_expiration_date, '9999-12-31'::timestamp without time zone) > NOW()
ORDER BY 
    bpg.identificatie