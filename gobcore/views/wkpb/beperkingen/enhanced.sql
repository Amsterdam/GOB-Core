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
    legacy.wkpb_beperkingen bpg
WHERE
      aard -> 'code' <> '3'
      AND COALESCE(_expiration_date, '9999-12-31'::timestamp without time zone) > NOW()
      AND _date_deleted IS NULL
ORDER BY 
    bpg.identificatie
