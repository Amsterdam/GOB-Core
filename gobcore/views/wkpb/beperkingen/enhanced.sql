SELECT identificatie
     , beperking
     , begin_geldigheid
     , eind_geldigheid
FROM 
    wkpb_beperkingen bpg 
WHERE "status" -> 'code' = '3' 
      AND aard -> 'code' <> '3'
      AND (bpg._expiration_date IS NULL 
        OR bpg._expiration_date > NOW()) 
      AND "_date_deleted" IS NULL
ORDER BY 
    bpg.identificatie