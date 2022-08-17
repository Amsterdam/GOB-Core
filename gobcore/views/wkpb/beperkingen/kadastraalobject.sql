SELECT
  bpg.identificatie,
    kot.identificatie AS kadastraal_object,
    (kot.aangeduid_door_kadastralegemeentecode -> 'broninfo'::text) ->> 'omschrijving'::text AS aangeduid_door_kadastralegemeentecode,
    kse.code AS aangeduid_door_kadastralesectie,
    kot.perceelnummer,
    kot.indexletter,
    kot.indexnummer
FROM legacy.b_beperkingen bpg
INNER JOIN legacy.l_wkpb_bpg_brk_kot_belast_kadastrale_objecten rel_bpg_kot
  ON rel_bpg_kot.src_id = bpg._id
INNER JOIN legacy.k_kadastraleobjecten kot
  ON  rel_bpg_kot.dst_id = kot._id
  AND rel_bpg_kot.dst_volgnummer = kot.volgnummer
INNER JOIN legacy.v_brk_kot_brk_kse__angdd_dr__kadastralesectie rel_kot_kse
  ON kot."_id"  = rel_kot_kse.src_id
  AND kot.volgnummer  = rel_kot_kse.src_volgnummer
INNER JOIN legacy.k_kadastralesecties kse
  ON rel_kot_kse.dst_id  = kse."_source_id"
WHERE bpg._expiration_date IS NULL or bpg._expiration_date > NOW()
ORDER BY
  bpg.identificatie