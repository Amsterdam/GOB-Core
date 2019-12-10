SELECT
  gemeente,
  kadastrale_gemeente,
  kadastrale_gemeentecode,
  kadastrale_sectie_code,
  ST_CollectionExtract(ST_Collect(geometrie), 2) AS geometrie
FROM (
  SELECT
    k1.gemeente,
    k1.kadastrale_gemeente,
    k1.kadastrale_gemeentecode,
    k1.kadastrale_sectie_code,
    ST_Intersection(k1.geometrie, k2.geometrie) AS geometrie
    FROM (
      SELECT
        g.naam AS gemeente,
        kg.identificatie AS kadastrale_gemeente,
        kgc.identificatie AS kadastrale_gemeentecode,
        ks.code AS kadastrale_sectie_code,
        ST_MakeValid((ST_Dump(ks.geometrie)).geom) AS geometrie
      FROM brk_kadastralesecties ks
      LEFT JOIN brk_kadastralegemeentecodes kgc
      ON ks.is_onderdeel_van_kadastralegemeentecode->>'id' = kgc.identificatie
      LEFT JOIN brk_kadastralegemeentes kg
      ON kgc.is_onderdeel_van_kadastralegemeente->>'id' = kg.identificatie
      LEFT JOIN brk_gemeentes g
      ON kg.ligt_in_gemeente->>'id' = g.identificatie
    ) AS k1,
    (
      SELECT
        identificatie,
        ST_MakeValid((ST_Dump(geometrie)).geom) AS geometrie
      FROM brk_kadastralesecties
    ) AS k2
    WHERE
      k1.geometrie && k2.geometrie
      AND ST_IsValid(k1.geometrie)
      AND ST_IsValid(k2.geometrie)
) AS intersections
WHERE
  GeometryType(intersections.geometrie)
  IN ('MULTILINESTRING'::text, 'LINESTRING'::text)
GROUP BY kadastrale_sectie_code, gemeente, kadastrale_gemeente, kadastrale_gemeentecode
