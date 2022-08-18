    SELECT meetbouten_rollagen.identificatie
    ,       ROW_NUMBER() OVER (order by meetbouten_rollagen.identificatie) AS idx
    ,       ST_centroid(bouwblokken.geometrie) AS geometrie
    FROM  legacy.meetbouten_rollagen
        LEFT JOIN
            legacy.gebieden_bouwblokken AS bouwblokken
            ON meetbouten_rollagen.identificatie = bouwblokken.code
    ORDER BY meetbouten_rollagen.identificatie
