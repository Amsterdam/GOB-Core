    SELECT
        ggpgebieden.identificatie
    ,   ggpgebieden.code
    ,   ggpgebieden.naam
    ,   ggpgebieden.begin_geldigheid
    ,   ggpgebieden.eind_geldigheid
    ,   ggpgebieden.documentdatum
    ,   ggpgebieden.documentnummer
    ,   json_build_object(
             'identificatie', stadsdelen.identificatie,
             'code', stadsdelen.code,
             'naam', stadsdelen.naam
        ) AS _ref_ligt_in_stadsdeel_gbd_sdl
    ,   json_build_object(
             'identificatie', gemeentes.identificatie,
             'naam', gemeentes.naam
        ) AS _ref_ligt_in_gemeente_brk_gme
    ,   ggpgebieden.geometrie
    FROM
        gebieden_ggpgebieden AS ggpgebieden
        LEFT JOIN
            (SELECT
                identificatie
            ,   code
            ,   naam
            ,   eind_geldigheid
            ,   ligt_in_gemeente
            FROM gebieden_stadsdelen
            WHERE eind_geldigheid IS NULL
            ) AS stadsdelen
            ON ggpgebieden.ligt_in_stadsdeel->>'id' = stadsdelen.identificatie
        LEFT JOIN
            (SELECT
                identificatie
            ,   naam
            ,   eind_geldigheid
            FROM brk_gemeentes
            WHERE eind_geldigheid IS NULL
            ) AS gemeentes
            ON stadsdelen.ligt_in_gemeente->>'id' = gemeentes.identificatie
    WHERE ggpgebieden.eind_geldigheid IS NULL
    ORDER BY ggpgebieden.code