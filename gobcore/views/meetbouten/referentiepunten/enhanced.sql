    SELECT identificatie
    ,      geometrie
    ,      hoogte_tov_nap
    ,      datum
    ,      CONCAT_WS(' ', nabij_nummeraanduiding->>'bronwaarde', locatie)       AS locatie
    FROM  legacy.n_referentiepunten
    WHERE publiceerbaar = TRUE