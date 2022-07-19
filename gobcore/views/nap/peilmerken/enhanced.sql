    SELECT identificatie
    ,      hoogte_tov_nap
    ,      jaar
    ,      merk
    ,      omschrijving
    ,      windrichting
    ,      x_coordinaat_muurvlak
    ,      y_coordinaat_muurvlak
    ,      rws_nummer
    ,      geometrie
    FROM  legacy.nap_peilmerken
    WHERE publiceerbaar = TRUE
