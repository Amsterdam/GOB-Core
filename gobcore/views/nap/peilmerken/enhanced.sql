    SELECT identificatie
    ,      hoogte_tov_nap
    ,      jaar
    ,      json_build_object('code', merk_code, 'omschrijving', merk_omschrijving) as merk
    ,      omschrijving
    ,      windrichting
    ,      x_coordinaat_muurvlak
    ,      y_coordinaat_muurvlak
    ,      rws_nummer
    ,      geometrie
    FROM  nap_peilmerken
    WHERE publiceerbaar = TRUE