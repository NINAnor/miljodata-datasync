CREATE OR REPLACE TABLE vernacular_name AS
SELECT DISTINCT
    CONCAT_WS('|', taxonid, language, vernacularname) AS vernacularid,
    *
FROM pa_vernacular_names;
CREATE INDEX vernacular_names_taxonid ON vernacular_name (taxonid);
CREATE INDEX vernacular_names_language ON vernacular_name (language);
PRAGMA create_fts_index(
    'vernacular_name', 'vernacularID', 'vernacularName',
    overwrite = TRUE
);
