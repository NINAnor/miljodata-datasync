CREATE OR REPLACE TABLE taxon AS FROM pa_taxon;
CREATE INDEX taxon_taxonid ON taxon (taxonid);
PRAGMA create_fts_index(
    'taxon', 'taxonID', 'canonicalName',
    overwrite=TRUE
);
