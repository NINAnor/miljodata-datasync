SELECT
  "source"."identifier" AS "identifier",
  "source"."model_version" AS "model_version",
  "source"."resource_owner__owner" AS "resource_owner__owner",
  "source"."resource_owner__owner_affiliation" AS "resource_owner__owner_affiliation",
  "source"."type" AS "type",
  "source"."acontext" AS "acontext",
  "source"."index_document_created_at" AS "index_document_created_at",
  "source"."entity_description__reference" AS "entity_description__reference",
  "source"."entity_description__contributors_count" AS "entity_description__contributors_count",
  "source"."entity_description__main_title" AS "entity_description__main_title",
  "source"."entity_description__alternative_abstracts" AS "entity_description__alternative_abstracts",
  "source"."entity_description__language" AS "entity_description__language",
  "source"."entity_description__contributors" AS "entity_description__contributors",
  "source"."entity_description__contributors_preview" AS "entity_description__contributors_preview",
  "source"."entity_description__type" AS "entity_description__type",
  "source"."entity_description__publication_date" AS "entity_description__publication_date",
  "source"."created_date" AS "created_date",
  "source"."modified_date" AS "modified_date",
  "source"."publisher__id" AS "publisher__id",
  "source"."publisher__type" AS "publisher__type",
  "source"."files_status" AS "files_status",
  "source"."id" AS "id",
  "source"."pending_open_file_count" AS "pending_open_file_count",
  "source"."published_date" AS "published_date",
  "source"."status" AS "status",
  "source"."_dlt_load_id" AS "_dlt_load_id",
  "source"."_dlt_id" AS "_dlt_id",
  "source"."entity_description__npi_subject_heading" AS "entity_description__npi_subject_heading",
  "source"."entity_description__abstract" AS "entity_description__abstract",
  "source"."entity_description__tags" AS "entity_description__tags",
  "source"."handle" AS "handle",
  "source"."entity_description__description" AS "entity_description__description",
  "source"."entity_description__metadata_source" AS "entity_description__metadata_source",
  "source"."scientific_index__year" AS "scientific_index__year",
  "source"."scientific_index__type" AS "scientific_index__type",
  "source"."scientific_index__status" AS "scientific_index__status",
  "source"."entity_description__alternative_titles" AS "entity_description__alternative_titles",
  "source"."doi" AS "doi",
  "source"."duplicate_of" AS "duplicate_of",
  "source"."ref_type" AS "ref_type",
  "source"."pub_instance_type" AS "pub_instance_type",
  "source"."volume" AS "volume",
  "source"."pages_type" AS "pages_type",
  "source"."ctx_identifier" AS "ctx_identifier",
  "source"."ctx_year" AS "ctx_year",
  "source"."ctx_name" AS "ctx_name",
  "source"."ctx_type" AS "ctx_type",
  "source"."ctx_print_issn" AS "ctx_print_issn",
  "source"."publication_year" AS "publication_year",
  "source"."series_name" AS "series_name",
  "source"."publication_month" AS "publication_month",
  "source"."publication_day" AS "publication_day",
  "source"."publication_date" AS "publication_date",
  (
    SELECT string_agg(
             json_extract_string(j.value, '$.identity.name'),
             ', '
             ORDER BY CAST(json_extract_string(j.value, '$.sequence') AS INTEGER)
           )
    FROM json_each("source".entity_description__contributors_preview) AS j
  ) AS contributors_names

FROM
  (
    SELECT
      r.*,
      -- top-level
      json_extract_string(r.entity_description__reference, '$.type') AS ref_type,
      -- publicationInstance
      json_extract_string(
        r.entity_description__reference,
        '$.publicationInstance.type'
      ) AS pub_instance_type,
      json_extract_string(
        r.entity_description__reference,
        '$.publicationInstance.volume'
      ) AS volume,
      -- pages (nested inside publicationInstance)
      json_extract_string(
        r.entity_description__reference,
        '$.publicationInstance.pages.type'
      ) AS pages_type,
      --json_extract_string(r.entity_description__reference, '$.publicationInstance.pages.begin') AS pages_begin,
      --json_extract_string(r.entity_description__reference, '$.publicationInstance.pages.end')   AS pages_end,
      -- publicationContext
      json_extract_string(
        r.entity_description__reference,
        '$.publicationContext.identifier'
      ) AS ctx_identifier,
      json_extract_string(
        r.entity_description__reference,
        '$.publicationContext.year'
      ) AS ctx_year,
      json_extract_string(
        r.entity_description__reference,
        '$.publicationContext.name'
      ) AS ctx_name,
      json_extract_string(
        r.entity_description__reference,
        '$.publicationContext.type'
      ) AS ctx_type,
      json_extract_string(
        r.entity_description__reference,
        '$.publicationContext.printIssn'
      ) AS ctx_print_issn,
      -- publication date parts (conditionally present)
      json_extract_string(r.entity_description__publication_date, '$.year') AS publication_year,
      json_extract_string(
        r.entity_description__reference,
        '$.publicationContext.series.name'
      ) AS series_name,
      CASE
        WHEN json_extract_string(r.entity_description__publication_date, '$.month') IS NOT NULL THEN CAST(
          json_extract_string(r.entity_description__publication_date, '$.month') AS INTEGER
        )
        ELSE NULL
      END AS publication_month,
      CASE
        WHEN json_extract_string(r.entity_description__publication_date, '$.day') IS NOT NULL THEN CAST(
          json_extract_string(r.entity_description__publication_date, '$.day') AS INTEGER
        )
        ELSE NULL
      END AS publication_day,
      -- one "best effort" DATE column:
      -- - if year+month+day exist -> that exact date
      -- - if only year exists -> 1st Jan of that year (change if you prefer NULL instead)
      CASE
        WHEN json_extract_string(r.entity_description__publication_date, '$.year') IS NULL THEN NULL
        WHEN json_extract_string(r.entity_description__publication_date, '$.month') IS NOT NULL

   AND json_extract_string(r.entity_description__publication_date, '$.day') IS NOT NULL THEN make_date(
          CAST(
            json_extract_string(r.entity_description__publication_date, '$.year') AS INTEGER
          ),
          CAST(
            json_extract_string(r.entity_description__publication_date, '$.month') AS INTEGER
          ),
          CAST(
            json_extract_string(r.entity_description__publication_date, '$.day') AS INTEGER
          )
        )
        ELSE make_date(
          CAST(
            json_extract_string(r.entity_description__publication_date, '$.year') AS INTEGER
          ),
          1,
          1
        )
      END AS publication_date
    FROM
      read_parquet(
        $data_s3_path
      ) r
  ) AS "source"
WHERE
  (
    LOWER("source"."pub_instance_type") LIKE '%report%'
  )
  AND (
    ("source"."series_name" = 'NINA Dararapport')

    OR ("source"."series_name" = 'NINA Datarapport')
  )
ORDER BY
  "source"."published_date" DESC
LIMIT
  100
