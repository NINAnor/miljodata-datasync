SELECT
  "source".id AS "id",
  "source"."entity_description__main_title" AS "entity_description__main_title",
  "source"."entity_description__contributors_preview" AS "entity_description__contributors_preview",
  "source"."pub_instance_type" AS "pub_instance_type",
  "source"."ctx_print_issn" AS "ctx_print_issn",
  "source"."publication_year" AS "publication_year",
  "source"."publication_month" AS "publication_month",
  "source"."publication_day" AS "publication_day",
  "source"."publication_date" AS "publication_date",
  "source"."contributors_names" AS "contributors_names",
  "source"."ctx_print_issn" AS "ctx_print_issn",
FROM
  (
    SELECT
	  "source".id AS "id",
      "source"."entity_description__main_title" AS "entity_description__main_title",
      "source"."entity_description__contributors_preview" AS "entity_description__contributors_preview",
      "source"."pub_instance_type" AS "pub_instance_type",
      "source"."ctx_print_issn" AS "ctx_print_issn",
      "source"."publication_year" AS "publication_year",
      "source"."publication_month" AS "publication_month",
      "source"."publication_day" AS "publication_day",
      "source"."publication_date" AS "publication_date",
      (
        SELECT
          string_agg(
            json_extract_string(j.value, '$.identity.name'),
            ', '
			ORDER BY
              CAST(
                json_extract_string(j.value, '$.sequence') AS INTEGER
              )
          )
        FROM
          json_each(
            "source".entity_description__contributors_preview
          ) AS j
      ) AS contributors_names
    FROM
      (
        SELECT
          r.*,
          json_extract_string(
            r.entity_description__reference,
            '$.publicationInstance.type'
          ) AS pub_instance_type,
          json_extract_string(
            r.entity_description__reference,
            '$.publicationContext.printIssn'
          ) AS ctx_print_issn,

          json_extract_string(r.entity_description__publication_date, '$.year') AS publication_year,
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
        LOWER("source"."pub_instance_type") LIKE '%academicarticle%'
      )

    OR (
        LOWER("source"."pub_instance_type") LIKE '%academicliteraturereview%'
      )
      OR (
        LOWER("source"."pub_instance_type") LIKE '%professionalarticle%'
      )
      OR (
        LOWER("source"."pub_instance_type") LIKE '%popularsciencearticle%'
      )
      OR (
        LOWER("source"."pub_instance_type") LIKE '%academicmonograph%'
      )
      OR (
        LOWER("source"."pub_instance_type") LIKE '%nonfictionmonograph%'
      )
      OR (
        LOWER("source"."pub_instance_type") LIKE '%popularsciencemonograph%'
      )
      OR (
        LOWER("source"."pub_instance_type") LIKE '%bookanthology%'
      )
      OR (
        LOWER("source"."pub_instance_type") LIKE '%reportresearch%'
      )
      OR (
        LOWER("source"."pub_instance_type") LIKE '%reportbasic%'
      )
      OR (
        LOWER("source"."pub_instance_type") LIKE '%degreemaster%'
      )
      OR (
        LOWER("source"."pub_instance_type") LIKE '%degreephd%'
      )
      OR (
        LOWER("source"."pub_instance_type") LIKE '%academicchapter%'
      )
      OR (
        LOWER("source"."pub_instance_type") LIKE '%nonfictionchapter%'
      )
      OR (
        LOWER("source"."pub_instance_type") LIKE '%popularsciencechapter%'
      )
    ORDER BY
      "source"."published_date" DESC

LIMIT
      100
  ) AS "source"
ORDER BY
  "source"."publication_date" DESC
LIMIT
  1048575
