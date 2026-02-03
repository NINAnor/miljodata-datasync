SELECT
  "source"."publication_month" AS "publication_month",
  "source"."publication_day" AS "publication_day",
  "source"."citation_contributors_names" AS "citation_contributors_names",
  "source"."volume" AS "volume",
  "source"."ctx_name" AS "ctx_name",
  "source"."id" AS "id",
  "source"."publication_year" AS "publication_year",
  "source"."series_name" AS "series_name",
  "source"."series_number" AS "series_number",
  "source"."entity_description__main_title" AS "entity_description__main_title",
  "source"."doi_url" AS "doi_url"
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
        '$.publicationInstance.volume'
      ) AS volume,
      list_extract(
        (
          SELECT
            list(
              ai.value
              ORDER BY
                ai._dlt_list_idx
            )
          FROM additional_identifiers as ai
            WHERE
            ai._dlt_parent_id = r._dlt_id
   AND ai.source_name = 'nva@sikt'
        ),
        1
      ) AS nva_sikt_handle,
      list_extract(
        (
          SELECT
            list(
              ai.value
              ORDER BY
                ai._dlt_list_idx
            )
          FROM additional_identifiers as ai
          WHERE
            ai._dlt_parent_id = r._dlt_id
            AND ai.source_name = 'nva@brage'
        ),
        1
      ) AS nva_brage_handle,
      list_extract(
        (
          SELECT
            list(
              ai.value
              ORDER BY
                ai._dlt_list_idx
            )
          FROM additional_identifiers as ai
          WHERE
            ai._dlt_parent_id = r._dlt_id
            AND ai.source_name = 'cristin@nina'
        ),
        1
      ) AS cristin_nina_handle,
      list_extract(
        (
          SELECT
            list(
              ai.value
              ORDER BY
                ai._dlt_list_idx
            )
          FROM additional_identifiers as ai
          WHERE
            ai._dlt_parent_id = r._dlt_id
            AND ai.source_name = 'brage@nina'
        ),
        1
      ) AS brage_nina_handle,
      list_extract(
        (
          SELECT
            list(
              ai.value
              ORDER BY
                ai._dlt_list_idx
            )
          FROM additional_identifiers as ai
          WHERE
            ai._dlt_parent_id = r._dlt_id
            AND ai.source_name = 'nva@nnull'
        ),
        1
      ) AS nva_null_handle,
      list_extract(
        (
          SELECT
            list(
              ai.value
              ORDER BY
                ai._dlt_list_idx
            )
          FROM additional_identifiers as ai
          WHERE
            ai._dlt_parent_id = r._dlt_id
            AND ai.source_name = 'cristin@ntnu'
        ),
        1
      ) AS cristin_ntnu_handle,
      list_extract(
        (
          SELECT
            list(
              ai.value
              ORDER BY
                ai._dlt_list_idx
            )
          FROM additional_identifiers as ai
          WHERE
            ai._dlt_parent_id = r._dlt_id
            AND ai.source_name = 'cristin@niva'
        ),
        1
      ) AS cristin_niva_handle,
      list_extract(
        (
          SELECT
            list(
              ai.value
              ORDER BY
                ai._dlt_list_idx
            )
          FROM additional_identifiers as ai
          WHERE
            ai._dlt_parent_id = r._dlt_id
            AND ai.source_name = 'cristin@nmbu'
        ),
        1
      ) AS cristin_nmbu_handle,
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
        '$.publicationContext.series.printIssn'
      ) AS ctx_print_issn,
      json_extract_string(r.entity_description__reference, '$.doi') AS doi_url,
      json_extract_string(
        entity_description__reference,
        '$.publicationContext.seriesNumber'
      ) AS series_number,
      json_extract_string(
        entity_description__reference,
        '$.publicationContext.series.onlineIssn'
      ) AS online_issn,
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
      -- authors
      (
        WITH contrib AS (
          SELECT
            CAST(
              json_extract_string(j.value, '$.sequence') AS INTEGER
            ) AS seq,
            json_extract_string(j.value, '$.identity.name') AS full_name
          FROM
            json_each(r.entity_description__contributors_preview) AS j
        ),
        formatted AS (
          SELECT
            seq,
            -- Surname = last token; initials = first 1 (if 1–2 given names) or first 2 (if 3+ given names)
            list_element(parts, -1) || ', ' || array_to_string(
              list_transform(
                CASE
                  -- look at 3rd given-name token (after removing surname); if missing -> keep only first given name
                  WHEN list_element(list_slice(parts, 1, -1), 3) IS NULL THEN list_slice(list_slice(parts, 1, -1), 1, 1) -- otherwise keep first two given names
                  ELSE list_slice(list_slice(parts, 1, -1), 1, 2)
                END,
                x -> substr(x, 1, 1) || '.'
              ),
              ''
            ) AS name_fmt
          FROM
            (
              SELECT
                seq,
                str_split(full_name, ' ') AS parts
              FROM
                contrib
            )
        ),
        ord AS (
          SELECT
            seq,
            name_fmt,
            row_number() OVER (
              ORDER BY
                seq
            ) AS rn,
            count(*) OVER () AS n
          FROM
            formatted
        )
        SELECT
          string_agg(
            CASE
              WHEN n = 1 THEN name_fmt
              WHEN rn = 1 THEN name_fmt
              WHEN rn = n THEN ' & ' || name_fmt
              ELSE ', ' || name_fmt
            END,
            ''
            ORDER BY
              rn
          )
        FROM
          ord
      ) AS citation_contributors_names,
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
          json_each(r.entity_description__contributors_preview) AS j
      ) AS contributors_names,
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
    FROM resources AS r
  ) AS "source"
WHERE
  (
    (
      LOWER("source"."pub_instance_type") LIKE '%academicarticle%'
    )

    OR (
      LOWER("source"."pub_instance_type") LIKE '%academicliteraturereview%'
    )
    OR (
      LOWER("source"."pub_instance_type") LIKE '%academicmonograph%'
    )
    OR (
      LOWER("source"."pub_instance_type") LIKE '%reportresearch%'
    )
    OR (
      LOWER("source"."pub_instance_type") LIKE '%academicchapter%'
    )
  )
  AND ("source"."doi_url" IS NOT NULL)
  AND (
    ("source"."doi_url" <> '')
    OR ("source"."doi_url" IS NULL)
  )
ORDER BY
  "source"."publication_date" DESC
