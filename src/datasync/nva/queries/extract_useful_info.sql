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
    -- authors Claude helped with formatting of the names
    (
    WITH contrib AS (
        SELECT
        CAST(json_extract_string(j.value, '$.sequence') AS INTEGER) AS seq,
        json_extract_string(j.value, '$.identity.name') AS full_name
        FROM json_each(r.entity_description__contributors_preview) AS j
    ),

    parts AS (
        SELECT
        seq,
        str_split(full_name, ' ') AS p
        FROM contrib
    ),

    -- Find the first position (index) of a surname particle like "van", "der", "de", ...
    particle AS (
        SELECT
        seq,
        p,
        (
            SELECT MIN(idx)
            FROM unnest(p) WITH ORDINALITY AS u(tok, idx)
            WHERE lower(tok) IN ('van','von','der','den','de','del','da','di','la','le','du','ter','ten')
        ) AS particle_pos
        FROM parts
    ),

    formatted AS (
        SELECT
        seq,

        -- Build surname:
        -- if we find a particle, surname = from particle_pos .. end (e.g. "van der Kooij")
        -- else surname = last token
        (
            CASE
            WHEN particle_pos IS NOT NULL
                THEN array_to_string(list_slice(p, particle_pos, 999999), ' ')
            ELSE list_element(p, -1)
            END
            || ', '
            ||
            -- Build initials from given-name tokens:
            array_to_string(
            list_transform(
                CASE
                WHEN particle_pos IS NOT NULL THEN
                    -- given names are tokens before particle_pos
                    CASE
                    WHEN list_element(list_slice(p, 1, particle_pos - 1), 3) IS NULL
                        THEN list_slice(p, 1, 1)                              -- only first initial
                    ELSE list_slice(p, 1, 2)                                 -- first two initials
                    END
                ELSE
                    -- given names are all tokens except the last (surname)
                    CASE
                    WHEN list_element(list_slice(p, 1, -1), 3) IS NULL
                        THEN list_slice(list_slice(p, 1, -1), 1, 1)
                    ELSE list_slice(list_slice(p, 1, -1), 1, 2)
                    END
                END,
                x -> substr(x, 1, 1) || '.'
            ),
            ''
            )
        ) AS name_fmt

        FROM particle
    ),

    ord AS (
        SELECT
        seq,
        name_fmt,
        row_number() OVER (ORDER BY seq) AS rn,
        count(*) OVER () AS n
        FROM formatted
    )

    SELECT
        string_agg(
        CASE
            WHEN n = 1 THEN name_fmt
            WHEN rn = 1 THEN name_fmt
            WHEN rn = n THEN ' & ' || name_fmt
            ELSE ', ' || name_fmt
        END,
        '' ORDER BY rn
        )
    FROM ord
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
