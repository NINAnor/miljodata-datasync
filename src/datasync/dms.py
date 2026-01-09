import duckdb
import orjson
import typer
from pygeometa.schemas.iso19139 import ISO19139OutputSchema

from .libs.helpers import get_anytext
from .settings import (
    env,
    log,
)

DMS_DATASETS_BASE = env("DMS_DATASETS_BASE", default="")
DMS_ACCESS_KEY = env("DMS_ACCESS_KEY", default="")
DMS_SECRET_KEY = env("DMS_SECRET_KEY", default="")
DMS_AWS_ENDPOINT = env("DMS_AWS_ENDPOINT", default="")
DMS_BUCKET = env("DMS_BUCKET", default="")
DMS_GEOAPI_PREFIX = env("DMS_GEOAPI_PREFIX", default="/geoapi/")
DMS_CSW_PREFIX = env("DMS_CSW_PREFIX", default="/csw/dms-metadata.parquet")


app = typer.Typer()

iso = ISO19139OutputSchema()


def guess_type(driver: str) -> str:
    # TODO: improve mapping based on the driver
    return "image/tiff"


def to_iso19139(metadata: str) -> str | dict:
    loaded = orjson.loads(metadata)
    log.debug(loaded, id=loaded.get("identification", {}).get("identifier"))
    try:
        xml = iso.write(loaded)
        return xml
    except Exception as e:
        log.exception(e, id=loaded.get("identification", {}).get("identifier"))
        return ""


@app.command()
def generate_csw_metadata(
    base_url: str = DMS_DATASETS_BASE,
    access_key=DMS_ACCESS_KEY,
    secret_key=DMS_SECRET_KEY,
    endpoint=DMS_AWS_ENDPOINT,
    bucket=DMS_BUCKET,
    prefix=DMS_CSW_PREFIX,
    lang="en",
):
    # TODO: localize the output based on the language in input

    conn = duckdb.connect()
    conn.sql("INSTALL yaml FROM community; LOAD yaml; Install spatial; load spatial")

    conn.execute(f"""
        CREATE OR REPLACE SECRET secret (
            TYPE s3,
            REGION 'eu-west-1',
            KEY_ID '{access_key}',
            SECRET '{secret_key}',
            ENDPOINT '{endpoint.replace(r"https://", "")}',
            URL_STYLE 'path'
        );
    """).fetchall()

    conn.create_function("to_iso19139", to_iso19139)
    conn.create_function("xml_bag", get_anytext)

    datasets = conn.read_parquet(base_url + "datasets_dataset.parquet").filter(
        "json_keys(metadata) <> []"
    )
    log.debug(datasets)
    rasters = conn.read_parquet(base_url + "datasets_rasterresource.parquet")  # noqa: F841
    vectors = conn.read_parquet(base_url + "datasets_tabularresource.parquet")  # noqa: F841
    contributions = conn.read_parquet(
        base_url + "datasets_datasetcontribution.parquet"
    ).select("* replace (split(rtrim(ltrim(roles, '{'), '}'), ',') as roles)")

    resources = conn.sql("""
            from rasters
            select * replace (ST_GEomFromHEXWKB(extent) as extent),
                'raster' as type
            union all by name
            from vectors select * replace (ST_GEomFromHEXWKB(extent) as extent),
                case when extent is null then 'table' else 'vector' end as type
    """)
    log.debug(resources)

    metadata = resources.select(f"""
        id,
        dataset_id,
        {{
        -- TODO: investigate language
        language: '{lang}',
        charset: 'utf8',
        hierarchylevel: case
            when extent is not null then 'dataset' else 'nonGeographicDataset'
        end,
        datestamp: current_localtimestamp()::date,
        -- TODO: this needs some fixes
        dataseturi: uri,
        }}::json as metadata
                    """)

    log.debug(metadata)

    spatial = resources.select("""
        id,
        {
            datatype: case
                when type = 'table' then 'textTable'
                when type = 'raster' then 'grid'
                else 'vector'
            end,
            -- this needs to be fixed in a smarter way
            geomtype: 'point',
        }::json as spatial
        """)

    log.debug(spatial)

    descriptions_structure = datasets.aggregate(
        "json_group_structure(metadata->'$.descriptions[*]') as stucture"
    ).fetchone()
    descriptions_structure = (
        descriptions_structure[0] if descriptions_structure else None
    )

    log.debug("Generated schema of abstract", structure=descriptions_structure)

    descriptions = (
        resources.set_alias("r")
        .join(datasets.set_alias("d"), condition="r.dataset_id = d.id")
        .select(
            f"r.id, unnest(json_transform(d.metadata->'$.descriptions[*]', '{descriptions_structure}'), recursive := true)"  # noqa: E501
        )
    )
    log.debug(descriptions)

    abstracts = descriptions.filter(
        (
            duckdb.ColumnExpression("descriptionType")
            == duckdb.ConstantExpression("Abstract")
        )
        and (duckdb.ColumnExpression("lang") == duckdb.ConstantExpression(lang)),
    ).aggregate("id, lang, string_agg(description, '\n') as description")
    log.debug(abstracts)

    licenses = (
        resources.set_alias("r")
        .join(datasets.set_alias("d"), condition="r.dataset_id = d.id")
        .select(
            """r.id, unnest(json_transform(d.metadata->'$.rightsList[*]', '[{"lang": "VARCHAR", "rights": "VARCHAR", "rightsURI": "VARCHAR", "rightsIdentifier": "VARCHAR", "schemeUri": "VARCHAR"}]'), recursive := true)"""  # noqa: E501
        )
        .filter(duckdb.ColumnExpression("lang") == duckdb.ConstantExpression(lang))
        .aggregate("id, lang, first(rights) as rights, first(rightsURI) as url")
    )
    log.debug(licenses)

    subjects = (
        resources.set_alias("r")
        .join(datasets.set_alias("d"), condition="r.dataset_id = d.id")
        .select(
            """r.id, unnest(json_transform(d.metadata->'$.subjects[*]', '[{"subject": "VARCHAR", "subjectScheme": "VARCHAR", "schemeURI": "VARCHAR", "valueURI": "VARCHAR", "classificationCode": "VARCHAR"}]'), recursive := true)"""  # noqa: E501
        )
        .aggregate(
            "id, array_agg(subject) as keywords, subjectScheme as scheme_name, schemeURI as url"  # noqa: E501
        )
        .aggregate(
            """id,
            json_group_object(
                coalesce(scheme_name, 'default'),
                case when url is not null then
                {"keywords_type": 'theme', "keywords": keywords, "vocabulary": {
                    "name": scheme_name, "url": url
                },}
                else
                {"keywords_type": 'theme', "keywords": keywords }
                end
            ) as keywords
            """
        )
    )
    log.debug(subjects)

    identification = conn.sql("""
    from resources as r
    join datasets as d on r.dataset_id = d.id
    left join abstracts as a on a.id = r.id
    left join licenses as l on l.id = r.id
    left join subjects as s on s.id = r.id
    select
        r.id,
        {
            identifier: r.id,
            language: d.metadata->'$.language',
            title: d.title || ' - ' || r.title,
            fee: 'None',
            status: 'completed',
            rights: l.rights,
            abstract: a.description || '\n' || r.description,
            url: r.uri,
            dates: {
                creation: r.created_at,
                revision: r.last_modified_at
            },
            extents: {
                spatial: case when r.extent is not null then [
                    {
                        bbox: [
                            ST_XMIN(r.extent), ST_YMIN(r.extent),
                            ST_XMAX(r.extent), ST_YMAX(r.extent),
                        ],
                        crs: 4326
                    }
                ] else [{
                    -- default to Norway BBOX
                    bbox: [
                        4.99207807783, 58.0788841824, 31.29341841, 80.6571442736
                    ],
                    crs: 4326
                    }]
                end
            },
            license: {
                "name": coalesce(l.rights, 'All rights reserved'),
                "uri": coalesce(l.url, ''),
            },
            keywords: coalesce(s.keywords, {
                "default": {
                    -- need to join with dataset to read those!
                    keywords_type: 'theme',
                    keywords: [],
                }
            }),
        }::json as identification
    """)  # noqa: E501

    log.debug(identification)

    content_info = resources.select("""
        id,
        {
            type: case
                when type = 'raster' then 'coverage'
                else 'feature_catalogue'
            end,
            dimensions: [],
        }::json as content_info
    """)

    log.debug(content_info)

    contacts = (
        contributions.select("""
                dataset_id,
                unnest(roles) as role,
                {
                    organization: 'Norsk institutt for naturforsking',
                    individualname: last_name || ', ' || first_name,
                    -- default to NINA
                    phone: '' ,
                    positionname: '',
                    fax: '',
                    address: '',
                    postalcode: '',
                    country: 'Norway',
                    email: email,
                    url: 'https://www.nina.no',
                    city: 'Trondheim'
                } as contact,
    """)
        .select(
            "dataset_id, role, contact, row_number() over (partition by dataset_id, role order by contact->>'individualname') as role_order"  # noqa: E501
        )
        .aggregate(
            "dataset_id, json_group_object(lower(role[1]) || role[2:] || case when role_order = 1 then '' else ('_' || role_order) end, contact) as contact",  # noqa: E501
            group_expr="dataset_id",
        )
    )

    contributors_csw = contributions.aggregate(
        "dataset_id, string_agg(last_name || ', ' || first_name, ';') as contributors",
        group_expr="dataset_id",
    )

    log.debug(contributors_csw)

    log.debug(contacts)

    distribution = resources.select("""
        id,
        {
            file: {
                url: uri,
                type: case when type = 'raster' then 'FILE:GEO'
                        when type = 'vector' then 'FILE:GEO'
                        else 'download'
                end,
                name: title,
                description: description,
                function: 'download'
            }
        }::json as distribution
        """)

    log.debug(distribution)

    res = conn.sql(
        """
            select r.* rename (metadata as res_metadata),
            {
                mcf: { version: 1.0 },
                metadata: m.metadata,
                identification: i.identification,
                content_info: ci.content_info,
                spatial: sp.spatial,
                distribution: d.distribution,
                contact: c.contact
            }::json as metadata
            from resources as r
            join metadata as m on r.id = m.id
            join identification as i on m.id = i.id
            join content_info as ci on ci.id = m.id
            join spatial as sp on sp.id = m.id
            join distribution as d on d.id = m.id
            join contacts as c on m.dataset_id = c.dataset_id
        """
    )
    log.debug(res)

    iso_xml = res.select("*, to_iso19139(metadata) as xml")

    log.debug(iso_xml)

    csw = conn.sql("""
        select
            r.id as identifier,
            'gmd:MD_Metadata' as typename,
            'http://www.isotc211.org/2005/gmd' as schema,
            'local' as mdsource,
            coalesce(
                strftime(r.created_at, '%Y-%m-%d'),
                ''
            ) as insert_date,
            r.metadata->>'$.identification.title' as title,
            coalesce(
                strftime(r.last_modified_at, '%Y-%m-%d'),
                ''
            ) as date_modified,
            'dataset' as type,
            null::varchar as format,
            ST_AsText(case
                when r.extent is not null then r.extent
                else st_makeEnvelope(4.99207807783, 58.0788841824, 31.29341841, 80.6571442736)
            end) as wkt_geometry,
            r.xml as metadata,
            r.xml,
            coalesce(
                replace(
                list_aggregate(
                   r.metadata->'$.identification.keywords.default.keywords[*]',
                   'string_agg',
                   ','
                ), '"', ''),
                ''
            ) as keywords,
            'application/xml' as metadata_type,
            xml_bag(r.xml) as anytext,
            coalesce(r.metadata->>'$.identification.abstract', '') as abstract,
            coalesce(
                strftime(r.last_modified_at, '%Y-%m-%d'),
                '',
            ) as date,
            'Norsk institutt for naturforskning' as creator,
            'Norsk institutt for naturforskning' as publisher,
            coalesce(c.contributors, '') as contributor,
            ([{
                name: r.title,
                url: r.uri,
                description: r.description,
                protocol: r.metadata->>'$.distribution.file.type'
            }]::json)::varchar as links
        from iso_xml as r
        left join contributors_csw as c on c.dataset_id = r.dataset_id
    """)  # noqa: E501

    log.debug(csw)

    csw.write_parquet(
        f"s3://{bucket}{prefix}",
        compression="zstd",
        overwrite=True,
    )


@app.command()
def generate_geoapi_config(
    base_url: str = DMS_DATASETS_BASE,
    access_key=DMS_ACCESS_KEY,
    secret_key=DMS_SECRET_KEY,
    endpoint=DMS_AWS_ENDPOINT,
    bucket=DMS_BUCKET,
    prefix=DMS_GEOAPI_PREFIX,
    lang="en",
):
    # TODO: localize the output based on the language in input

    conn = duckdb.connect()
    conn.sql("Install spatial; load spatial")
    conn.create_function("guess_type", guess_type)

    conn.execute(f"""
        CREATE OR REPLACE SECRET secret (
            TYPE s3,
            REGION 'eu-west-1',
            KEY_ID '{access_key}',
            SECRET '{secret_key}',
            ENDPOINT '{endpoint.replace(r"https://", "")}',
            URL_STYLE 'path'
        );
    """).fetchall()

    datasets = conn.read_parquet(base_url + "datasets_dataset.parquet").filter(
        "json_keys(metadata) <> []"
    )
    log.debug(datasets)
    rasters = conn.read_parquet(base_url + "datasets_rasterresource.parquet").select(
        "* replace (ST_GEomFromHEXWKB(extent) as extent), 'raster' as type"
    )  # noqa: F841
    vectors = conn.read_parquet(base_url + "datasets_tabularresource.parquet").select(
        "* replace (ST_GEomFromHEXWKB(extent) as extent), 'vector' as type"
    )  # noqa: F841
    datatables = conn.read_parquet(base_url + "datasets_datatable.parquet").select(
        "* replace (ST_GEomFromHEXWKB(extent) as extent)"
    )  # noqa: F841
    log.debug(vectors)
    log.debug(datatables)

    descriptions_structure = datasets.aggregate(
        "json_group_structure(metadata->'$.descriptions[*]') as stucture"
    ).fetchone()
    descriptions_structure = (
        descriptions_structure[0] if descriptions_structure else None
    )

    descriptions = (
        (
            rasters.set_alias("r")
            .join(datasets.set_alias("d"), condition="r.dataset_id = d.id")
            .select(
                f"r.id, unnest(json_transform(d.metadata->'$.descriptions[*]', '{descriptions_structure}'), recursive := true)"  # noqa: E501
            )
        )
        .filter(
            duckdb.ColumnExpression("lang") == duckdb.ConstantExpression(lang),
        )
        .aggregate(
            "id, lang, string_agg('# ' || descriptionType || '\n' || description, '\n') as description"  # noqa: E501
        )
    )
    log.debug(descriptions)

    subjects = (
        rasters.set_alias("r")
        .join(datasets.set_alias("d"), condition="r.dataset_id = d.id")
        .select(
            """r.id, unnest(json_transform(d.metadata->'$.subjects[*]', '[{"subject": "VARCHAR", "subjectScheme": "VARCHAR", "schemeURI": "VARCHAR", "valueURI": "VARCHAR", "classificationCode": "VARCHAR"}]'), recursive := true)"""  # noqa: E501
        )
        .aggregate(
            "id, array_agg(subject) as keywords"  # noqa: E501
        )
    )
    log.debug(subjects)

    geo_raster = conn.sql("""
             from rasters as r
             join datasets as d on d.id = r.dataset_id
             left join descriptions as descr on descr.id = d.id
             left join subjects as sbj on sbj.id = d.id
             select
                d.id || '/' || r.id as id,
                'collection' as type,
                'default' as visibility,
                d.title || ' - ' || r.title as title,
                coalesce(sbj.keywords, []) as keywords,
                coalesce(r.description || '\n' || descr.description, '') as description,
                {
                    "spatial": {
                          bbox: [
                            ST_XMIN(r.extent), ST_YMIN(r.extent),
                            ST_XMAX(r.extent), ST_YMAX(r.extent),
                        ],
                        crs: 4326
                    }
                } as extents,
                [{
                    "type": 'coverage',
                    "default": true,
                    "name": 'rasterio',
                    "data": '/vsicurl/' || r.uri,
                    "format": {
                        "name": r.metadata->>'$.driverShortName',
                        "mimetype": guess_type(r.metadata->>'$.driverShortName')
                    }
                }] as providers
            where r.extent is not null and r.metadata is not null
    """)

    log.debug(geo_raster)
    conn.sql(f"""
        COPY geo_raster to 's3://{bucket}{prefix}/dms-raster.json' (FORMAT json, ARRAY true)
    """)  # noqa: E501
    descriptions = (
        (
            vectors.set_alias("r")
            .join(datasets.set_alias("d"), condition="r.dataset_id = d.id")
            .select(
                f"r.id, unnest(json_transform(d.metadata->'$.descriptions[*]', '{descriptions_structure}'), recursive := true)"  # noqa: E501
            )
        )
        .filter(
            duckdb.ColumnExpression("lang") == duckdb.ConstantExpression(lang),
        )
        .aggregate(
            "id, lang, string_agg('# ' || descriptionType || '\n' || description, '\n') as description"  # noqa: E501
        )
    )
    log.debug(descriptions)

    subjects = (
        vectors.set_alias("r")
        .join(datasets.set_alias("d"), condition="r.dataset_id = d.id")
        .select(
            """r.id, unnest(json_transform(d.metadata->'$.subjects[*]', '[{"subject": "VARCHAR", "subjectScheme": "VARCHAR", "schemeURI": "VARCHAR", "valueURI": "VARCHAR", "classificationCode": "VARCHAR"}]'), recursive := true)"""  # noqa: E501
        )
        .aggregate(
            "id, array_agg(subject) as keywords"  # noqa: E501
        )
    )
    log.debug(subjects)

    geo_vector = conn.sql("""
             from vectors as r
             join datasets as d on d.id = r.dataset_id
             join datatables as dt on dt.resource_id = r.id
             left join descriptions as descr on descr.id = d.id
             left join subjects as sbj on sbj.id = d.id
             select
                d.id || '/' || r.id || '/' || dt.name as id,
                'collection' as type,
                'default' as visibility,
                d.title || ' - ' || r.title as title,
                coalesce(sbj.keywords, []) as keywords,
                coalesce(r.description || '\n' || descr.description, '') as description,
                {
                    "spatial": {
                          bbox: [
                            ST_XMIN(dt.extent), ST_YMIN(dt.extent),
                            ST_XMAX(dt.extent), ST_YMAX(dt.extent),
                        ],
                        crs: 4326
                    }
                } as extents,
                [{
                    "type": 'feature',
                    "default": true,
                    "name": 'OGR',
                    "editable": false,
                    "id_field": coalesce(dt.fields->>'$[0].name', 'fid'),
                    "data": {
                        "source": '/vsicurl/' || r.uri,
                        "source_type": r.metadata->>'$.driverShortName',
                    },
                    "layer": dt.name
                }] as providers
            where dt.extent is not null and dt.metadata is not null
    """)
    log.debug(geo_vector)

    conn.sql(f"""
        COPY geo_vector to 's3://{bucket}{prefix}/dms-vector.json' (FORMAT json, ARRAY true)
    """)  # noqa: E501


if __name__ == "__main__":
    app()
