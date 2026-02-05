import duckdb
import orjson
import s3fs
import typer
from pygeometa.schemas.iso19139 import ISO19139OutputSchema

from .libs.csw import publish_csw_record
from .libs.geoapi import publish_pygeoapi_resource
from .libs.helpers import ClientError
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

DMS_OGC_RECORDS_PUBLISH_URL: str = env.str("DMS_OGC_RECORDS_PUBLISH_URL", default=None)
DMS_GEOAPI_PUBLISH_URL: str = env.str("DMS_GEOAPI_PUBLISH_URL", default=None)


app = typer.Typer()

iso = ISO19139OutputSchema()


def guess_type(driver: str) -> str:
    # TODO: improve mapping based on the driver
    return "image/tiff"


@app.command()
def generate_csw_metadata(
    base_url: str = DMS_DATASETS_BASE,
    access_key=DMS_ACCESS_KEY,
    secret_key=DMS_SECRET_KEY,
    endpoint=DMS_AWS_ENDPOINT,
    bucket=DMS_BUCKET,
    publish_url=DMS_OGC_RECORDS_PUBLISH_URL,
    limit: int | None = typer.Option(
        default=None,
    ),
    search: str | None = typer.Option(
        default=None,
        help="Filter resources by title using a LIKE expression",
    ),
):
    s3 = s3fs.S3FileSystem(
        endpoint_url=endpoint,
        key=access_key,
        secret=secret_key,
    )

    conn = duckdb.connect()
    conn.install_extension("spatial")
    conn.install_extension("httpfs")
    conn.load_extension("spatial")
    conn.load_extension("httpfs")

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
    log.debug(
        "pre_filter", resources=resources.select("title"), columns=resources.columns
    )

    if search:
        resources = resources.filter(f"title LIKE '%{search}%'")

    log.debug("found", resources=resources)

    # --- General metadata --- #

    metadata = (
        resources.set_alias("r")
        .join(datasets.set_alias("d"), condition="r.dataset_id = d.id")
        .select("""
        r.id,
        r.dataset_id,
        {
        identifier: r.id,
        language: 'en',
        language_alternate: 'no',
        charset: 'utf8',
        hierarchylevel: case
            when r.extent is not null then 'dataset' else 'nonGeographicDataset'
        end,
        datestamp: r.last_modified_at,
        dataseturi: r.uri,
        }::json as metadata
                    """)
    )

    log.debug(metadata)

    # --- Spatial --- #

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

    # --- Descriptions --- #

    descriptions_structure = datasets.aggregate(
        "json_group_structure(metadata->'$.descriptions[*]') as stucture"
    ).fetchone()
    descriptions_structure = (
        descriptions_structure[0] if descriptions_structure else None
    )

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
        ),
    ).aggregate("id, lang, string_agg(description, '\n') as description")
    log.debug(abstracts)

    # --- Licenses --- #

    licenses = (
        resources.set_alias("r")
        .join(datasets.set_alias("d"), condition="r.dataset_id = d.id")
        .select(
            """r.id, unnest(json_transform(d.metadata->'$.rightsList[*]', '[{"lang": "VARCHAR", "rights": "VARCHAR", "rightsURI": "VARCHAR", "rightsIdentifier": "VARCHAR", "schemeUri": "VARCHAR"}]'), recursive := true)"""  # noqa: E501
        )
        .aggregate("id, lang, first(rights) as rights, first(rightsURI) as url")
    )
    log.debug("licenses", data=licenses)

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
    log.debug("subjects", data=subjects)

    # --- Identification --- #

    identification = conn.sql("""
        from resources as r
        join datasets as d on r.dataset_id = d.id
        left join abstracts as a on a.id = r.id
        left join licenses as l on l.id = r.id
        left join subjects as s on s.id = r.id
        select
            r.id,
            {
                language: d.metadata->'$.language',
                title: d.title || ' - ' || r.title,
                fee: 'None',
                status: 'completed',
                rights: l.rights,
                abstract: a.description || '\n' || r.description,
                edition: d.version,
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
                --license: {
                --    "name": coalesce(l.rights, 'All rights reserved'),
                    --"uri": coalesce(l.url, ''),
                --},
                keywords: coalesce(s.keywords, {
                    "default": {
                        -- need to join with dataset to read those!
                        keywords_type: 'theme',
                        keywords: [],
                    }
                }),
            }::json as identification
    """)  # noqa: E501

    log.debug("identification", data=identification)

    # --- Content Info --- #

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

    log.debug("content_info", data=content_info)

    # --- Contacts --- #

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

    log.debug("contacts", data=contacts)

    # --- Distribution --- #
    distribution = resources.select("""
        id,
        {
            file: {
                url: uri,
                type: case when type = 'raster' then 'FILE:GEO'
                        when type = 'vector' then 'FILE:GEO'
                        else 'WWW:FILE'
                end,
                name: title,
                description: description,
                function: 'download'
            }
        }::json as distribution
        """)

    log.debug("distribution", data=distribution)

    res = conn.sql(
        """
            select
            r.id,
            {
                mcf: { version: 1.0 },
                metadata: m.metadata,
                identification: i.identification,
                content_info: ci.content_info,
                spatial: sp.spatial,
                distribution: d.distribution,
                contact: c.contact
            }::json as metadata,
            '/data/project/' || ds.project_id || '/datasets/' || ds.id || '/' || r.id || '.xml' as path,
            from resources as r
            join datasets as ds on r.dataset_id = ds.id
            join metadata as m on r.id = m.id
            join identification as i on m.id = i.id
            join content_info as ci on ci.id = m.id
            join spatial as sp on sp.id = m.id
            join distribution as d on d.id = m.id
            join contacts as c on m.dataset_id = c.dataset_id
        """  # noqa: E501
    )
    log.debug(res)

    i = 0
    for item in res.fetchall():
        mcf = orjson.loads(item[1])

        if "pointOfContact" in mcf["contact"]:
            mcf["contact"]["pointOfContact"] = mcf["contact"]["pointOfContact"] | {
                "organization": "Norsk institutt for naturforskning",
                "url": "https://www.nina.no",
                "email": "firmapost@nina.no",
            }
        else:
            mcf["contact"]["pointOfContact"] = {
                "organization": "Norsk institutt for naturforskning",
                "url": "https://www.nina.no",
                "email": "firmapost@nina.no",
                "individualname": "",
                "phone": "",
                "positionname": "",
                "fax": "",
                "address": "",
                "postalcode": "",
                "country": "Norway",
                "city": "Trondheim",
            }

        metadata = iso.write(mcf)
        log.debug("metadata", metadata=metadata, original=mcf)
        try:
            log.info("Publishing CSW record", id=item[0])
            publish_csw_record(publish_url, metadata, item[0])
        except ClientError as e:
            log.error(e)

        with s3.open(f"{bucket}{item[2]}", mode="w") as f:
            f.write(metadata)

        i += 1
        if limit and limit < i:
            break


@app.command()
def generate_geoapi_config(
    base_url: str = DMS_DATASETS_BASE,
    publish_url=DMS_GEOAPI_PUBLISH_URL,
    search: str | None = typer.Option(
        default=None,
        help="Filter resources by title using a LIKE expression",
    ),
):
    conn = duckdb.connect()
    conn.install_extension("spatial")
    conn.load_extension("spatial")
    conn.create_function("guess_type", guess_type)

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

    # Apply search filter early
    if search:
        rasters = (
            rasters.join(datasets.set_alias("d"), condition="dataset_id = d.id")
            .filter(f"d.title LIKE '%{search}%' or title LIKE '%{search}%'")
            .select("rasters.*")
        )
        vectors = (
            vectors.join(datasets.set_alias("d"), condition="dataset_id = d.id")
            .filter(f"d.title LIKE '%{search}%' or title LIKE '%{search}%'")
            .select("vectors.*")
        )
        # Also filter datatables based on filtered vectors
        datatables = datatables.join(
            vectors.set_alias("v"), condition="resource_id = v.id"
        ).select("datatables.*")

    log.debug(vectors)
    log.debug(datatables)

    descriptions_structure = datasets.aggregate(
        "json_group_structure(metadata->'$.descriptions[*]') as stucture"
    ).fetchone()
    descriptions_structure = (
        descriptions_structure[0] if descriptions_structure else None
    )

    descriptions = (
        rasters.set_alias("r")
        .join(datasets.set_alias("d"), condition="r.dataset_id = d.id")
        .select(
            f"r.id, unnest(json_transform(d.metadata->'$.descriptions[*]', '{descriptions_structure}'), recursive := true)"  # noqa: E501
        )
    ).aggregate(
        "id, lang, string_agg('# ' || descriptionType || '\n' || description, '\n') as description"  # noqa: E501
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
                r.id as id,
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
                        crs: '4326'
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

    for e in geo_raster.to_arrow_table().to_pylist():
        publish_pygeoapi_resource(publish_url, e)

    descriptions = (
        vectors.set_alias("r")
        .join(datasets.set_alias("d"), condition="r.dataset_id = d.id")
        .select(
            f"r.id, unnest(json_transform(d.metadata->'$.descriptions[*]', '{descriptions_structure}'), recursive := true)"  # noqa: E501
        )
    ).aggregate(
        "id, lang, string_agg('# ' || descriptionType || '\n' || description, '\n') as description"  # noqa: E501
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
                r.id || '__' || dt.name as id,
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
                        crs: '4326'
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

    for e in geo_vector.to_arrow_table().to_pylist():
        publish_pygeoapi_resource(publish_url, e)


if __name__ == "__main__":
    app()
