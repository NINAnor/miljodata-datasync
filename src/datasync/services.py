import copy
from collections import OrderedDict

import duckdb
import fsspec
import pyarrow as pa
import s3fs
import typer
import yaml

from .settings import env

app = typer.Typer(help="Miljødata Infrastructure as Code pipelines")


AWS_BUCKET = env("SERVICES_AWS_BUCKET", default=None)
AWS_ENDPOINT = env("SERVICES_AWS_ENDPOINT", default=None)
AWS_ACCESS_KEY = env("SERVICES_AWS_ACCESS_KEY", default=None)
AWS_SECRET_KEY = env("SERVICES_AWS_SECRET_KEY", default=None)

SERVICES_REPO = env("SERVICES_REPO", None)
SERVICES_ORG = env("SERVICES_ORG", default="ninanor")

PARQUET_PREFIX = env("SERVICES_STORAGE_PREFIX", default="/dms/tables")


GIT_USERNAME = env("GIT_USERNAME", default="")
GIT_TOKEN = env("GIT_TOKEN", default="")


@app.command(
    help="Convert metadata.yml definitions to a set of parquet "
    "that can be imported in the DMS"
)
def services_to_parquet(
    org=SERVICES_ORG,
    repo: str | None = SERVICES_REPO,
    bucket: str | None = AWS_BUCKET,
    endpoint: str | None = AWS_ENDPOINT,
    access_key: str | None = AWS_ACCESS_KEY,
    secret_key: str | None = AWS_SECRET_KEY,
    prefix=PARQUET_PREFIX,
    git_username=GIT_USERNAME,
    git_token=GIT_TOKEN,
):
    fs = fsspec.filesystem(
        "github",
        org=org,
        repo=repo,
        username=git_username,
        token=git_token,
    )
    conn = duckdb.connect()

    services_list = []

    services_paths = fs.glob(path="services/*/*/metadata.yml")
    for spath in services_paths:
        with fs.open(spath, "r") as f:
            data = yaml.load(f, Loader=yaml.SafeLoader)
            _, project_name, module_name, _ = spath.split("/")
            services_list.append(
                {
                    **data,
                    "project_name": project_name,
                    "module_name": module_name,
                    "projects": list(map(str, data.get("projects") or [])),
                    "technologies": list(map(str, data.get("technologies") or [])),
                    "related_projects": list(
                        map(str, data.get("related_projects") or [])
                    ),
                }
            )

    df = pa.Table.from_pylist(services_list)

    conn.sql(f"""
    create secret (
        type s3,
        endpoint '{endpoint}',
        KEY_ID '{access_key}',
        SECRET '{secret_key}',
        REGION 'us-east-1',
        URL_STYLE path
    );
    """)

    conn.from_arrow(df).write_parquet(
        f"s3://{bucket}{prefix}/services_raw.parquet", overwrite=True
    )

    # Projects
    conn.from_arrow(df).select("""
    project_name || '__' || module_name as service_id,
    unnest(projects) as project_id
    """).write_parquet(
        f"s3://{bucket}{prefix}/services_projectservice.parquet", overwrite=True
    )

    # Service
    conn.from_arrow(df).select("""
    project_name || '__' || module_name as id,
    title,
    description,
    '{' || array_to_string(list_transform(keywords, x -> '"'||x||'"' ), ',') || '}' as keywords,
    '{' || array_to_string(list_transform(technologies, x -> '"'||x||'"' ), ',') || '}' as technologies,
    """).write_parquet(  # noqa: E501
        f"s3://{bucket}{prefix}/services_service.parquet", overwrite=True
    )

    # Contributors
    conn.sql("""
    from (
        from df
        select
            project_name || '__' || module_name as service_id,
            unnest(contributors) as contributor
        )
    select
    service_id,
    contributor.email as email,
    contributor.role as role,
    """).write_parquet(
        f"s3://{bucket}{prefix}/services_contributor.parquet", overwrite=True
    )

    # Resources
    conn.sql("""
    from (
        from df
        select
            project_name || '__' || module_name as service_id,
            to_json(unnest(resources)) as resource,
            generate_subscripts(resources, 1) AS index
    )
    select
    service_id || '_' || "index" as id,
    service_id,
    nullif(trim(resource->'title', '""'), 'null') as title,
    nullif(trim(resource->'type', '""'), 'null') as type,
    nullif(trim(resource->'uri', '""'), 'null') as uri,
    nullif(trim(resource->'description', '""'), 'null') as description,
    nullif(trim(resource->'access', '""'), 'null') as access,
    nullif(trim(resource->'internal_ref', '""'), 'null') as internal_ref,
    coalesce(try_cast(trim(resource->'external', '""') as bool), false) as external
    """).write_parquet(
        f"s3://{bucket}{prefix}/services_resource.parquet", overwrite=True
    )

    # Related_projects
    conn.from_arrow(df).select("""
    project_name || '__' || module_name as from_service_id,
    unnest(related_projects) as to_service_id
    ;
    """).write_parquet(
        f"s3://{bucket}{prefix}/services_servicerelated.parquet", overwrite=True
    )


DASHBOARD_PREFIX = env("SERVICES_DASHBOARD_PREFIX", default="/dms/services")


DASHBOARD_REPO = env("SERVICES_DASHBOARD_REPO", None)
DASHBOARD_ORG = env("SERVICES_DASHBOARD_ORG", default="ninanor")


@app.command(
    help="Produce a Homer Dashbord using the Miljødata Infrastructure as Code "
    "repository as data source"
)
def dashboard(
    org=SERVICES_ORG,
    repo: str | None = SERVICES_REPO,
    config_org=DASHBOARD_ORG,
    config_repo=DASHBOARD_REPO,
    bucket: str | None = AWS_BUCKET,
    endpoint: str | None = AWS_ENDPOINT,
    access_key: str | None = AWS_ACCESS_KEY,
    secret_key: str | None = AWS_SECRET_KEY,
    git_username=GIT_USERNAME,
    git_token=GIT_TOKEN,
    prefix=DASHBOARD_PREFIX,
):
    s3 = s3fs.S3FileSystem(
        endpoint_url=f"https://{endpoint}",
        secret=secret_key,
        key=access_key,
    )

    infrastucture_repo = fsspec.filesystem(
        "github",
        org=org,
        repo=repo,
        username=git_username,
        token=git_token,
    )

    dashboard_repo = fsspec.filesystem(
        "github",
        org=config_org,
        repo=config_repo,
        username=git_username,
        token=git_token,
    )

    with dashboard_repo.open("base-config.yml", "r") as base_conf_file:
        base_conf = yaml.load(base_conf_file, Loader=yaml.SafeLoader)

    curated = OrderedDict()
    all_services = OrderedDict()

    ICON = {
        "web": "fas fa-globe",
        "code": "fab fa-github",
        "data": "fas fa-database",
        "docs": "fas fa-book",
        "mobileApp": "fas fa-mobile",
        "template": "fas fa-copy",
        "scripts": "fas fa-wrench",
        "other": "",
    }
    TAG = {
        "permit": "is-danger",
        "private": "is-info",
        "public": "is-success",
    }

    services_paths = infrastucture_repo.glob(path="services/*/*/metadata.yml")
    for spath in services_paths:
        with infrastucture_repo.open(spath, "r") as f:
            data = yaml.load(f, Loader=yaml.SafeLoader)
            _, project_name, module_name, _ = spath.split("/")
            for resource in data.get("resources"):
                if not resource["uri"].startswith("http") or resource.get(
                    "external", False
                ):
                    print(resource)
                    # skip non http resources and external resources
                    continue

                if group := resource.get("group"):
                    if group not in curated:
                        curated[group] = {
                            "name": group,
                            "items": [],
                        }

                    curated[group]["items"].append(
                        {
                            "name": resource.get("title", data.get("title", "")),
                            "subtitle": resource.get("description", ""),
                            "url": resource["uri"],
                            "tag": resource["access"],
                            "tagstyle": TAG[resource["access"]],
                            "target": "_blank",
                            "icon": ICON[resource.get("type", "web")],
                        }
                    )

                group_2 = f"{project_name}"
                if group_2 not in all_services:
                    all_services[group_2] = {
                        "name": group_2.capitalize(),
                        "items": [],
                    }

                all_services[group_2]["items"].append(
                    {
                        "name": resource.get("title", data.get("title", "")),
                        "subtitle": resource.get("description", ""),
                        "url": resource["uri"],
                        "tag": resource["access"],
                        "tagstyle": TAG[resource["access"]],
                        "target": "_blank",
                        "icon": ICON[resource.get("type", "web")],
                    }
                )

    curated_conf = copy.deepcopy(base_conf)
    curated_conf["services"] = [group for _, group in curated.items()]
    curated_conf["links"].append({"name": "All services", "url": "#all"})

    with s3.open(f"{bucket}{prefix}/curated.yml", "w") as f:
        yaml.dump(
            curated_conf,
            f,
        )

    all_conf = copy.deepcopy(base_conf)
    all_conf["services"] = [group for _, group in all_services.items()]
    all_conf["links"].append({"name": "Curated services", "url": "/"})

    with s3.open(f"{bucket}{prefix}/all.yml", "w") as f:
        yaml.dump(
            all_conf,
            f,
        )
