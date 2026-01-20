from ..libs.dms import upsert_dms_element
from .settings import DMS_PROJECT_ID


def create_dms_dataset(ds, parquet_url, xml_url):
    ds_id = "IPT__" + ds["id"]

    # Upsert dataset
    dataset_data = {
        "id": ds_id,
        "title": ds["title"],
        "metadata": {},
        "version": "latest",
        "project_id": DMS_PROJECT_ID,
    }
    dataset_update_data = {
        "title": ds["title"],
        "version": "latest",
    }

    upsert_dms_element("datasets", ds_id, dataset_data, dataset_update_data)

    if parquet_url:
        # Upsert tabular resource
        tabular_resource_data = {
            "id": ds_id + "_parquet",
            "dataset_id": ds_id,
            "title": ds["title"] + " Parquet",
            "uri": parquet_url,
            "access_type": "public",
            "role": "data",
            "metadata_url": xml_url,
        }
        tabular_resource_update_data = {
            "title": ds["title"] + " Parquet",
            "uri": parquet_url,
            "metadata_url": xml_url,
        }

        upsert_dms_element(
            "tabularresources",
            ds_id + "_parquet",
            tabular_resource_data,
            tabular_resource_update_data,
        )

        # Upsert resource
        resource_data = {
            "id": ds_id + "_dwca",
            "dataset_id": ds_id,
            "title": ds["title"] + " DWCA",
            "uri": ds["ipt_dwca"],
            "access_type": "public",
            "role": "data",
            "metadata_url": xml_url,
        }
        resource_update_data = {
            "title": ds["title"] + " DWCA",
            "uri": ds["ipt_dwca"],
            "metadata_url": xml_url,
        }

        upsert_dms_element(
            "resources", ds_id + "_dwca", resource_data, resource_update_data
        )
