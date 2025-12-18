import backoff
import httpx

from ..settings import env
from ..settings import log as logger

DMS_API_URL: str = env.str("DMS_API_URL")
DMS_API_TOKEN: str = env.str("DMS_TOKEN")

client = httpx.Client(
    base_url=DMS_API_URL, headers={"Authorization": f"Token {DMS_API_TOKEN}"}
)


@backoff.on_exception(
    backoff.expo,
    (httpx.HTTPStatusError, httpx.ConnectError),
    max_tries=5,
    jitter=backoff.full_jitter,
)
def upsert_dms_element(endpoint, element_id, create_data, update_data=None):
    """
    Generic function to upsert (create or update) a DMS API element.

    Args:
        endpoint: API endpoint (e.g., 'datasets', 'resources', 'tabularresources')
        element_id: ID of the element
        create_data: Data to use when creating the element
        update_data: Data to use when updating (defaults to create_data if not provided)

    Returns:
        bool: True if element was created, False if updated
    """
    if update_data is None:
        update_data = create_data.copy()

    try:
        # Check if element exists
        res = client.get(f"{endpoint}/{element_id}/")
        res.raise_for_status()
        logger.info(f"{endpoint[:-1]} {element_id} already exists in DMS")

        # Update existing element
        logger.info(f"updating {endpoint[:-1]} {element_id} in DMS")
        res = client.patch(f"{endpoint}/{element_id}/", json=update_data)
        res.raise_for_status()
        return False

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            # Element doesn't exist, create it
            logger.info(f"creating {endpoint[:-1]} {element_id} in DMS")
            res = client.post(f"{endpoint}/", json=create_data)
            res.raise_for_status()
            logger.info("done")
            return True
        else:
            print(e.response.text)
            # Other HTTP error, re-raise
            raise

    except httpx.ConnectError:
        logger.error("DMS not reachable")
        raise
