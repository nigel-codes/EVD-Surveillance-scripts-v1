"""Load event-based surveillance signals (tasks) from the m-Dharura API into MinIO.

m-Dharura is Kenya's event-based surveillance (EBS) system. The Data Export
endpoints are documented at:
https://api.m-dharura.health.go.ke/swaggerui/#/Data%20Export

Destination config (bucket URL, MinIO credentials + endpoint) lives in
.dlt/config.toml and .dlt/secrets.toml — or the equivalent env vars in
deployment:
    DESTINATION__FILESYSTEM__BUCKET_URL
    DESTINATION__FILESYSTEM__CREDENTIALS__AWS_ACCESS_KEY_ID
    DESTINATION__FILESYSTEM__CREDENTIALS__AWS_SECRET_ACCESS_KEY
    DESTINATION__FILESYSTEM__CREDENTIALS__ENDPOINT_URL
"""

import dlt
from dlt.sources.rest_api import rest_api_source


def map_task(task: dict) -> dict:
    """Reshape each task record before it is written to the destination."""
    unit = task.get("unit") or {}
    subcounty = unit.get("parent") or {}
    county = subcounty.get("parent") or {}

    return {
        "id": task.get("_id"),
        "signal": task.get("signal"),
        "community_unit": unit.get("name"),
        "subcounty": subcounty.get("name"),
        "county": county.get("name"),
        "created_at": task.get("createdAt"),
    }


source = rest_api_source(
    {
        "client": {
            "base_url": "https://api.m-dharura.health.go.ke/v1/",
            "paginator": {
                "type": "page_number",
                "base_page": 1,
                "total_path": "pages",
            },
        },
        "resources": [
            {
                "name": "signals",
                "primary_key": "id",
                "processing_steps": [
                    {"map": map_task},
                ],
                "write_disposition": "append",
                "endpoint": {
                    "path": "export/tasks",
                    "params": {"limit": 50, "state": "live"},
                    "data_selector": "data",
                    "incremental": {
                        "start_param": "dateStart",
                        "cursor_path": "created_at",
                        "initial_value": "2026-07-06T00:00:00.000Z",
                    },
                },
            },
        ],
    },
    name="mdharura",
    max_table_nesting=0,
)

pipeline = dlt.pipeline(
    pipeline_name="mdharura",
    destination="filesystem",
    dataset_name="mdharura_raw",
)
