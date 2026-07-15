"""passive_surveillance_flags: load data from <API> into MinIO.

See docs/developer-walkthrough.md for the step-by-step guide.
"""

import dlt
from dlt.sources.rest_api import rest_api_source

source = rest_api_source(
    {
        "client": {
            "base_url": "https://api.example.org/v1/",
            "paginator": {
                "type": "page_number",
                "base_page": 1,
                "total_path": "pages",
            },
        },
        "resources": [
            {
                "name": "records",
                "primary_key": "id",
                "write_disposition": "append",
                "endpoint": {
                    "path": "records",
                    "params": {"limit": 50},
                    "data_selector": "data",
                },
            },
        ],
    },
    name="passive_surveillance_flags",
    max_table_nesting=0,
)

pipeline = dlt.pipeline(
    pipeline_name="passive_surveillance_flags",
    destination="filesystem",
    dataset_name="passive_surveillance_flags_raw",
)
