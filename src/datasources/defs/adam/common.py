import dlt
import requests

def build_request(
    tool_id: str,
    page_size: int,
    projection: dict,
    timestamp_start: str,
    timestamp_end: str | None,
) -> dict:

    return {
        "tool_id": tool_id,
        "timestamp_start": timestamp_start,
        "timestamp_end": timestamp_end,
        "complete": True,
        "limit": page_size,
        "format": "tabular",
        "projection": projection,
    }


def poll(api_url: str, page: int, body: dict) -> list[dict]:

    request = body.copy()
    request["page"] = page

    response = requests.post(
        api_url,
        json=request,
        timeout=60,
    )

    response.raise_for_status()

    return response.json()["payload"]["rows"]

def create_source(config):
    @dlt.source(name=config.SOURCE_NAME)
    def source():

        @dlt.resource(
            name=config.RESOURCE_NAME,
            primary_key=config.PRIMARY_KEY,
            write_disposition=config.WRITE_DISPOSITION,
        )
        def resource(
            created_timestamp=dlt.sources.incremental(
                "created_timestamp",
                initial_value=config.INITIAL_TIMESTAMP,
            ),
        ):

            body = build_request(
                config.API_URL,
                config.PAGE_SIZE,
                config.PROJECTION,
                created_timestamp.last_value,
                created_timestamp.end_value,
            )

            page = 0

            while True:

                rows = poll(
                    config.API_URL,
                    page,
                    body,
                )

                if not rows:
                    break

                yield rows

                if len(rows) < config.PAGE_SIZE:
                    break

                page += 1

        return resource

    return source