"""Load event-based surveillance signals (tasks) from the m-Dharura API into MinIO.

m-Dharura is Kenya's event-based surveillance (EBS) system. The Data Export
endpoints are documented at:
https://api.m-dharura.health.go.ke/swaggerui/#/Data%20Export

The signals resource is windowed on created_at: each Dagster partition run
loads one day (dateStart/dateEnd), so history backfills are ordinary Dagster
backfills over the partition range.
"""

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

client = RESTClient(
    base_url="https://echis.health.go.ke/postgres-api-access"
)

# only these EBS signal codes are loaded; the API has no signal query param,
# so records are filtered as they stream through the generator
#SIGNALS_OF_INTEREST = {"7", "8", "H4"}


def map_task(task: dict) -> dict:
    """Reshape each task record before it is written to the destination."""
    unit = task.get("unit") or {}
    subcounty = unit.get("parent") or {}
    county = subcounty.get("parent") or {}
    form = task.get("cebs") or task.get("hebs") or {}
    verification_form = form.get("verificationForm") or {}
    investigation_form = form.get("investigationForm") or {}
    threat_still_exists = verification_form.get("isThreatStillExisting")

    return {
        "id": task.get("_id"),
        "signal": task.get("signal"),
        "unit_name": unit.get("name"),
        "unit_type": unit.get("type"),
        "subcounty": subcounty.get("name"),
        "county": county.get("name"),
        "signal_verified": bool(verification_form),
        "signal_verified_true": bool(threat_still_exists),
        "signal_verification_date": verification_form.get("createdAt"),
        "signal_investigated": bool(investigation_form),
        "signal_investigation_date": investigation_form.get("createdAt"),
        "created_at": task.get("createdAt"),
    }


@dlt.source(name="echis")
def echis_source():
    @dlt.resource(name="signals", primary_key="id", write_disposition="append")
    def signals(
        created_at=dlt.sources.incremental(
            "created_at", initial_value="2026-06-01T00:00:00.000Z"
        ),
    ):
        params = {}
       
        for page in client.get("echis_signals_evd", params=params):
            yield [
                map_task(task)
                for task in page
            ]

    return signals


source = echis_source()

pipeline = dlt.pipeline(
    pipeline_name="echis",
    destination="filesystem",
    dataset_name="echis_raw",
)
