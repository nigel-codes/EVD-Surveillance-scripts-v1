"""Load point-of-entry health screenings from KRCS Data Capture Forms into MinIO.

Screenings are recorded by border officers at airports, seaports and land
crossings, then moved through a surveillance workflow: Screened -> Suspected ->
Referred -> Confirmed / Cleared. Records arrive flat and FHIR-aligned, and are
pseudonymised at the source -- no name, passport number or date of birth, just a
salted `subjectPseudoId` that is stable per traveler across screenings.

The screenings resource is windowed on `modified`, not on creation time: a
screening's workflow state changes *after* it is first recorded, so a created-at
cursor would freeze every traveler in their initial `Screened` state. This is the
staleness caveat in docs/resources.md, avoided rather than inherited.

The cursor holds only the *latest* modification, so a screening lands in whichever
partition its `modified` fell in at the moment that partition ran. Every screening
is captured at least once, and one can appear in several partitions --
`write_disposition` is `append`, so deduplicate downstream on `screeningIdentifier`
keeping the greatest `modified`. Re-running an old partition will not reproduce it:
a screening modified since no longer matches that window.

The API speaks camelCase; dlt snake_cases the top-level columns on the way into the
bucket (`screeningIdentifier` -> `screening_identifier`). `symptoms` and
`countriesVisited` are kept as opaque JSON values by `max_table_nesting=0`, so the
keys inside them are not normalised and stay camelCase.

Source endpoint:
https://<site>/api/method/data_capture_forms.evd_screening.lake.screenings
"""

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

# Frappe wraps every whitelisted method's return value in a `message` envelope,
# so both the page count and the records sit one level down.
ENDPOINT = "data_capture_forms.evd_screening.lake.screenings"
PAGE_SIZE = 200


def _client() -> RESTClient:
    """Built lazily, at extract time.

    `dg check defs` imports this module in CI, where no secrets are configured;
    reading them at module scope would turn a missing token into a failed
    definition load rather than a failed run.
    """
    base_url = dlt.secrets["datasources.evd_screening.base_url"]
    key = dlt.secrets["datasources.evd_screening.api_key"]
    secret = dlt.secrets["datasources.evd_screening.api_secret"]

    # An *absent* key raises ConfigFieldMissingException above. An unset
    # compose variable is different: `${EVD_SCREENING_API_KEY:-}` resolves to
    # the empty string, which dlt reads as present. Without this, the run
    # would sail on and fail as an opaque 403 from Frappe.
    missing = [
        n for n, v in (("base_url", base_url), ("api_key", key), ("api_secret", secret)) if not v
    ]
    if missing:
        raise ValueError(
            f"datasources.evd_screening: empty or unset: {', '.join(missing)}. Set these in "
            ".dlt/secrets.toml, or as DATASOURCES__EVD_SCREENING__* env vars "
            "(docker-compose forwards them from .env)."
        )

    return RESTClient(
        base_url=base_url,
        headers={"Authorization": f"token {key}:{secret}"},
        paginator=PageNumberPaginator(base_page=1, total_path="message.pages"),
    )


@dlt.source(name="evd_screening", max_table_nesting=0)
def evd_screening_source():
    @dlt.resource(
        name="screenings",
        primary_key="screeningIdentifier",
        write_disposition="append",
    )
    def screenings(
        modified=dlt.sources.incremental(
            "modified", initial_value="2026-06-01T00:00:00.000Z"
        ),
    ):
        # initial_value/end_value are overwritten by the component with the run's
        # partition window; the default above only applies to an unpartitioned run.
        params = {
            "limit": PAGE_SIZE,
            "dateStart": modified.last_value,
        }
        if modified.end_value:
            params["dateEnd"] = modified.end_value
        yield from _client().paginate(
            ENDPOINT, params=params, data_selector="message.data"
        )

    return screenings


source = evd_screening_source()

pipeline = dlt.pipeline(
    pipeline_name="evd_screening",
    destination="filesystem",
    dataset_name="evd_screening_raw",
)
