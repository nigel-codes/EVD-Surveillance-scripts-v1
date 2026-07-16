"""Load passively-flagged surveillance cases from Taifa Care (KenyaEMR) into MinIO.

The DMI (Disease Management Information) system on the KenyaHMIS platform
exposes cases that KenyaEMR has flagged against notifiable/priority conditions,
windowed on created_at (startDate/endDate) so each partition run loads one day.

Direct patient identifiers (NUPI, address, date of birth) are dropped as
records stream through — only a non-identifying subset reaches the raw bucket.
"""

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.auth import OAuth2ClientCredentials
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

BASE_URL = "https://dmistaging.kenyahmis.org/api/"
TOKEN_URL = "https://keycloak.kenyahmis.org/realms/dmi/protocol/openid-connect/token"

# earliest data to load; the partition start_date in defs.yaml must match
INITIAL_VALUE = "2025-01-01T00:00:00.000Z"


def _client() -> RESTClient:
    """Build a RESTClient at run time so `dg check defs` (import only) never
    needs credentials."""
    secrets = dlt.secrets["datasources.taifa_care_kenyaemr"]
    return RESTClient(
        base_url=BASE_URL,
        auth=OAuth2ClientCredentials(
            access_token_url=TOKEN_URL,
            client_id=secrets["client_id"],
            client_secret=secrets["client_secret"],
        ),
        paginator=PageNumberPaginator(base_page=0, total_path="data.totalPages"),
    )


def map_case(c: dict) -> dict:
    """Reshape a case to a flat, non-identifying subset — drops the subject's
    NUPI, address and date of birth."""
    subject = c.get("subject") or {}
    return {
        "id": c.get("caseUniqueId"),
        "hospital_id_number": c.get("hospitalIdNumber"),
        "emr_id": c.get("emrId"),
        "status": c.get("status"),
        "final_outcome": c.get("finalOutcome"),
        "final_outcome_date": c.get("finalOutcomeDate"),
        "interview_date": c.get("interviewDate"),
        "admission_date": c.get("admissionDate"),
        "outpatient_date": c.get("outpatientDate"),
        # pseudonymous linkage key for a patient's multiple cases
        "patient_unique_id": subject.get("patientUniqueId"),
        "sex": subject.get("sex"),
        "county": subject.get("county"),
        "sub_county": subject.get("subCounty"),
        "diagnosis": c.get("diagnosis"),
        "flagged_conditions": c.get("flaggedConditions"),
        "vital_signs": c.get("vitalSigns"),
        "risk_factors": c.get("riskFactors"),
        "vaccinations": c.get("vaccinations"),
        "complaints": c.get("complaints"),
        "lab": c.get("lab"),
        "art_linkages": c.get("artLinkages"),
        "created_at": c.get("createdAt"),
        "updated_at": c.get("updatedAt"),
    }


@dlt.source(name="taifa_care_kenyaemr", max_table_nesting=0)
def taifa_care_kenyaemr_source():
    @dlt.resource(name="flagged_cases", primary_key="id", write_disposition="append")
    def flagged_cases(
        created_at=dlt.sources.incremental("created_at", initial_value=INITIAL_VALUE),
    ):
        client = _client()
        params = {"size": 100, "startDate": created_at.last_value[:10]}
        if created_at.end_value:
            params["endDate"] = created_at.end_value[:10]
        for page in client.paginate("case", params=params, data_selector="data.content"):
            yield [map_case(c) for c in page]

    return flagged_cases


source = taifa_care_kenyaemr_source()

pipeline = dlt.pipeline(
    pipeline_name="taifa_care_kenyaemr",
    destination="filesystem",
    dataset_name="taifa_care_kenyaemr_raw",
    progress=dlt.progress.tqdm(colour="yellow"),
)
