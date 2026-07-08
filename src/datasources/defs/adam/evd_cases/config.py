API_URL = "https://api.adam.health.go.ke/api/records/composite"

SOURCE_NAME = "adam"
RESOURCE_NAME = "cases"
PRIMARY_KEY = "id"
WRITE_DISPOSITION = "merge"
INITIAL_TIMESTAMP = "2026-05-01T00:00:00.000Z"
PAGE_SIZE = 250
TOOL_ID = "a78b43f0-e4f0-11ee-a969-7765f1f98ba9"

PROJECTION = {
  "id": "id",
  "name": [
    "case_demographics_family",
    " ",
    "case_demographics_given"
  ],
  "sex": "case_demographics_sex",
  "date_of_birth": "case_demographics_date_of_birth",
  "nationality": "case_demographics_country_of_nationality",
  "identifier": "national_id",
  "type": "type_of_record",
  "initial_classification": "type_of_record",
  "outcome": "clinical_care_outcome_of_case",
  "samples_collected": "laboratory_sample_collected",
  "final_laboratory_results": "laboratory_final_laboratory_result",
  "reporting_county": "reporting_county",
  "reporting_subcounty": "reporting_subcounty",
  "health_facility": "case_demographics_health_facility",
  "date_of_investigation": "date_of_investigation"
}