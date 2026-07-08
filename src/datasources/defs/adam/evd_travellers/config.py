API_URL = "https://api.adam.health.go.ke/api/records/composite"
SOURCE_NAME = "adam"
RESOURCE_NAME = "travellers"
PRIMARY_KEY = "id"
WRITE_DISPOSITION = "merge"
INITIAL_TIMESTAMP = "2026-05-01T00:00:00.000Z"
PAGE_SIZE = 250

TOOL_ID = "59635360-67c3-11ef-8f3c-c9e80a669bbc"

PROJECTION = {
  "id": "id",
  "name": "name_of_traveler",
  "sex": "sex",
  "date_of_birth": "date_of_birth",
  "nationality": "country_of_nationality",
  "identifier": "id_number",
  "classification": "traveller_symptoms_ebola_classification_of_traveller",
  "screened": "Yes",
  "point_of_entry": "point_of_entry",
  "created_timestamp": "created_timestamp"
}