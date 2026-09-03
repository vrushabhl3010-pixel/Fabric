# Architecture

FHIR R4 API
  -> Fabric Spark Notebook
  -> Raw JSON files
  -> Bronze Delta
  -> Silver Delta
  -> SCD Type 2
  -> Gold tables
  -> Power BI

The raw response is saved without changing the API payload.

Example raw path:

Files/raw/fhir/<Resource>/extraction_date=YYYY-MM-DD/batch_id=<id>/page_00001.json

Bronze keeps the source JSON along with basic ingestion information.

Silver extracts the fields needed for reporting, removes duplicate versions of the same resource and standardizes the structure.

SCD Type 2 keeps the previous version when tracked attributes change.

Gold contains the dimension and fact tables used by the reporting layer.
