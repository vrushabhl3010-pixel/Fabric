# Assignment Mapping

- Ingestion: Fabric Spark notebook
- Incremental filter: FHIR `_lastUpdated`
- Pagination: FHIR Bundle `link` with relation `next`
- Raw layer: JSON response stored as-is
- Bronze: Delta tables
- Silver: cleaned and deduplicated tables
- History: SCD Type 2
- Metadata: batch id, timestamps, API information and record count
- Orchestration: Fabric Data Pipeline
- Gold: reporting-ready dimension and fact tables

Resource order:
Patient -> Encounter -> Observation -> Condition
