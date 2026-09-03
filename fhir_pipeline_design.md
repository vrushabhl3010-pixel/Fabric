# Fabric Data Pipeline

Pipeline name: `pl_fhir_incremental_ingestion`

Notebook sequence:

1. `NB_01_FHIR_Ingestion`
2. `NB_02_Raw_To_Bronze`
3. `NB_03_Bronze_To_Silver`
4. `NB_04_SCD_Type2`
5. `NB_05_Gold_Model`

Each notebook runs after the previous notebook succeeds.

Parameters:
- `start_date`
- `end_date`
- `page_size`
- `api_base_url`

The ingestion process handles resources in this order:

Patient -> Encounter -> Observation -> Condition

Spark is used for the JSON-to-table transformations.
