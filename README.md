# FHIR API Ingestion and Analytics - Microsoft Fabric

This assignment loads FHIR R4 data into a Fabric Lakehouse and prepares it for reporting.

Flow:

FHIR API -> Raw JSON -> Bronze -> Silver -> SCD Type 2 -> Gold -> Power BI

FHIR resources used:
- Patient
- Encounter
- Observation
- Condition

Notebook order:
1. 01_fhir_incremental_ingestion
2. 02_raw_to_bronze
3. 03_bronze_to_silver
4. 04_scd_type2
5. 05_gold_model

The ingestion notebook uses a date range and FHIR pagination. Raw API responses are kept as JSON before the data is converted to Delta tables.

Parameters used by the ingestion process:
- start_date
- end_date
- page_size
- api_base_url

Default API:
https://hapi.fhir.org/baseR4

Before running the notebooks, attach a Fabric Lakehouse.
