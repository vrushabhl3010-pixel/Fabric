# Fabric Spark Notebook
from datetime import datetime, timedelta, timezone
import json
import time
import uuid
import requests

start_date = "2026-08-30"
end_date = "2026-09-02"       # end date is not included
page_size = 100
api_base_url = "https://hapi.fhir.org/baseR4"

raw_root = "Files/raw/fhir"
resources = ["Patient", "Encounter", "Observation", "Condition"]

spark.sql("""
CREATE TABLE IF NOT EXISTS api_ingestion_log (
    batch_id STRING,
    resource_name STRING,
    page_number INT,
    extraction_timestamp TIMESTAMP,
    data_saved_timestamp TIMESTAMP,
    api_url_or_params STRING,
    raw_path STRING,
    record_count INT,
    status STRING,
    error_message STRING
) USING DELTA
""")

def save_raw(path, payload):
    notebookutils.fs.put(
        path,
        json.dumps(payload, ensure_ascii=False),
        True
    )

def get_next_url(bundle):
    for link in bundle.get("link", []):
        if link.get("relation") == "next":
            return link.get("url")
    return None

day = datetime.strptime(start_date, "%Y-%m-%d").date()
last_day = datetime.strptime(end_date, "%Y-%m-%d").date()

for resource in resources:
    current_day = day

    while current_day < last_day:
        batch_id = str(uuid.uuid4())
        page_number = 1
        url = f"{api_base_url}/{resource}"

        params = {
            "_lastUpdated": [
                f"ge{current_day}T00:00:00Z",
                f"lt{current_day + timedelta(days=1)}T00:00:00Z"
            ],
            "_count": page_size
        }

        while url:
            extraction_time = datetime.now(timezone.utc)

            try:
                response = requests.get(
                    url,
                    params=params if page_number == 1 else None,
                    timeout=120
                )
                response.raise_for_status()
                bundle = response.json()

                raw_path = (
                    f"{raw_root}/{resource}/"
                    f"extraction_date={current_day}/"
                    f"batch_id={batch_id}/"
                    f"page_{page_number:05d}.json"
                )

                save_raw(raw_path, bundle)

                record_count = len(bundle.get("entry", []))

                log_row = [(
                    batch_id,
                    resource,
                    page_number,
                    extraction_time,
                    datetime.now(timezone.utc),
                    response.url,
                    raw_path,
                    record_count,
                    "SUCCESS",
                    None
                )]

                log_schema = """
                    batch_id string,
                    resource_name string,
                    page_number int,
                    extraction_timestamp timestamp,
                    data_saved_timestamp timestamp,
                    api_url_or_params string,
                    raw_path string,
                    record_count int,
                    status string,
                    error_message string
                """

                spark.createDataFrame(log_row, log_schema)                     .write.mode("append")                     .saveAsTable("api_ingestion_log")

                url = get_next_url(bundle)
                page_number += 1
                time.sleep(0.1)

            except Exception as exc:
                raise RuntimeError(
                    f"FHIR ingestion failed for {resource} "
                    f"on {current_day}: {exc}"
                )

        current_day += timedelta(days=1)

print("FHIR ingestion completed")
