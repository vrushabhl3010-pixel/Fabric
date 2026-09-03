# Fabric Spark Notebook
from pyspark.sql import functions as F
from delta.tables import DeltaTable

def apply_scd2(source_table, target_table, tracked_columns):
    source = spark.table(source_table)

    hash_expr = F.sha2(
        F.concat_ws(
            "||",
            *[
                F.coalesce(F.col(column).cast("string"), F.lit(""))
                for column in tracked_columns
            ]
        ),
        256
    )

    source = source.withColumn("record_hash", hash_expr)

    if not spark.catalog.tableExists(target_table):
        (
            source
            .withColumn("effective_from", F.current_timestamp())
            .withColumn(
                "effective_to",
                F.lit(None).cast("timestamp")
            )
            .withColumn("is_current", F.lit(True))
            .write
            .format("delta")
            .saveAsTable(target_table)
        )
        return

    target = DeltaTable.forName(spark, target_table)

    current = (
        spark.table(target_table)
        .filter("is_current = true")
        .select("resource_id", "record_hash")
    )

    changed = (
        source.alias("s")
        .join(current.alias("t"), "resource_id", "left")
        .filter(
            "t.resource_id IS NULL OR s.record_hash <> t.record_hash"
        )
        .select("s.*")
    )

    (
        target.alias("t")
        .merge(
            changed.alias("s"),
            "t.resource_id = s.resource_id AND t.is_current = true"
        )
        .whenMatchedUpdate(
            set={
                "effective_to": "current_timestamp()",
                "is_current": "false"
            }
        )
        .execute()
    )

    (
        changed
        .withColumn("effective_from", F.current_timestamp())
        .withColumn(
            "effective_to",
            F.lit(None).cast("timestamp")
        )
        .withColumn("is_current", F.lit(True))
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(target_table)
    )

apply_scd2(
    "silver_patient",
    "scd2_patient",
    ["active", "gender", "birth_date", "family_name", "given_name"]
)

apply_scd2(
    "silver_encounter",
    "scd2_encounter",
    ["status", "class_code", "subject_reference",
     "period_start", "period_end"]
)

apply_scd2(
    "silver_observation",
    "scd2_observation",
    ["status", "code_text", "subject_reference",
     "effective_datetime", "value_quantity", "value_unit"]
)

apply_scd2(
    "silver_condition",
    "scd2_condition",
    ["clinical_status", "verification_status", "code_text",
     "subject_reference", "onset_datetime"]
)

print("SCD Type 2 completed")
