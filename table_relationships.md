# Table Relationships

`gold_dim_patient.patient_id`

  -> `gold_fact_encounter.patient_id`
  -> `gold_fact_observation.patient_id`
  -> `gold_fact_condition.patient_id`

Patient is the main dimension. Encounter, Observation and Condition are the fact tables.
