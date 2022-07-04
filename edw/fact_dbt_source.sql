{{
  config(
    unique_key = 'source_id',
    tags=['dbt_logs']
  )
}}

select 
    file_name,
    line,
    modified_timestamp,
    source_id,
    run_id,
    job_id,
    environment_id,
    project_id,
    account_id, 
    table_name,
    source_name,
    source_description,
    source_state,
    source_state_code,
    max_loaded_at_timestamp,
    snapshotted_at_timestamp,
    run_generated_at_timestamp,
    run_elapsed_time,
    criteria,
    tests,
    fivetran_synced_timestamp
from {{ ref('stg_dbt_source') }} 
