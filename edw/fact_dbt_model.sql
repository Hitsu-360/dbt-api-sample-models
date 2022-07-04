{{
  config(
    unique_key = 'model_id',
    tags=['dbt_logs']
  )
}}

select 
    file_name,
    line,
    modified_timestamp,
    model_id,
    run_id,
    job_id,
    environment_id,
    project_id,
    account_id,
    model_name,
    error,
    schema,
    status,
    status_code, 
    skip,
    execution_time,
    tests,
    fivetran_synced_timestamp,
    execute_started_at_timestamp,
    execute_completed_at_timestamp
from {{ ref('stg_dbt_model') }}
