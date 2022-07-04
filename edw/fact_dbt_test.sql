{{
  config(
    unique_key = 'test_id',
    tags=['dbt_logs']
  )
}}

select 
    test_id,
    run_id, 
    account_id,
    project_id,
    environment_id,
    job_id,
    test_name,
    test_status,
    test_state,
    is_pass,
    is_warning,
    is_error
from {{ ref('stg_dbt_test') }} 
