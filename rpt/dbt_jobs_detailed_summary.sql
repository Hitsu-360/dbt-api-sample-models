select 
    job_id
    ,job_name
    ,run_id
    ,run_status
    ,run_custom_status
    ,run_custom_status_code
    ,is_complete
    ,is_success
    ,is_error
    ,is_cancelled
    ,is_in_progress
    ,run_started_at_timestamp
    ,run_started_at_date_id
    ,run_finished_at_timestamp
    ,run_finished_at_timestamp_id
    ,run_duration_name
    ,run_duration
    ,run_step_id
    ,run_step_name
    ,run_step_index
    ,run_step_status_humanized
    ,run_step_started_at_timestamp
    ,run_step_started_at_date_id
    ,run_step_finished_at_timestamp
    ,run_step_finished_at_date_id
    ,run_step_duration_name
    ,run_step_duration
    ,run_step_logs
    ,run_step_totals_message
    ,run_step_pass_count
    ,run_step_warn_count
    ,run_step_error_count
    ,run_step_skip_count
    ,run_step_total_count
    ,dbt_test_failure_message
    ,dbt_error_message
    ,run_href
from {{ ref('fact_dbt_run') }} 
where is_active_flag = 'Y'
