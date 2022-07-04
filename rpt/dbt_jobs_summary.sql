select distinct
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
    ,job_warn_count
    ,job_error_count
    ,job_skip_count
    ,job_total_count
    ,run_started_at_timestamp
    ,run_started_at_date_id
    ,run_finished_at_timestamp
    ,run_finished_at_timestamp_id
    ,run_duration_name
    ,run_duration
    ,run_href
from {{ ref('fact_dbt_run') }}
where is_active_flag = 'Y'