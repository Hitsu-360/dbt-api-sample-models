with runs_max_finished_at_timestamp_by_job as (

    select 
        job_definition_id as job_id,
        max(to_timestamp(run.finished_at)) as max_finished_at_timestamp
    from {{ source('dbt_api', 'run') }} 
    where is_complete and environment_id = '0000' --production environment
    group by job_definition_id 

), runs as (

    select
        *,
        case
            when latest.max_finished_at_timestamp is not null and to_timestamp(run.finished_at) = latest.max_finished_at_timestamp then 'Y'
            else 'N'
        end as is_active_flag
    from {{ source('dbt_api', 'run') }} run
    left join runs_max_finished_at_timestamp_by_job latest on 
        run.job_id = latest.job_id and 
        to_timestamp(run.finished_at) = latest.max_finished_at_timestamp

), runs_with_steps as (

    select
        -- RUN
        run.id as run_id
        ,run.job_definition_id as job_id
        ,run.status_humanized as run_status
        ,run.is_complete
        ,run.is_success
        ,run.is_error
        ,run.is_cancelled
        ,run.in_progress as is_in_progress
        ,run.href as run_href
        ,run.is_active_flag
        -- RUN STEP 
        ,to_timestamp(run.started_at) as run_started_at_timestamp
        ,to_number(to_char(run_started_at_timestamp,'yyyymmdd')) as run_started_at_date_id
        ,to_timestamp(run.finished_at) as run_finished_at_timestamp
        ,to_number(to_char(run_finished_at_timestamp,'yyyymmdd')) as run_finished_at_timestamp_id
        ,run.duration_humanized as run_duration_name
        ,run.duration as run_duration
        ,run_step.value:id::number as run_step_id
        ,run_step.value:name::string as run_step_name
        ,run_step.value:index::string as run_step_index
        ,run_step.value:status_humanized::string as run_step_status_humanized
        ,to_timestamp(run_step.value:started_at)  as run_step_started_at_timestamp
        ,to_number(to_char(run_step_started_at_timestamp,'yyyymmdd')) as run_step_started_at_date_id
        ,to_timestamp(run_step.value:finished_at) as run_step_finished_at_timestamp
        ,to_number(to_char(run_step_finished_at_timestamp,'yyyymmdd')) as run_step_finished_at_date_id
        ,run_step.value:duration_humanized::string as run_step_duration_name
        ,run_step.value:duration::string as run_step_duration
        ,run_step.value:logs::string as run_step_logs
        ,substr(run_step_logs,regexp_instr(run_step_logs, 'Done.')+6,abs(regexp_instr(run_step_logs, 'Failure') - regexp_instr(run_step_logs, 'SKIP='))) as run_step_totals_message
        ,split(run_step_totals_message,' ') as run_step_totals
        ,to_number(regexp_substr(run_step_totals[0]::string,'\\d{1,}')) as run_step_pass_count
        ,to_number(regexp_substr(run_step_totals[1]::string,'\\d{1,}')) as run_step_warn_count
        ,to_number(regexp_substr(run_step_totals[2]::string,'\\d{1,}')) as run_step_error_count
        ,to_number(regexp_substr(run_step_totals[3]::string,'\\d{1,}')) as run_step_skip_count
        ,to_number(regexp_substr(run_step_totals[4]::string,'\\d{1,}')) as run_step_total_count 
        ,case 
            when run_step_name like '%dbt test%'
                then replace(replace(substr(run_step_logs,regexp_instr(run_step_logs, 'Failure'),abs(regexp_instr(run_step_logs, 'Failure') - regexp_instr(run_step_logs, 'Done.'))),'[31mFailure','Failure'), '[33mWarning', 'Warning') 
        end as dbt_test_failure_message
        ,case 
            when run_step_name not like '%dbt test%' and run_step_status_humanized != 'Success'
                then trim(substr(run_step_logs,regexp_instr(run_step_logs, 'Encountered an error'),500)) 
        end as dbt_error_message
    from runs run
    ,lateral flatten(input => run.run_steps) run_step -- flattening run steps json column

), custom_metrics as (

    select 
        run_id
        ,job_id
        ,sum(run_step_warn_count) as job_warn_count
        ,sum(run_step_error_count) as job_error_count
        ,sum(run_step_skip_count) as job_skip_count
        ,sum(run_step_total_count) as job_total_count
      from runs_with_steps
    group by run_id, job_id

)

select distinct
    -- RUN
    run.run_id
    ,run.job_id
    ,run.run_status
    ,case 
        when lower(run.run_status) = 'success' and custom_metrics.job_warn_count > 0 then 'Warning'
        else run.run_status
    end as run_custom_status
    ,case 
        when lower(run_custom_status) = 'success' then 1
        when lower(run_custom_status) = 'warning' then 2
        when lower(run_custom_status) = 'error' then 3
        else 0
    end as run_custom_status_code
    ,run.is_complete
    ,run.is_success
    ,run.is_error
    ,run.is_cancelled
    ,run.is_in_progress
    ,run.run_href
    ,run.is_active_flag
    -- RUN STEP 
    ,run.run_started_at_timestamp
    ,run.run_started_at_date_id
    ,run.run_finished_at_timestamp
    ,run.run_finished_at_timestamp_id
    ,run.run_duration_name
    ,run.run_duration
    ,run.run_step_id
    ,run.run_step_name
    ,run.run_step_index
    ,run.run_step_status_humanized
    ,run.run_step_started_at_timestamp
    ,run.run_step_started_at_date_id
    ,run.run_step_finished_at_timestamp
    ,run.run_step_finished_at_date_id
    ,run.run_step_duration_name
    ,run.run_step_duration
    ,run.run_step_logs
    ,run.run_step_totals_message
    ,run.run_step_totals
    ,run.run_step_pass_count
    ,run.run_step_warn_count
    ,run.run_step_error_count
    ,run.run_step_skip_count
    ,run.run_step_total_count 
    ,run.dbt_test_failure_message
    ,run.dbt_error_message
    ,custom_metrics.job_warn_count
    ,custom_metrics.job_error_count
    ,custom_metrics.job_skip_count
    ,custom_metrics.job_total_count
from runs_with_steps run
left join custom_metrics on
    run.job_id = custom_metrics.job_id and
    run.run_id = custom_metrics.run_id