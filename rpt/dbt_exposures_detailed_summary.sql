with exposures_summary as (
    select 
        --EXPOSURE
        exposure.exposure_id,
        exposure.run_id as exposure_run_id,
        exposure.job_id as exposure_job_id,
        exposure.environment_id as exposure_environment_id,
        exposure.project_id as exposure_project_id,
        exposure.account_id as exposure_account_id,
        exposure.exposure_name,
        exposure.exposure_description,
        exposure.resource_type,
        exposure.exposure_owner_name,
        exposure.exposure_owner_email,
        exposure.exposure_url,
        exposure.exposure_sources,
        exposure.exposure_models,
        --MODEL
        model.model_id,
        model.run_id as model_run_id,
        model.job_id as model_job_id,
        model.environment_id as model_environment_id,
        model.project_id as model_project_id,
        model.account_id as model_account_id,
        model.model_name,
        model.error as model_error,
        model.schema as model_schema,
        model.status as model_status,
        model.status_code as model_status_code,
        model.skip as model_skip,
        model.execution_time as model_execution_time,
        model.execute_started_at_timestamp as model_execute_started_at_timestamp,
        model.execute_completed_at_timestamp as model_execute_completed_at_timestamp,
        --SOURCE
        source.source_id,
        source.run_id as source_run_id,
        source.job_id as source_job_id,
        source.environment_id as source_environment_id,
        source.project_id as source_project_id,
        source.account_id as source_account_id, 
        source.table_name as source_table,
        source.source_name,
        source.source_description,
        source.source_state,
        source.source_state_code,
        source.max_loaded_at_timestamp as source_max_loaded_at_timestamp,
        source.snapshotted_at_timestamp as source_snapshotted_at_timestamp,
        source.run_generated_at_timestamp as source_run_generated_at_timestamp,
        source.run_elapsed_time as source_run_elapsed_time,
        source.criteria as source_criteria
    from {{ref('fact_dbt_exposure')}} exposure
    left join {{ref('stg_dbt_exposure_model')}} exposure_model on exposure.exposure_id = exposure_model.exposure_id
    left join {{ref('stg_dbt_exposure_source')}} exposure_source on exposure.exposure_id = exposure_source.exposure_id
    left join {{ref('fact_dbt_model')}} model on exposure_model.model_id = model.model_id
    left join {{ref('fact_dbt_source')}} source on exposure_source.source_id = source.source_id
), metrics as (
    select 
        exposure_id,
        count_if(model_status_code = 1) as exposure_count_model_success,
        count_if(model_status_code = 2) as exposure_count_model_error,
        count_if(source_state_code = 1) as exposure_count_source_pass, 
        count_if(source_state_code = 2) as exposure_count_source_warn,
        count_if(source_state_code = 3) as exposure_count_source_fail
    from exposures_summary
    group by exposure_id
)

select 
    exposures_summary.*,
    case 
        when metrics.exposure_count_model_error = 0 and  
            metrics.exposure_count_source_warn = 0 and
            metrics.exposure_count_source_fail = 0
        then 'Success'
        when metrics.exposure_count_model_error = 0 and 
            metrics.exposure_count_source_warn > 0 and
            metrics.exposure_count_source_fail = 0
            then 'Warning'
        else 'Error' 
    end as exposure_status,
    case 
        when lower(exposure_status) = 'success' then 1
        when lower(exposure_status) = 'warning' then 2
        when lower(exposure_status) = 'error' then 3
        else 0
    end as exposure_status_code,
    metrics.exposure_count_model_success,
    metrics.exposure_count_model_error,
    metrics.exposure_count_source_pass,
    metrics.exposure_count_source_warn,
    metrics.exposure_count_source_fail
from exposures_summary
left join metrics on exposures_summary.exposure_id = metrics.exposure_id

