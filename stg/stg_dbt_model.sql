with model as (
    select 
        model.*,
        max(model.execute_completed_at) over(partition by model.unique_id) as execute_completed_at_timestamp 
    from {{source('dbt_api', 'model')}} model
    where environment_id = '0000' --production environment
)
select 
    _file as file_name,
    _line as line,
    _modified as modified_timestamp,
    unique_id as model_id,
    run_id,
    job_id,
    environment_id,
    project_id,
    account_id,
    name as model_name,
    error,
    schema,
    status,
    case 
        when lower(status) = 'success' then 1
        when lower(status) = 'error' then 3
        else 0
    end as status_code,
    skip,
    execution_time,
    tests,
    _fivetran_synced as fivetran_synced_timestamp,
    execute_started_at as execute_started_at_timestamp,
    execute_completed_at as execute_completed_at_timestamp
from model 
where execute_completed_at = execute_completed_at_timestamp