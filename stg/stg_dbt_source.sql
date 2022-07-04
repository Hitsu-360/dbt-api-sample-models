with source as (
    select 
        source.*,
        max(source.snapshotted_at) over(partition by source.unique_id) as max_snapshotted_at_timestamp 
    from {{source('dbt_api', 'source')}} source
    where environment_id = '0000' --production environment
)
select 
    _file as file_name,
    _line as line,
    _modified as modified_timestamp,
    unique_id as source_id,
    run_id,
    job_id,
    environment_id,
    project_id,
    account_id, 
    name as table_name,
    source_name,
    source_description,
    state as source_state,
    case 
        when lower(source_state) = 'pass' then 1
        when lower(source_state) = 'warn' then 2
        when lower(source_state) = 'error' then 3
        else 0
    end as source_state_code,
    max_loaded_at as max_loaded_at_timestamp,
    snapshotted_at as snapshotted_at_timestamp,
    run_generated_at as run_generated_at_timestamp,
    run_elapsed_time,
    criteria,
    tests,
    _fivetran_synced as fivetran_synced_timestamp
from source 
where snapshotted_at_timestamp = max_snapshotted_at_timestamp