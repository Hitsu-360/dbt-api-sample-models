with exposure as (
    select 
        exposure.*,
        max(exposure._modified) over(partition by exposure.unique_id) as max_modified_timestamp 
    from {{source('dbt_api', 'exposure')}} exposure
    where 
        parents_sources != '[]' and
        parents_models != '[]' and
        environment_id = '0000' --production environment
)
select 
    _file as file_name,
    _line as line,
    _modified as modified_timestamp,
    unique_id as exposure_id,
    run_id,
    job_id,
    environment_id,
    project_id,
    account_id,
    name as exposure_name,
    description as exposure_description,
    resource_type as resource_type,
    owner_name as exposure_owner_name,
    owner_email as exposure_owner_email,
    url as exposure_url,
    parents_sources as exposure_sources,
    parents_models as exposure_models,
    _fivetran_synced as fivetran_synced_timestamp
from exposure 
where _modified = max_modified_timestamp