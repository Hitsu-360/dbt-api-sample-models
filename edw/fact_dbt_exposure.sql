{{
  config(
    unique_key = 'exposure_id',
    tags=['dbt_logs']
  )
}}

select 
    exposure.file_name,
    exposure.line,
    exposure.modified_timestamp,
    exposure.exposure_id,
    exposure.run_id,
    exposure.job_id,
    exposure.environment_id,
    exposure.project_id,
    exposure.account_id,
    exposure.exposure_name,
    exposure.exposure_description,
    exposure.resource_type,
    exposure.exposure_owner_name,
    exposure.exposure_owner_email,
    exposure.exposure_url,
    exposure.exposure_sources,
    exposure.exposure_models,
    exposure.fivetran_synced_timestamp
from {{ ref('stg_dbt_exposure') }} exposure
