with exposure as (
    select 
        exposure.exposure_id,
        exposure_model.value:uniqueId::string as exposure_model_unique_id
    from  {{ref('stg_dbt_exposure')}} exposure,
    lateral flatten(input => parse_json(exposure.exposure_models)) exposure_model
)
select 
    exposure.exposure_id,
    exposure.exposure_model_unique_id as model_id
from exposure