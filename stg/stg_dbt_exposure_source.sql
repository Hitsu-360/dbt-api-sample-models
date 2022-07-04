with exposure as (
    select 
        exposure.exposure_id,
        exposure_source.value:uniqueId::string as exposure_source_unique_id
    from  {{ref('stg_dbt_exposure')}} exposure,
    lateral flatten(input => parse_json(exposure.exposure_sources)) exposure_source
)
select 
    exposure.exposure_id,
    exposure.exposure_source_unique_id as source_id
from exposure