with jobs as (
  select 
    *,
    max(to_timestamp(_modified)) over() as max_modified_timestamp
  from {{source('dbt_api','job')}}
  where environment_id = '0000' --production environment
)

select 
    id as job_id			
    ,account_id as job_account_id			
    ,project_id	as job_project_id			
    ,environment_id	as job_environment_id			
    ,_file as file_name
    ,_line as line		
    ,execution as job_execution		
    ,generate_docs as job_generate_docs		
    ,run_generate_sources as job_run_generate_sources		
    ,name as job_name			
    ,dbt_version as job_dbt_version			
    ,execute_steps as job_execute_steps		
    ,state as job_state			
    ,deferring_job_definition_id as deferring_job_definition_id			
    ,lifecycle_webhooks as job_lifecycle_webhooks		
    ,triggers as job_triggers		
    ,settings as job_settings		
    ,schedule as job_schedule		
    ,is_deferrable as is_job_deferrable		
    ,generate_sources as job_generate_sources		
    ,cron_humanized as job_cron_humanized			
    ,next_run as job_next_run			
    ,next_run_humanized as job_next_run_humanized			
    ,to_timestamp(_fivetran_synced) as fivetran_synced_timestamp		
    ,to_timestamp(_modified) as modified_at_timestamp			
    ,to_timestamp(created_at) as job_created_at_timestamp			
    ,to_timestamp(updated_at) as job_updated_at_timestamp	
    ,to_number(to_char(modified_at_timestamp,'yyyymmdd')) as modified_at_date_id
    ,to_number(to_char(job_created_at_timestamp,'yyyymmdd')) as job_created_at_date_id
    ,to_number(to_char(job_updated_at_timestamp,'yyyymmdd')) as job_updated_at_date_id		
from jobs
where modified_at_timestamp = max_modified_timestamp