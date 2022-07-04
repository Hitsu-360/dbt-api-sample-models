with dbt_tests as (

    select replace(tests, 'None', 'null') as tests from {{ref('stg_dbt_model')}} where tests != '[]'
        union
    select replace(tests, 'None', 'null') as tests from {{ref('stg_dbt_source')}} where tests != '[]'

), flatten_tests as (
    select distinct
        test.value:uniqueId::string as test_id,
        test.value:runId::number as run_id, 
        test.value:accountId::number as account_id,
        test.value:projectId::number as project_id,
        test.value:environmentId::number as environment_id,
        test.value:jobId::number as job_id, 
        test.value:name::string as test_name,
        test.value:status::string as test_status,
        test.value:state::string as test_state,
        test.value:pass::boolean as is_pass,
        test.value:warn::boolean as is_warning,
        test.value:error::boolean as is_error
    from dbt_tests,
    lateral flatten(input => parse_json(dbt_tests.tests)) test
), max_run_id_by_test as (
    select 
        test_id,
        max(run_id) as max_run_id
    from flatten_tests
    group by test_id
)

select 
    flatten_tests.test_id,
    run_id, 
    account_id,
    project_id,
    environment_id,
    job_id,
    test_name,
    test_status,
    test_state,
    is_pass,
    is_warning,
    is_error
from flatten_tests
inner join max_run_id_by_test on 
    flatten_tests.test_id = max_run_id_by_test.test_id and 
    flatten_tests.run_id = max_run_id_by_test.max_run_id




