
    
    

select
    user_ip as unique_field,
    count(*) as n_records

from DEV_QA.DBT_POC.stg_ipflow_merge_success_output
where user_ip is not null
group by user_ip
having count(*) > 1


