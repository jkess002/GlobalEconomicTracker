select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select timestamp
from "airflow"."public"."daily_returns"
where timestamp is null



      
    ) dbt_internal_test