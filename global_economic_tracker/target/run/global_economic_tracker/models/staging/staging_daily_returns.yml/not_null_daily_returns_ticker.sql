select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select ticker
from "airflow"."public"."daily_returns"
where ticker is null



      
    ) dbt_internal_test