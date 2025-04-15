select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select daily_return
from "airflow"."public"."stg_index_returns"
where daily_return is null



      
    ) dbt_internal_test