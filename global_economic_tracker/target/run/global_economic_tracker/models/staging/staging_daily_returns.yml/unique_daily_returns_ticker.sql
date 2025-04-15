select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    ticker as unique_field,
    count(*) as n_records

from "airflow"."public"."daily_returns"
where ticker is not null
group by ticker
having count(*) > 1



      
    ) dbt_internal_test