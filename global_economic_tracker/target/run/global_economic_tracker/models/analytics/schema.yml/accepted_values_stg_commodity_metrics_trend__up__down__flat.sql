select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        trend as value_field,
        count(*) as n_records

    from "airflow"."public"."stg_commodity_metrics"
    group by trend

)

select *
from all_values
where value_field not in (
    'up','down','flat'
)



      
    ) dbt_internal_test