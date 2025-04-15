
    
    

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


