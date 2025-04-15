
    
    

select
    ticker as unique_field,
    count(*) as n_records

from "airflow"."public"."daily_returns"
where ticker is not null
group by ticker
having count(*) > 1


