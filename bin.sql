with a as (
SELECT json_extract_scalar(replace(run_body, 'dagster-k8s/config', 'dagsterk8sconfig'), '$.tags.dagsterk8sconfig') as tags,
    run_id,
    end_time - start_time as duration,
    pipeline_name,
    start_time
    FROM "datalake_agg"."dagster_runs" 
    where from_unixtime(cast(start_time as int)) >= date_parse('2025-08-25 04:00', '%Y-%m-%d %H:%i')
    and from_unixtime(cast(start_time as int)) < date_parse('2025-08-25 12:00', '%Y-%m-%d %H:%i')
    and status = 'SUCCESS'
)
, b as (
    select 
        json_extract(tags, '$.container_config.resources.limits') as limits,
        run_id,
        duration,
        pipeline_name, 
        from_unixtime(cast(start_time as int)) as start_time
    from a
)
, c as (
    select 
    pipeline_name,
    run_id,
    start_time,
    duration,
    json_extract_scalar(limits, '$.cpu') as cpus, 
    json_extract_scalar(limits, '$.memory') as mem    
    from b
)
, d as (
    select pipeline_name, min(start_time) as m
    from c
    group by pipeline_name
)
select c.*
from c
join d on d.m = c.start_time and d.pipeline_name = c.pipeline_name
order by pipeline_name
