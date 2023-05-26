{{ config(materialized='view') }}

with tripdata as
(
    select *,
    from {{source('staging', 'fhv_tripdata') }}
    where dispatching_base_num is not null
)
--columns: 'dispatching_base_num', 'pickup_datetime', 'dropOff_datetime', 'PUlocationID', 'DOlocationID', "SR_Flag', 'Affiliated_base_number'
select
-- identifiers
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime', 'PUlocationID']) }} as tripid,
    dispatching_base_num,
    cast(PUlocationID as integer) as pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,
    cast(SR_Flag as integer) as sr_flag,

-- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,

from tripdata

-- dbt run --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=false) %}

limit 100

{% endif %}