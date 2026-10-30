-- Incremental: each run merges only the sequences Silver added past this
-- table's highest id. `olf product run --full-refresh` rebuilds it.
{{ config(materialized='incremental', unique_key='urs', incremental_strategy='merge') }}

select
    id,
    urs,
    length,
    case
        when length is null then 'unknown'
        when length < 50 then 'under 50 nt'
        when length < 200 then '50-199 nt'
        when length < 1000 then '200-999 nt'
        else '1000+ nt'
    end as length_band,
    md5,
    created_at
from {{ source('silver', 'sequences') }}
{% if is_incremental() %}
where id > (select coalesce(max(id), 0) from {{ this }})
{% endif %}
