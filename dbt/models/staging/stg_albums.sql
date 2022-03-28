{{ config(materialized='view') }}

select
    -- Identifiers
    cast(id as int) as id,

    -- Metadata
    cast(album as string) as album,
    cast(artist as string) as artist,
    cast(track_length as numeric) as len_album,
    cast(length as integer) as num_tracks,

    -- Billboard 200 chart info
    cast(`date_parsed` as date) as chart_week,
    cast(`rank` as integer) as chart_rank
from
    {{ source('staging', 'albums') }}