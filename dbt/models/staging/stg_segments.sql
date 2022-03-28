{{ config(materialized='view') }}

select
    -- Identifiers
    cast(`key` as string) as primary_key,
    cast(id as string) as spotify_track_id,
    cast(album_id as string) as spotify_album_id,

    -- Metadata
    cast(artist as string) as artist,
    cast(song as string) as track,
    cast(album as string) as album,

    -- Track's audio analysis
    cast(timbre as string) as timbre,
    cast(pitches as string) as pitches,
    cast(confidence as numeric) as confidence,
    cast(duration as numeric) as duration,
    cast(loudness_max as numeric) as loudness_max,
    cast(loudness_max_time as numeric) as loudness_max_time,
    cast(loudness_start as numeric) as loudness_start
from
    {{ source('staging', 'segments') }}