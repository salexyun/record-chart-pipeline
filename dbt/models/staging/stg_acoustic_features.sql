{{ config(materialized='view') }}

select
    -- Identifiers
    cast(id as string) as spotify_track_id,
    cast(album_id as string) as spotify_album_id,

    -- Metadata
    cast(artist as string) as artist,
    cast(song as string) as track,
    cast(album as string) as album,
    cast(date_parsed as date) as album_released,

    -- Track's audio features
    cast(acousticness as numeric) as acousticness,
    cast(danceability as numeric) as danceability,
    cast(duration_ms as integer) as duration_ms,
    cast(energy as numeric) as energy,
    cast(instrumentalness as numeric) as instrumentalness,
    cast(`key` as integer) as key,
    cast(liveness as numeric) as liveness,
    cast(loudness as numeric) as loudness,
    cast(mode as integer) as mode,
    cast(speechiness as numeric) as speechiness,
    cast(tempo as numeric) as tempo,
    cast(time_signature as integer) as time_signature,
    cast(valence as numeric) as valence
from
    {{ source('staging', 'acoustic_features') }}