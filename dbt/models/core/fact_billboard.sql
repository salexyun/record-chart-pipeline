{{ config(materialized='table') }}

with acoustic_data as (
    select *
    from {{ ref('stg_acoustic_features') }}
),

segment_data as (
    select *
    from {{ ref('stg_segments') }}
),

-- Joins audio features and analysis
track_data as (
    select
        acoustic_data.spotify_track_id,
        acoustic_data.spotify_album_id,

        acoustic_data.artist,
        acoustic_data.track,
        acoustic_data.album,

        acoustic_data.album_released,

        acoustic_data.acousticness,
        acoustic_data.danceability,
        acoustic_data.duration_ms,
        acoustic_data.energy,
        acoustic_data.instrumentalness,
        acoustic_data.`key`,
        acoustic_data.liveness,
        acoustic_data.loudness,
        acoustic_data.mode,
        acoustic_data.speechiness,
        acoustic_data.tempo,
        acoustic_data.time_signature,
        acoustic_data.valence,
        
        segment_data.timbre,
        segment_data.pitches,
        segment_data.confidence,
        segment_data.duration,
        segment_data.loudness_max,
        segment_data.loudness_max_time,
        segment_data.loudness_start
    from
        acoustic_data,
        segment_data
    where acoustic_data.spotify_track_id = segment_data.spotify_track_id
),

album_data as (
    select *
    from {{ ref('stg_albums') }}
)

select
    track_data.spotify_track_id,
    track_data.spotify_album_id,
    -- track_data.primary_key,
    -- track_data.id,

    track_data.acousticness,
    track_data.danceability,
    track_data.duration_ms,
    track_data.energy,
    track_data.instrumentalness,
    track_data.`key`,
    track_data.liveness,
    track_data.loudness,
    track_data.mode,
    track_data.speechiness,
    track_data.tempo,
    track_data.time_signature,
    track_data.valence,
    track_data.timbre,
    track_data.pitches,
    track_data.confidence,
    track_data.duration,
    track_data.loudness_max,
    track_data.loudness_max_time,
    track_data.loudness_start,
    track_data.artist,
    track_data.track,
    track_data.album,
    track_data.album_released,

    album_data.len_album,
    album_data.num_tracks,
    album_data.chart_week,
    album_data.chart_rank
from track_data
join album_data on track_data.album = album_data.album