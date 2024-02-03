-- Load data into tables from temporary tables
INSERT INTO
    music_data.artist_tb (artist_id, artist_name)
SELECT
    artist_id,
    artist_name
FROM
    music_data.temp_artist_tb
ON CONFLICT DO NOTHING;

INSERT INTO music_data.song_tb
    (isrc, song_name, song_duration_ms, is_explicit, spotify_url, apple_music_url)
SELECT
    isrc,
    song_name,
    song_duration_ms,
    is_explicit,
    spotify_url,
    apple_music_url
FROM
    music_data.temp_song_tb
ORDER BY
    apple_music_url NULLS LAST
ON CONFLICT DO NOTHING;

INSERT INTO
    music_data.artist_song_map_tb (artist_id, isrc)
SELECT
    artist_id,
    isrc
FROM
    music_data.temp_artist_song_map_tb
ON CONFLICT DO NOTHING;

INSERT INTO
    music_data.ranking_tb (isrc, ranking_date, rank, ranking_source)
SELECT
    isrc,
    ranking_date,
    rank,
    ranking_source
FROM
    music_data.temp_ranking_tb
ON CONFLICT DO NOTHING;

-- Clean up temporary tables
DROP TABLE music_data.temp_artist_tb CASCADE;

DROP TABLE music_data.temp_song_tb CASCADE;

DROP TABLE music_data.temp_artist_song_map_tb CASCADE;

DROP TABLE music_data.temp_ranking_tb CASCADE;
