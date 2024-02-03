-- Clean up temporary tables
DROP TABLE IF EXISTS music_data.temp_artist_tb CASCADE;

DROP TABLE IF EXISTS music_data.temp_song_tb CASCADE;

DROP TABLE IF EXISTS music_data.temp_artist_song_map_tb CASCADE;

DROP TABLE IF EXISTS music_data.temp_ranking_tb CASCADE;

-- Create temporary tables (cannot use CREATE TEMP TABLE for this)
CREATE TABLE
    music_data.temp_artist_tb AS
SELECT
    *
FROM
    music_data.artist_tb
WITH
    NO DATA;

CREATE TABLE
    music_data.temp_song_tb AS
SELECT
    *
FROM
    music_data.song_tb
WITH
    NO DATA;

CREATE TABLE
    music_data.temp_artist_song_map_tb AS
SELECT
    *
FROM
    music_data.artist_song_map_tb
WITH
    NO DATA;

CREATE TABLE
    music_data.temp_ranking_tb AS
SELECT
    *
FROM
    music_data.ranking_tb
WITH
    NO DATA;
