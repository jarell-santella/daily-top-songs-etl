-- Create schemas
CREATE SCHEMA IF NOT EXISTS music_data;

-- Clean up schema
DROP TYPE IF EXISTS music_data.music_source_enum_type CASCADE;

DROP TABLE IF EXISTS music_data.artist_tb CASCADE;

DROP TABLE IF EXISTS music_data.song_tb CASCADE;

DROP TABLE IF EXISTS music_data.artist_song_map_tb CASCADE;

DROP TABLE IF EXISTS music_data.ranking_tb CASCADE;

-- Create types
CREATE TYPE music_data.music_source_enum_type AS ENUM('Spotify', 'Apple Music');

-- Create tables
CREATE TABLE
    music_data.artist_tb (
        artist_id VARCHAR(22),
        artist_name TEXT NOT NULL,
        CONSTRAINT artist_tb_pkey PRIMARY KEY (artist_id)
    );

ALTER TABLE music_data.artist_tb ENABLE ROW LEVEL SECURITY;

CREATE TABLE
    music_data.song_tb (
        isrc VARCHAR(12),
        song_name TEXT NOT NULL,
        song_duration_ms INTEGER NOT NULL,
        is_explicit BOOLEAN NOT NULL,
        spotify_url TEXT NOT NULL,
        apple_music_url TEXT, 
        CONSTRAINT song_tb_pkey PRIMARY KEY (isrc),
        CONSTRAINT song_tb_spotify_url_key UNIQUE (spotify_url),
        CONSTRAINT song_tb_apple_music_url_key UNIQUE (apple_music_url)
    );

ALTER TABLE music_data.song_tb ENABLE ROW LEVEL SECURITY;

CREATE TABLE
    music_data.artist_song_map_tb (
        artist_id VARCHAR(22),
        isrc VARCHAR(12),
        CONSTRAINT artist_song_map_tb_pkey PRIMARY KEY (artist_id, isrc),
        CONSTRAINT artist_song_map_tb_artist_id_fkey FOREIGN KEY (artist_id) REFERENCES music_data.artist_tb (artist_id) ON DELETE NO ACTION ON UPDATE NO ACTION,
        CONSTRAINT artist_song_map_tb_isrc_fkey FOREIGN KEY (isrc) REFERENCES music_data.song_tb (isrc) ON DELETE CASCADE ON UPDATE NO ACTION
    );

ALTER TABLE music_data.artist_song_map_tb ENABLE ROW LEVEL SECURITY;

CREATE TABLE
    music_data.ranking_tb (
        ranking_id SERIAL,
        isrc VARCHAR(12) NOT NULL,
        ranking_date DATE NOT NULL,
        rank INTEGER NOT NULL,
        ranking_source music_data.music_source_enum_type NOT NULL,
        CONSTRAINT ranking_tb_pkey PRIMARY KEY (ranking_id),
        CONSTRAINT ranking_tb_isrc_fkey FOREIGN KEY (isrc) REFERENCES music_data.song_tb (isrc) ON DELETE NO ACTION ON UPDATE NO ACTION,
        CONSTRAINT ranking_tb_ranking_date_check CHECK (ranking_date <= CURRENT_DATE),
        CONSTRAINT ranking_tb_rank_check CHECK (
            rank >= 1
            AND rank <= 10
        ),
        CONSTRAINT ranking_tb_isrc_ranking_date_ranking_source_key UNIQUE (isrc, ranking_date, ranking_source),
        CONSTRAINT ranking_tb_ranking_date_rank_ranking_source_key UNIQUE (ranking_date, rank, ranking_source)
    );

ALTER TABLE music_data.ranking_tb ENABLE ROW LEVEL SECURITY;

-- Create policies for row-level security
CREATE POLICY artist_tb_all_policy ON music_data.artist_tb FOR ALL USING (false);

CREATE POLICY song_tb_all_policy ON music_data.song_tb FOR ALL USING (false);

CREATE POLICY artist_song_map_tb_all_policy ON music_data.artist_song_map_tb FOR ALL USING (false);

CREATE POLICY ranking_tb_all_policy ON music_data.ranking_tb FOR ALL USING (false);
