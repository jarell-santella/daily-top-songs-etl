-- Create indexes
DROP INDEX IF EXISTS music_data.artist_song_map_tb_artist_id_idx;

CREATE INDEX artist_song_map_tb_artist_id_idx ON music_data.artist_song_map_tb USING btree (artist_id);

DROP INDEX IF EXISTS music_data.artist_song_map_tb_isrc_idx;

CREATE INDEX artist_song_map_tb_isrc_idx ON music_data.artist_song_map_tb USING btree (isrc);

DROP INDEX IF EXISTS music_data.ranking_tb_ranking_date_ranking_source_rank_idx;

CREATE INDEX ranking_tb_ranking_date_ranking_source_rank_idx ON music_data.ranking_tb USING btree (ranking_date DESC, ranking_source, rank);

-- Create procedural functions
CREATE OR REPLACE FUNCTION music_data.get_formatted_song_info_fn (isrc_input VARCHAR(12)) RETURNS TEXT AS $$
DECLARE
    artist_names TEXT;
    song_title TEXT;
BEGIN
    SELECT STRING_AGG(artist_name, ', ' ORDER BY artist_name)
    INTO artist_names
    FROM music_data.artist_song_map_tb AS t1
    INNER JOIN music_data.artist_tb AS t2
    ON t1.artist_id = t2.artist_id
    WHERE isrc = isrc_input;

    SELECT song_name
    INTO song_title
    FROM music_data.song_tb
    WHERE isrc = isrc_input;

    RETURN artist_names || ' - ' || song_title;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION music_data.calculate_rank_delta_between_dates_fn (baseline_date DATE, comparison_date DATE) RETURNS TABLE (platform music_data.music_source_enum_type, isrc VARCHAR(12), delta INT) AS $$
BEGIN
    RETURN QUERY
    SELECT
        t1.ranking_source AS platform,
        t1.isrc,
        t2.rank - t1.rank AS delta
    FROM
        music_data.ranking_tb t1
    INNER JOIN
        music_data.ranking_tb t2 ON t1.isrc = t2.isrc AND t1.ranking_source = t2.ranking_source
    WHERE
        t1.ranking_date = baseline_date
        AND t2.ranking_date = comparison_date
    ORDER BY
        t1.ranking_source,
        delta DESC;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION music_data.delete_unreferenced_artist_records_fn () RETURNS TRIGGER AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM music_data.artist_song_map_tb WHERE artist_id = OLD.artist_id) THEN
        DELETE FROM music_data.artist_tb WHERE artist_id = OLD.artist_id;
    END IF;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION music_data.delete_unreferenced_song_records_fn () RETURNS TRIGGER AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM music_data.ranking_tb WHERE isrc = OLD.isrc) THEN
        DELETE FROM music_data.song_tb WHERE isrc = OLD.isrc;
    END IF;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION music_data.delete_old_ranking_records_fn () RETURNS TRIGGER AS $$
BEGIN
    DELETE FROM music_data.ranking_tb
    WHERE ranking_date <= NEW.ranking_date - INTERVAL '1 year';
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create triggers
CREATE OR REPLACE TRIGGER artist_song_map_tb_after_delete_tr
AFTER DELETE ON music_data.artist_song_map_tb FOR EACH ROW
EXECUTE FUNCTION music_data.delete_unreferenced_artist_records_fn ();

CREATE OR REPLACE TRIGGER ranking_tb_after_delete_tr
AFTER DELETE ON music_data.ranking_tb FOR EACH ROW
EXECUTE FUNCTION music_data.delete_unreferenced_song_records_fn ();

CREATE OR REPLACE TRIGGER ranking_tb_after_insert_tr
AFTER INSERT ON music_data.ranking_tb FOR EACH ROW
EXECUTE FUNCTION music_data.delete_old_ranking_records_fn ();

-- Create views
CREATE OR REPLACE VIEW
    music_data.all_rankings_view AS
SELECT
    ranking_date AS date,
    ranking_source AS platform,
    rank,
    music_data.get_formatted_song_info_fn (isrc) AS song
FROM
    music_data.ranking_tb
ORDER BY
    ranking_date DESC,
    ranking_source,
    rank;

CREATE OR REPLACE VIEW
    music_data.all_rankings_with_urls_view AS
SELECT
    ranking_date AS date,
    ranking_source AS platform,
    rank,
    music_data.get_formatted_song_info_fn (t1.isrc) AS song,
    spotify_url,
    apple_music_url
FROM
    music_data.ranking_tb t1
LEFT JOIN
    music_data.song_tb t2 ON t1.isrc = t2.isrc
ORDER BY
    ranking_date DESC,
    ranking_source,
    rank;

CREATE OR REPLACE VIEW
    music_data.all_rankings_with_delta_view AS
SELECT
    ranking_date AS date,
    ranking_source AS platform,
    rank,
    music_data.get_formatted_song_info_fn (t1.isrc) AS song,
    spotify_url,
    apple_music_url,
    CASE
        WHEN t1.ranking_date = LAG(t1.ranking_date) OVER (
            PARTITION BY ranking_source, t1.isrc
            ORDER BY ranking_date
        ) + INTERVAL '1 day' THEN
            LAG(t1.rank) OVER (
                PARTITION BY ranking_source, t1.isrc
                ORDER BY ranking_date
            ) - rank
        ELSE
            NULL
    END AS delta
FROM
    music_data.ranking_tb t1
LEFT JOIN
    music_data.song_tb t2 ON t1.isrc = t2.isrc
ORDER BY
    ranking_date DESC,
    ranking_source,
    rank;
