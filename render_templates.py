"""
This script is designed to generate files from data queried from a database.

Technical Details
-----------------

The script uses several libraries:

- `asyncpg` for asynchronous interaction with PostgreSQL.
- `configparser` for configuration management.
- `Jinja2` for template processing.
- `python-dotenv` for environment variable handling.

Environment Variables and Configuration
----------------------------------------

- Database connection parameters should be set as environment variables.
"""


import asyncio
from configparser import ConfigParser
import datetime
import logging
import os

import asyncpg
from jinja2 import Environment, FileSystemLoader
from dotenv import load_dotenv


load_dotenv()


DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


config = ConfigParser()
config.read(os.path.join(BASE_DIR, "config.ini"))


LOGGING_LEVEL = config["LOGGING"]["LOGGING_LEVEL"]


if LOGGING_LEVEL in ("", "NOTSET"):
    logging.disable()
    LOGGING_LEVEL = logging.NOTSET


logging.basicConfig(
    level=LOGGING_LEVEL,
    format="%(asctime)s - %(levelname)s (line %(lineno)d): %(message)s",
)


logger = logging.getLogger(__name__)


async def main() -> None:
    """
    Main asynchronous entry point of the script.

    This function orchestrates the overall workflow of the script. It performs tasks such as
    querying the database and generating files from Jinja2 templates from the data that was
    fetched from the database.

    The function performs the following major steps:

    1. Establishes an asynchronous HTTP client session and a connection pool to the database.
    2. Fetches ranking data from the database.
    3. Renders Jinja2 templates and loads the rendered content into files.

    Returns:
        None: This function does not return any value but performs file operations.
    """
    date_today = datetime.datetime.now(datetime.UTC).date()
    formatted_date_today = date_today.strftime("%A, %B %d, %Y").replace(" 0", " ")

    logger.debug("Date today: '%s'.", date_today)
    logger.debug("Formatted date today: '%s'.", formatted_date_today)

    env = Environment(loader=FileSystemLoader("templates"))

    connection = await asyncpg.connect(
        host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD
    )

    logger.info("Querying database for today's song data.")

    records = await connection.fetch(
        """
        SELECT
            song,
            spotify_url,
            apple_music_url,
            delta
        FROM
            music_data.all_rankings_with_delta_view
        WHERE
            date = $1;
        """,
        date_today,
    )

    await connection.close()

    spotify_songs = []
    apple_music_songs = []
    for i, record in enumerate(records):
        record = dict(record)

        logger.debug("Record %s queried from database.", record)

        record["spotify_embed_src"] = (
            record["spotify_url"].replace("/track/", "/embed/track/") + "?theme=0"
        )
        if record["apple_music_url"] is not None:
            record["apple_music_embed_src"] = record["apple_music_url"].replace(
                "https://music.apple.com/", "https://embed.music.apple.com/"
            )
        if i < 10:
            spotify_songs.append(record)
        else:
            apple_music_songs.append(record)

    env = Environment(loader=FileSystemLoader("templates"))
    readme_md_template = env.get_template("readme.md.jinja2")

    data = {
        "date": formatted_date_today,
        "spotify_songs": spotify_songs,
        "apple_music_songs": apple_music_songs,
    }

    logger.debug("Final template data:\n\n%s\n", data)

    logger.info("Rendering README.md Jinja2 template.")

    rendered_readme_md_template = readme_md_template.render(data)

    logger.info("Saving rendered README.md Jinja2 template.")

    with open(os.path.join(BASE_DIR, "README.md"), "w", encoding="UTF-8") as file:
        file.write(rendered_readme_md_template)

    logger.info("Script finished successfully.")


if __name__ == "__main__":
    logger.info("Script starting.")

    asyncio.run(main())
