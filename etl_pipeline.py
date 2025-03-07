# pylint: disable=too-many-lines
"""
This script is designed to act as an ETL (Extract, Transform, Load) pipeline that periodically
fetches and processes music playlist data from Spotify and Apple Music, and then loads this data
into a database as well as saves it to CSV files. It leverages asynchronous programming for
efficient handling of web requests and database operations.

Key Features
------------

- **Fetching Playlist Data**: Uses web scraping and API calls to obtain playlist data from Spotify
  and Apple Music.
- **Data Extraction**: Extracts song, artist, and ranking data from data fetched from Spotify and
  Apple Music though both web scraping and API calls.
- **Data Transformation**: Transforms the data and cleans it to maintain data integrity and increase
  usability.
- **Data Loading**: Loads the extracted data into a PostgreSQL database and saves it to CSV files.

Technical Details
-----------------

The script is mostly I/O bound and utilizes several asynchronous libraries for coroutine support
to make HTTP requests, database operations, and file operations concurrently:

- `aiohttp` for asynchronous HTTP requests.
- `asyncpg` for asynchronous interaction with PostgreSQL.
- `aiofiles` for asynchronous file operations.

Furthermore, the script uses several libraries for data parsing, and configuration:

- `Beautiful Soup` for HTML parsing.
- `json` for JSON parsing.
- `configparser` and `python-dotenv` for configuration management and environment variable handling.

Environment Variables and Configuration
----------------------------------------

- Spotify API credentials (client ID and secret) and database connection parameters should be set as
  environment variables.
- Additional configuration, such as the specific Spotify and Apple Music playlists to scrape, are
  available in the 'config.ini' file.
- The script checks the Python version at runtime and requires Python 3.9 or higher.

Assumptions
------------

This script's functionality is dependent on the structure of the web pages and APIs it interacts
with. Changes to Spotify's or Apple Music's web structure or API response formats may require
updates to the script.
"""

import asyncio
import base64
from configparser import ConfigParser
import datetime
import json
import logging
import os
import sys
from typing import Any

import aiofiles
import aiohttp
import asyncpg
from bs4 import BeautifulSoup
from dotenv import load_dotenv


load_dotenv()


SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")


DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


BASE_DIR = os.path.dirname(os.path.abspath(__file__))


config = ConfigParser()
config.read(os.path.join(BASE_DIR, "config.ini"))


SPOTIFY_PLAYLIST_URL = config["DEFAULT"]["SPOTIFY_PLAYLIST_URL"]
APPLE_MUSIC_PLAYLIST_URL = config["DEFAULT"]["APPLE_MUSIC_PLAYLIST_URL"]


SPOTIFY_BASE_API_URL = config["API"]["SPOTIFY_BASE_API_URL"]
SPOTIFY_TOKEN_API_URL = config["API"]["SPOTIFY_TOKEN_API_URL"]


if SPOTIFY_BASE_API_URL[-1] != "/":
    SPOTIFY_BASE_API_URL += "/"


LOGGING_LEVEL = config["LOGGING"]["LOGGING_LEVEL"]


if LOGGING_LEVEL in ("", "NOTSET"):
    logging.disable()
    LOGGING_LEVEL = logging.NOTSET


logging.basicConfig(
    level=LOGGING_LEVEL,
    format="%(asctime)s - %(levelname)s (line %(lineno)d): %(message)s",
)


logger = logging.getLogger(__name__)


class UnexpectedContentTypeError(Exception):
    """
    Exception raised when an unexpected content type is encountered in a HTTP response's
    'Content-Type' header.

    This exception is used to indicate that a response from an HTTP request has a content type
    that is not handled by the application. It is typically raised when a function expects a
    specific content type (like 'text/html' or 'application/json') but encounters a different one.

    Attributes:
        message (str): An optional error message that can provide additional context about the
            content type issue.
    """

    def __init__(
        self,
        message="Expected content type of either 'text/html' or 'application/json'.",
    ):
        super().__init__(message)


class SongDataNotFoundError(Exception):
    """
    Exception raised when expected song data is missing.

    This exception is used to indicate that critical song data is missing such that the ETL
    pipeline cannot continue in order to preserve data integrity.

    Attributes:
        message (str): An optional error message that can provide additional context about the
            data not found issue.
    """

    def __init__(
        self,
        message="Expected song data is missing.",
    ):
        super().__init__(message)


class UnsupportedPythonVersionError(Exception):
    """
    Exception raised when the Python version is not supported.

    This exception is used to signal that the running version of Python does not meet the
    application's or script's minimum version requirement. It is typically raised during
    startup or configuration checks to ensure compatibility and prevent runtime errors
    associated with version incompatibilities.

    Attributes:
        message (str): An optional error message that can provide additional context about the
            Python version issue.
    """

    def __init__(self, message=f"Python version {sys.version} is not supported."):
        super().__init__(message)


async def fetch(
    session: aiohttp.ClientSession, url: str, **kwargs: Any
) -> str | dict[str, Any]:
    """
    Asynchronously fetches content from a given URL using an aiohttp session.

    This function sends an asynchronous HTTP GET request to the specified URL. It handles responses
    with content types of either 'text/html' or 'application/json'. For 'text/html', it returns the
    raw text response. For 'application/json', it returns the response as a JSON object
    (dictionary).

    Parameters:
        session (aiohttp.ClientSession): The HTTP client session for making requests.
        url (str): The URL to fetch data from.
        **kwargs (Any): Additional keyword arguments to be passed to the session.get() method.

    Returns:
        str | dict[str, Any]: The content of the response, either as a string (for HTML content) or
            as a dictionary (for JSON content).

    Raises:
        aiohttp.ClientError: If an HTTP request fails.
        UnexpectedContentTypeError: If the response's content type is neither 'text/html' nor
            'application/json'.

    Note:
        This function is a general-purpose HTTP GET request handler, supporting content negotiation
        for commonly used content types. It should be used when the expected content type is known
        to be either HTML or JSON.
    """
    logger.debug("Fetching '%s'.", url)
    async with session.get(url, **kwargs) as response:
        response: aiohttp.ClientResponse
        response.raise_for_status()

        content_type = response.headers["Content-Type"]
        if "text/html" in content_type:
            logger.debug("Fetch for '%s' has content of 'text/html' type.", url)
            return await response.text()
        if "application/json" in content_type:
            logger.debug("Fetch for '%s' has content of 'application/json' type.", url)
            return await response.json()

        raise UnexpectedContentTypeError(
            f"Content type '{content_type}' was not expected."
        )


async def fetch_spotify(
    session: aiohttp.ClientSession, url: str, spotify_access_token: str, **kwargs: Any
) -> dict[str, Any]:
    """
    Asynchronously fetches data from the Spotify Web API.

    This function is a wrapper around a generic fetch function for making authenticated requests to
    the Spotify Web API. It sends an asynchronous HTTP GET request to the specified URL, using the
    provided Spotify access token for authorization.

    Parameters:
        session (aiohttp.ClientSession): The HTTP client session for making requests.
        url (str): The URL of the Spotify Web API endpoint to be accessed.
        spotify_access_token (str): The access token for authenticating with the Spotify Web API.
        **kwargs (Any): Additional keyword arguments to be passed to the underlying fetch function.

    Returns:
        dict[str, Any]: A dictionary containing the JSON response data from the Spotify Web API.

    Raises:
        aiohttp.ClientError: If an HTTP request to the Spotify Web API fails.

    Note:
        This function is specifically tailored for requests to the Spotify Web API, handling the
        necessary authorization headers. The caller is responsible for providing a valid access
        token and the correct Spotify Web API endpoint to access.
    """
    return await fetch(
        session,
        url,
        headers={"Authorization": f"Bearer {spotify_access_token}"},
        **kwargs,
    )


async def get_spotify_access_token(session: aiohttp.ClientSession) -> str:
    """
    Asynchronously obtains an access token from Spotify Web API for client credentials
    authorization flow.

    This function generates a Base64-encoded authorization string using the Spotify client ID and
    secret and then makes an asynchronous POST request to the Spotify Token API URL to obtain an
    access token. This token is required for making authenticated requests to the Spotify Web API.

    Parameters:
        session (aiohttp.ClientSession): The HTTP client session for making requests.

    Returns:
        str: An access token obtained from the Spotify Web API.

    Raises:
        aiohttp.ClientError: If an HTTP request to the Spotify Web API fails.

    Note:
        This function uses the 'client_credentials' grant type for authentication, which is suitable
        for server-to-server requests where user authorization is not required. The client ID and
        secret are expected to be set as environment variables or in a similar configuration.
    """
    # See: https://developer.spotify.com/documentation/web-api
    base64_encoded_authorization = base64.b64encode(
        f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}".encode()
    ).decode()
    async with session.post(
        SPOTIFY_TOKEN_API_URL,
        data={"grant_type": "client_credentials"},
        headers={"Authorization": f"Basic {base64_encoded_authorization}"},
    ) as response:
        data = await response.json()
        return data["access_token"]


def get_spotify_song_urls(html: str) -> list[str]:
    """
    Extracts a list of song URLs from the HTML content of a Spotify playlist page.

    This function parses the provided HTML content of a Spotify playlist page using BeautifulSoup.
    It searches for meta tags that contain URLs of individual songs. From the tag's
    content, this function extracts and returns the first 10 song URLs.

    Parameters:
        html (str): The HTML content of a Spotify playlist page.

    Returns:
        list[str]: A list of URLs extracted from the HTML content, each corresponding to a song on
            the Spotify playlist. The list is limited to the first 10 URLs found.

    Raises:
        AttributeError: If the HTML structure is different than expected and required elements are
            not found.

    Note:
        This function depends on the structure of the Spotify playlist page to locate the URLs.
        Changes in the website's structure may require updates to the parsing logic.
    """
    spotify_soup = BeautifulSoup(html, "html.parser")
    return [
        song["content"]
        for song in spotify_soup.find_all("meta", {"name": "music:song"}, limit=10)
    ]


def get_relevant_spotify_song_response_data(
    response: dict[str, Any],
) -> tuple[str, dict[str, Any]]:
    """
    Extracts relevant data from a Spotify Web API song response.

    This function processes a single song's response data from the Spotify Web API and extracts key
    information about the song. The extracted data includes the song's ISRC (International Standard
    Recording Code), artist information, duration in milliseconds, explicit content flag, Spotify
    song URL, and song name.

    Parameters:
        response (dict[str, Any]): A dictionary representing a single song's data as returned by the
            Spotify Web API.

    Returns:
        str: The ISRC extracted from the song response.
        dict[str, Any]: A dictionary containing the data of the corresponding song.

    Note:
        This function assumes that the response dictionary has a specific structure as provided by
        the Spotify Web API. Changes in the Spotify Web API could affect this function's output and
        may require updates to the request logic or data extraction process.
    """
    artist_data = response["artists"]
    return (
        response["external_ids"]["isrc"],
        {
            "artists": [
                {"artist_id": artist["id"], "artist_name": artist["name"]}
                for artist in artist_data
            ],
            "song_duration_ms": response["duration_ms"],
            "is_explicit": response["explicit"],
            "spotify_url": response["external_urls"]["spotify"],
            "song_name": response["name"],
        },
    )


async def get_spotify_song_data_from_spotify_song_urls(
    session: aiohttp.ClientSession, spotify_access_token: str, song_urls: list[str]
) -> tuple[list[str], dict[str, Any]]:
    """
    Asynchronously fetches Spotify song data for a list of Spotify song URLs.

    This function extracts song IDs for the provided Spotify song URLs and then fetches the song
    data of the songs from the extracted song IDs using the Spotify Web API. It utilizes Spotify's
    Web API capability to make one request for multiple tracks instead of separate requests for each
    track which is more efficient.

    Parameters:
        session (aiohttp.ClientSession): The HTTP client session for making requests.
        spotify_access_token (str): The access token for authenticating with the Spotify Web API.
        song_urls (list[str]): A list of Spotify song URLs for which to fetch song data.

    Returns:
        list[str]: A list of ISRCs corresponding to each provided Spotify song URL.
        dict[str, Any]: A dictionary where each key is an ISRC and each value is the corresponding
            song data from Spotify.

    Raises:
        aiohttp.ClientError: If an HTTP request to the Spotify Web API fails.
        KeyError: If the expected data structure in the Spotify Web API response is not present.

    Note:
        This function depends on the 'fetch_spotify' function to make individual API requests and
        'get_relevant_spotify_song_response_data' to extract relevant data from each response.
        Changes in the Spotify Web API could affect this function's output and may require updates
        to the request logic or data extraction process.
    """
    song_ids = [song_url.split("/").pop() for song_url in song_urls]
    # More efficient to make 1 request for 10 tracks than 10 requests for 1 track each
    data = await fetch_spotify(
        session,
        f"{SPOTIFY_BASE_API_URL}v1/tracks?ids={",".join(song_ids)}",
        spotify_access_token,
    )

    isrcs = []
    spotify_song_data = {}
    # The endpoint returns the tracks in order
    for song_response_data in data["tracks"]:
        isrc, song_data = get_relevant_spotify_song_response_data(song_response_data)
        isrcs.append(isrc)
        spotify_song_data[isrc] = song_data

    return isrcs, spotify_song_data


def get_apple_music_song_urls(html: str) -> list[str]:
    """
    Extracts a list of song URLs from the HTML content of an Apple Music playlist page.

    This function parses the provided HTML content of an Apple Music playlist page using
    BeautifulSoup. It searches for meta tags that contain URLs of individual songs. From the tag's
    content, this function extracts and returns the first 10 song URLs.

    Parameters:
        html (str): The HTML content of an Apple Music playlist page.

    Returns:
        list[str]: A list of URLs extracted from the HTML content, each corresponding to a song on
            the Apple Music playlist. The list is limited to the first 10 URLs found.

    Raises:
        AttributeError: If the HTML structure is different than expected and required elements are
            not found.

    Note:
        This function depends on the structure of the Apple Music playlist page to locate the URLs.
        Changes in the website's structure may require updates to the parsing logic.
    """
    apple_music_soup = BeautifulSoup(html, "html.parser")
    return [
        song["content"]
        for song in apple_music_soup.find_all(
            "meta", {"property": "music:song"}, limit=10
        )
    ]


def get_relevant_apple_music_song_data(html: str) -> tuple[str, dict[str, Any]]:
    """
    Extracts relevant data from the HTML content of an Apple Music song page.

    This function parses the provided HTML content of an Apple Music song page using BeautifulSoup.
    It searches for a script tag that contains song schema data in JSON format. The extracted data
    includes the artist information and song name.

    Parameters:
        html (str): The HTML content of an Apple Music song page.

    Returns:
        dict[str, Any]: A dictionary containing the data of the corresponding song.

    Raises:
        KeyError: If the expected data structure in the JSON is not found.
        JSONDecodeError: If the JSON parsing fails.
        AttributeError: If the HTML structure is different than expected and required elements are
            not found.

    Note:
        This function depends on the structure of the Apple Music song page to locate the relevant
        information. Changes in the website's structure may require updates to the parsing logic.
    """
    apple_music_soup = BeautifulSoup(html, "html.parser")
    song_schema_data = apple_music_soup.find(
        "script", {"id": "schema:song", "type": "application/ld+json"}
    )
    song_schema = json.loads(song_schema_data.text)

    return {
        "artists": {
            "artist_name": artist["name"] for artist in song_schema["audio"]["byArtist"]
        },
        "song_name": song_schema["name"],
    }


async def get_apple_music_song_data_from_apple_music_song_urls(
    session: aiohttp.ClientSession, spotify_access_token: str,  song_urls: list[str]
) -> tuple[list[str], dict[str, Any]]:
    """
    Asynchronously fetches Apple Music song data for a list of Apple Music song URLs.

    This function concurrently fetches the HTML content for the provided Apple Music song URLs and
    then extracts the song data of the songs from the fetched HTML content using BeautifulSoup. It
    then uses this data to concurrently search for the corresponding song on Spotify and fetch the
    song data from Spotify using the Spotify Web API.

    Parameters:
        session (aiohttp.ClientSession): The HTTP client session for making requests.
        spotify_access_token (str): The access token for authenticating with the Spotify Web API.
        song_urls (list[str]): A list of URLs for which to fetch ISRCs.

    Returns:
        list[str]: A list of ISRCs corresponding to each provided Apple Music song URL.
        dict[str, Any]: A dictionary where each key is an ISRC and each value is the corresponding
            song data from Apple Music.

    Raises:
        aiohttp.ClientError: If an HTTP request fails.
        KeyError: If the expected data structure in the Spotify Web API response is not present.

    Note:
        This function depends on the 'fetch_spotify' function to make individual API requests and
        'get_relevant_spotify_song_response_data' to extract relevant data from each response.
        Changes in the Spotify Web API could affect this function's output and may require updates
        to the request logic or data extraction process. This function also depends on the structure
        of the Apple Music song page to locate the song data. Changes in the website's structure may
        require updates to the parsing logic.
    """
    song_htmls = await asyncio.gather(
        *[fetch(session, song_url) for song_url in song_urls]
    )

    relevant_apple_music_song_data = []
    for song_html in song_htmls:
        relevant_apple_music_song_data.append(
            get_relevant_apple_music_song_data(song_html)
        )

    search_queries = [
        " ".join(
            [
                f"artist:{artist_name}"
                for artist_name in relevant_lookup_data["artists"].values()
            ]
        )
        + f" track:{relevant_lookup_data['song_name']}"
        for relevant_lookup_data in relevant_apple_music_song_data
    ]

    data = await asyncio.gather(
        *[
            fetch_spotify(
                session,
                f"{SPOTIFY_BASE_API_URL}v1/search?q={search_query}&type=track",
                spotify_access_token,
            )
            for search_query in search_queries
        ]
    )

    isrcs = []
    apple_music_song_data = {}
    for song_response_data in data:
        song_response_data_details = song_response_data["tracks"]["items"]

        if not song_response_data_details:
            raise SongDataNotFoundError(
                "Could not find any data on Spotify for song with ISRC."
            )

        isrc, song_data = get_relevant_spotify_song_response_data(
            song_response_data_details[0]
        )
        isrcs.append(isrc)
        apple_music_song_data[isrc] = song_data

    return isrcs, apple_music_song_data


# Parametrized queries to prevent SQL injection attacks. Postgres uses server-side argument binding
async def load_artist_data(
    connection: asyncpg.Connection, song_data: dict[str, Any]
) -> None:
    """
    Asynchronously loads artist data into the database and appends it to a CSV file.

    This function performs an INSERT operation into the 'artist_tb' table of the 'music_data'
    database schema. It inserts data about artists based on the provided song data. The data
    includes the artist's Spotify ID and name. In case of a conflict (e.g., duplicate entries),
    the insertion of the problematic row does nothing (ignores the conflict).

    After successfully inserting the data into the database, it appends the same data to a CSV file
    named 'artist.csv' for record-keeping. The CSV file is stored in the 'db' directory.

    Parameters:
        connection (asyncpg.Connection): The database connection object.
        song_data (dict[str, Any]): A dictionary containing song data, where the key is the ISRC
            and the value is a dictionary of song details.

    Returns:
        None: This function does not return any value but performs database and file operations.
    """
    successful_records = await connection.fetch(
        """
        INSERT INTO
            music_data.artist_tb (artist_id, artist_name) (
                SELECT
                    artist_id,
                    artist_name
                FROM
                    UNNEST($1::music_data.artist_tb[])
            )
        ON CONFLICT DO NOTHING
        RETURNING artist_id, artist_name;
        """,
        [
            (artist["artist_id"], artist["artist_name"])
            for song in song_data.values()
            for artist in song["artists"]
        ],
    )

    async with aiofiles.open(
        os.path.join(BASE_DIR, "db", "csv", "artist.csv"), "a"
    ) as file:
        for record in successful_records:
            logger.debug("Record %s succesfully inserted into artist table.", record)
            await file.write('"' + '","'.join(list(record.values())) + '"\n')


async def load_song_data(
    connection: asyncpg.Connection, song_data: dict[str, Any]
) -> None:
    """
    Asynchronously loads song data into the database and appends it to a CSV file.

    This function performs an INSERT operation into the 'song_tb' table of the 'music_data'
    database schema. It inserts data about songs based on the provided song data.
    The data includes the ISRC (International Standard Recording Code), song name, song duration
    in milliseconds, explicit content flag, Spotify URL, and Apple Music URL. In case of a conflict
    (e.g., duplicate entries), the insertion of the problematic row does nothing (ignores the
    conflict).

    Additionally, before performing the INSERT operation, this function checks for existing records
    in the database without an Apple Music URL

    After successfully inserting the data into the database, it appends the same data to a CSV file
    named 'song.csv'. The CSV file is stored in the 'db' directory.

    Parameters:
        connection (asyncpg.Connection): The database connection object.
        song_data (dict[str, Any]): A dictionary containing song data, where the key is the ISRC
            and the value is a dictionary of song details.

    Returns:
        None: This function does not return any value but performs database and file operations.
    """
    existing_records = await connection.fetch(
        """
        SELECT
            isrc,
            apple_music_url
        FROM
            music_data.song_tb
        WHERE
            isrc = ANY ($1::VARCHAR(12) [])
            AND apple_music_url IS NULL;
        """,
        list(song_data),
    )

    update_statement = await connection.prepare(
        """
        UPDATE music_data.song_tb
        SET
            apple_music_url = $1
        WHERE
            isrc = $2
        RETURNING *;
        """
    )

    async with aiofiles.open(
        os.path.join(BASE_DIR, "db", "csv", "song.csv"), "a"
    ) as file:
        for record in existing_records:
            isrc = record["isrc"]
            data = song_data[isrc]
            # Dictionaries in Python are implemented as hash tables and have O(1) lookup. Each
            # record should have an ISRC present in the data and no Apple Music URL in the database
            if "apple_music_url" in data:
                updated_row = await update_statement.fetchrow(
                    data["apple_music_url"], isrc
                )
                logger.debug(
                    "Record %s succesfully updated in song table to %s.",
                    record,
                    updated_row,
                )
                await file.write(
                    '"'
                    + '","'.join([str(value) for value in list(updated_row.values())])
                    + '"\n'
                )
                # Avoid inserting what is already in the database
                del song_data[isrc]

        successful_records = await connection.fetch(
            # pylint: disable=line-too-long
            """
            INSERT INTO
                music_data.song_tb (isrc, song_name, song_duration_ms, is_explicit, spotify_url, apple_music_url) (
                    SELECT
                        isrc,
                        song_name,
                        song_duration_ms,
                        is_explicit,
                        spotify_url,
                        apple_music_url
                    FROM
                        UNNEST($1::music_data.song_tb[])
                )
            ON CONFLICT DO NOTHING
            RETURNING isrc, song_name, song_duration_ms, is_explicit, spotify_url, apple_music_url;
            """,
            [
                (
                    isrc,
                    song["song_name"],
                    song["song_duration_ms"],
                    song["is_explicit"],
                    song["spotify_url"],
                    song.get("apple_music_url"),
                )
                for isrc, song in song_data.items()
            ],
        )

        for record in successful_records:
            logger.debug("Record %s succesfully inserted into song table.", record)
            # For CSV files, a "," with no data will be interpreted as a null value when loaded into
            # Postgres. An empty string ("") will make this happen
            await file.write(
                ",".join(
                    [
                        f'"{value}"' if value is not None else ""
                        for value in list(record.values())
                    ]
                )
                + "\n"
            )


async def load_artist_song_map_data(
    connection: asyncpg.Connection, song_data: dict[str, Any]
) -> None:
    """
    Asynchronously loads artist-song mapping data into the database and appends it to a CSV file.

    This function performs an INSERT operation into the 'artist_song_map_tb' table of the
    'music_data' database schema. It inserts mapping data between artists and songs based on the
    provided song data. The data includes the artist's Spotify ID and the ISRC (International
    Standard Recording Code) of the song. In case of a conflict (e.g., duplicate entries), the
    insertion of the problematic row does nothing (ignores the conflict).

    After successfully inserting the data into the database, it appends the same data to a CSV file
    named 'artist_song_map.csv'. The CSV file is stored in the 'db' directory.

    Parameters:
        connection (asyncpg.Connection): The database connection object.
        song_data (dict[str, Any]): A dictionary containing song data, where the key is the ISRC
            and the value is a dictionary of song details.

    Returns:
        None: This function does not return any value but performs database and file operations.
    """
    successful_records = await connection.fetch(
        """
        INSERT INTO
            music_data.artist_song_map_tb (artist_id, isrc) (
                SELECT
                    artist_id,
                    isrc
                FROM
                    UNNEST($1::music_data.artist_song_map_tb[])
            )
        ON CONFLICT DO NOTHING
        RETURNING artist_id, isrc;
        """,
        [
            (artist["artist_id"], isrc)
            for isrc, song in song_data.items()
            for artist in song["artists"]
        ],
    )

    async with aiofiles.open(
        os.path.join(BASE_DIR, "db", "csv", "artist_song_map.csv"), "a"
    ) as file:
        for record in successful_records:
            logger.debug(
                "Record %s succesfully inserted into artist song map table.",
                record,
            )
            await file.write('"' + '","'.join(list(record.values())) + '"\n')


async def load_ranking_data(connection: asyncpg.Connection, isrcs: list[str]) -> None:
    """
    Asynchronously loads ranking data into the database and appends it to a CSV file.

    This function performs an INSERT operation into the 'ranking_tb' table of the 'music_data'
    database schema. It inserts data about song rankings based on the provided ISRCs (International
    Standard Recording Codes). The data includes the ISRC, ranking date, rank, and ranking source.
    In case of a conflict (e.g., duplicate entries), the insertion of the problematic row does
    nothing (ignores the conflict).

    After successfully inserting the data into the database, it appends the same data to a CSV file
    named 'ranking.csv'. The CSV file is stored in the 'db' directory.

    Parameters:
        connection (asyncpg.Connection): The database connection object.
        isrcs (list[str]): A list of ISRCs for which the ranking data is to be loaded.

    Returns:
        None: This function does not return any value but performs database and file operations.

    Note:
        This function assumes the ISRCs are provided in a specific order - the first half
        corresponds to Spotify rankings and the second half to Apple Music. This ordering affects
        how the 'rank' and 'ranking_source' fields are determined.
    """
    successful_records = await connection.fetch(
        """
        INSERT INTO
            music_data.ranking_tb (isrc, ranking_date, rank, ranking_source) (
                SELECT
                    isrc,
                    ranking_date,
                    rank,
                    ranking_source
                FROM
                    UNNEST($1::music_data.ranking_tb[])
            )
        ON CONFLICT DO NOTHING
        RETURNING isrc, ranking_date, rank, ranking_source;
        """,
        [
            (
                None,
                isrc,
                datetime.datetime.now(datetime.UTC).date(),
                # The former half are from Spotify and the latter half are from Apple Music due to
                # the concatenation of the data
                i + 1 if i < 10 else i - 9,
                "Spotify" if i < 10 else "Apple Music",
            )
            for i, isrc in enumerate(isrcs)
        ],
    )

    async with aiofiles.open(
        os.path.join(BASE_DIR, "db", "csv", "ranking.csv"), "a"
    ) as file:
        for record in successful_records:
            logger.debug("Record %s succesfully inserted into ranking table.", record)
            await file.write(
                '"'
                + '","'.join(
                    [
                        (
                            value.isoformat()
                            if isinstance(value, datetime.date)
                            else str(value)
                        )
                        for value in list(record.values())
                    ]
                )
                + '"\n'
            )


async def main() -> None:
    """
    Main asynchronous entry point of the script.

    This function orchestrates the overall workflow of the script. It performs tasks such as
    fetching Spotify and Apple Music playlist data, extracting song URLs, fetching song details
    from Spotify based on URLs and ISRCs, and finally loading the extracted data into a database
    and CSV files. It handles these operations using asynchronous I/O for efficiency.

    This function performs the following major steps:

    1. Establishes an asynchronous HTTP client session and a connection pool to the database.
    2. Fetches Spotify access token and the HTML content of Spotify and Apple Music playlists.
    3. Extracts the URLs and ISRCs of the top 10 songs from both Spotify and Apple Music playlists.
    4. Fetches song data from Spotify using the extracted URLs and ISRCs.
    5. Merges Spotify song data of both Spotify and Apple Music songs.
    6. Loads the data into the database and CSV files.

    Returns:
        None: This function does not return any value.

    Raises:
        SongDataNotFoundError: If there is missing data needed to perform the ranking.
    """
    async with (
        aiohttp.ClientSession() as session,
        asyncpg.create_pool(
            # 4 SQL queries are needed in total, but they depend on the first 2 due to foreign key
            # constraints, so only 2 connections are needed at any given moment to perform 2 SQL
            # queries concurrently
            min_size=2,
            max_size=2,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        ) as pool,
    ):
        logger.info(
            "Fetching Spotify access token and HTML for Spotify and Apply Music playlists."
        )
        logger.debug("SPOTIFY_PLAYLIST_URL: '%s'.", SPOTIFY_PLAYLIST_URL)
        logger.debug("APPLE_MUSIC_PLAYLIST_URL: '%s'.", APPLE_MUSIC_PLAYLIST_URL)

        # Web scraping is required as the Spotify Web API does not have an endpoint for the required
        # data and making requests to the Apple Music API (and MusicKit) is locked behind a paywall
        # as you need to be a Apple Developer Program member. See:
        # https://developer.apple.com/documentation/applemusicapi/generating_developer_tokens
        spotify_access_token, spotify_html, apple_music_html = await asyncio.gather(
            get_spotify_access_token(session),
            fetch(session, SPOTIFY_PLAYLIST_URL),
            fetch(session, APPLE_MUSIC_PLAYLIST_URL),
        )

        logger.info("Extracting URLs of top 10 Spotify and Apple Music songs.")

        spotify_song_urls = get_spotify_song_urls(spotify_html)
        apple_music_song_urls = get_apple_music_song_urls(apple_music_html)

        logger.debug(
            "URLs of top 10 Spotify songs:\n - %s", "\n - ".join(spotify_song_urls)
        )
        logger.debug(
            "URLs of top 10 Apple Music songs:\n - %s",
            "\n - ".join(apple_music_song_urls),
        )

        logger.info(
            "Fetching Spotify song data from Spotify song URLs and ISRCs from Apple Music song "
            "URLs."
        )

        spotify_song_data, apple_music_song_data = await asyncio.gather(
            get_spotify_song_data_from_spotify_song_urls(
                session,
                spotify_access_token,
                spotify_song_urls,
            ),
            # Spotify song data is not immediately available; needs common identifier (ISRC) to
            # lookup Apple Music song in Spotify database to then retrieve the relevant data
            get_apple_music_song_data_from_apple_music_song_urls(
                session, spotify_access_token, apple_music_song_urls
            ),
        )

        spotify_song_isrcs, spotify_song_data = spotify_song_data
        apple_music_song_isrcs, apple_music_song_data = apple_music_song_data

        if len(spotify_song_isrcs) != 10 or len(apple_music_song_isrcs) != 10:
            raise SongDataNotFoundError("Ranking data is missing.")

        logger.debug(
            "ISRCs of top 10 Spotify songs:\n - %s", "\n - ".join(spotify_song_isrcs)
        )
        logger.debug(
            "ISRCs of top 10 Apple Music songs:\n - %s",
            "\n - ".join(apple_music_song_isrcs),
        )

        logger.info(
            "Getting union of Spotify song data of Spotify and Apple Music songs."
        )

        # Technically prior to merging and on Python 3.7 or higher, the keys of both dictionaries
        # are in order of rank (1-10)
        song_data = spotify_song_data | apple_music_song_data

        logger.info("Adding Apple Music URL data to existing song data.")

        # `apple_music_song_isrcs` and `apple_music_song_urls` are both in 1-10 order
        for i, apple_music_song_isrc in enumerate(apple_music_song_isrcs):
            song_data[apple_music_song_isrc]["apple_music_url"] = apple_music_song_urls[
                i
            ]

        logger.debug("Final song data:\n\n%s\n", json.dumps(song_data, indent=4))

        async with pool.acquire() as con1, pool.acquire() as con2:
            logger.info(
                "Loading artist and song data into their respective tables and CSV files."
            )
            await asyncio.gather(
                load_artist_data(con1, song_data),
                load_song_data(con2, song_data),
            )

            logger.info(
                "Loading artist song map and ranking data into their respective tables and CSV "
                "files."
            )
            await asyncio.gather(
                load_artist_song_map_data(con1, song_data),
                # ISRCs are in 1-10 order. After concatenation, former are 1-10 from Spotify and
                # latter are 1-10 from Apple Music
                load_ranking_data(con2, spotify_song_isrcs + apple_music_song_isrcs),
            )

        logger.info("Script finished successfully.")


if __name__ == "__main__":
    major, minor, *_ = sys.version_info
    if (major, minor) < (3, 9):
        raise UnsupportedPythonVersionError("Please upgrade to Python 3.9 or higher.")

    logger.info("Script starting.")

    asyncio.run(main())
