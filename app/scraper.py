import json
import logging
import os
import re
from datetime import datetime, time
from time import sleep
from typing import Any, Final, Literal, TypedDict, cast
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import pymongo
import pytz
import requests
import schedule
from prometheus_client import Counter, start_http_server
from pythonjsonlogger import jsonlogger

# Configure logging for Docker with JSON format for Loki
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
handler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter()
handler.setFormatter(formatter)
logging.getLogger().addHandler(handler)
logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)

PeriodName = Literal["AM", "PM", "NIGHT"]


class PeriodData(TypedDict):
    data: list[dict[str, int]]
    startValidity: str | None
    endValidity: str | None


class RawData(TypedDict):
    data: list[dict[str, Any]]


class ParsedResults(TypedDict):
    AM: PeriodData
    PM: PeriodData
    NIGHT: PeriodData
    RAW: RawData
    datetime: str


ICAO_CODES: Final[list[str]] = [
    code.strip().upper()
    for code in os.getenv("ICAO_CODES", "CYYU").split(",")
    if code.strip()
]
MONGO_URI: Final[str] = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_CONNECTION_TIMEOUT: Final[int] = int(
    os.getenv("MONGO_CONNECTION_TIMEOUT", "5000")
)
COLLECTION_NAME: Final[str] = "upper_winds"
NAV_CANADA_URL_TEMPLATE: Final[str] = (
    "https://plan.navcanada.ca/weather/api/alpha/?site={icao_code}&alpha=upperwind"
)
REQUEST_TIMEOUT_SECONDS: Final[int] = 10
MONTREAL_TZ: Final[ZoneInfo] = ZoneInfo("America/Montreal")
PERIOD_ORDER: Final[tuple[PeriodName, ...]] = ("AM", "PM", "NIGHT")

ICAO_PATTERN: Final[re.Pattern[str]] = re.compile(r'^[A-Z]{4}$')

# Prometheus metrics
fetch_success = Counter('navcanada_fetch_success_total', 'Number of successful fetches', ['icao'])
fetch_failure = Counter('navcanada_fetch_failure_total', 'Number of failed fetches', ['icao'])
parse_success = Counter('navcanada_parse_success_total', 'Number of successful parses', ['icao'])
parse_failure = Counter('navcanada_parse_failure_total', 'Number of failed parses', ['icao'])
store_success = Counter('navcanada_store_success_total', 'Number of successful stores', ['icao'])
store_failure = Counter('navcanada_store_failure_total', 'Number of failed stores', ['icao'])
mongo_connection_failure = Counter('navcanada_mongo_connection_failure_total', 'Number of MongoDB connection failures')


def sanitize_mongo_uri(uri: str) -> str:
    """Mask credentials before logging a MongoDB URI."""
    parts = urlsplit(uri)
    if not parts.password:
        return uri

    netloc = parts.netloc.replace(parts.password, "***", 1)
    return parts._replace(netloc=netloc).geturl()


def validate_icao_code(icao_code: str) -> bool:
    """Validate ICAO code format (4 uppercase letters)."""
    return bool(ICAO_PATTERN.match(icao_code))


def validate_api_endpoint(icao_code: str) -> bool:
    """Validate API endpoint by testing a HEAD request."""
    api_url = NAV_CANADA_URL_TEMPLATE.format(icao_code=icao_code)
    try:
        response = requests.head(api_url, timeout=5)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False


def get_mongo_client() -> pymongo.MongoClient[dict[str, Any]]:
    """
    Return a connected MongoClient.
    Raises immediately if connection fails.
    """
    try:
        client = pymongo.MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=MONGO_CONNECTION_TIMEOUT,
            connectTimeoutMS=MONGO_CONNECTION_TIMEOUT,
            retryWrites=False,
        )
        client.admin.command("ping")
        logger.info("Connected to MongoDB")
        return client
    except pymongo.errors.PyMongoError as e:
        logger.error(
            "MongoDB connection failed: %s - %s: %s",
            sanitize_mongo_uri(MONGO_URI),
            type(e).__name__,
            e,
        )
        raise


def fetch_upper_winds(icao_code: str) -> dict[str, Any] | None:
    """Fetch upper winds data from the Nav Canada API for a specific ICAO code."""
    api_url = NAV_CANADA_URL_TEMPLATE.format(icao_code=icao_code)
    try:
        response = requests.get(api_url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            logger.error(
                "Unexpected response type for %s: expected JSON object, got %s",
                icao_code,
                type(payload).__name__,
            )
            fetch_failure.labels(icao=icao_code).inc()
            return None
        fetch_success.labels(icao=icao_code).inc()
        return cast(dict[str, Any], payload)
    except ValueError as e:
        logger.error("Invalid JSON payload for %s: %s", icao_code, e)
        fetch_failure.labels(icao=icao_code).inc()
    except requests.exceptions.RequestException as e:
        logger.error("Error fetching data for %s: %s", icao_code, e)
        fetch_failure.labels(icao=icao_code).inc()
    return None


def get_period(start: str) -> PeriodName | None:
    """Determine the forecast period from an ISO timestamp."""
    start_datetime = datetime.fromisoformat(start)
    if start_datetime.tzinfo is not None:
        start_datetime = start_datetime.astimezone(MONTREAL_TZ)

    start_time = start_datetime.timetz().replace(tzinfo=None)

    if time(5, 0) <= start_time < time(9, 0):
        return "AM"
    if time(9, 0) <= start_time < time(18, 0):
        return "PM"
    if time(18, 0) <= start_time or start_time < time(5, 0):
        return "NIGHT"
    return None


def parse_wind_data(raw_text: str) -> list[dict[str, int]]:
    """Parse a Nav Canada wind payload into normalized rows."""
    decoded = json.loads(raw_text)
    if not isinstance(decoded, list) or not decoded:
        raise ValueError("text field did not contain a non-empty JSON list")

    wind_rows = decoded[-1]
    if not isinstance(wind_rows, list):
        raise ValueError("text field final element was not a list of wind rows")

    normalized_rows: list[dict[str, int]] = []
    for item in wind_rows:
        if not isinstance(item, list) or len(item) < 4:
            raise ValueError(f"invalid wind row structure: {item!r}")

        altitude, heading, wind, temperature = item[:4]
        if not isinstance(altitude, int):
            raise ValueError(f"invalid altitude value: {altitude!r}")
        if heading is not None and not isinstance(heading, int):
            raise ValueError(f"invalid heading value: {heading!r}")
        if wind is not None and not isinstance(wind, int):
            raise ValueError(f"invalid wind value: {wind!r}")
        if temperature is not None and not isinstance(temperature, int):
            raise ValueError(f"invalid temperature value: {temperature!r}")

        normalized_rows.append(
            {
                "altitude": altitude,
                "heading": heading if heading is not None else 0,
                "wind": wind if wind is not None else 0,
                "temperature": temperature if temperature is not None else 0,
            }
        )

    return normalized_rows


def parse_data(data: dict[str, Any]) -> ParsedResults:
    """Parse and filter data for AM, PM and NIGHT periods"""
    parsed_results: ParsedResults = {
        "AM": {"data": [], "startValidity": None, "endValidity": None},
        "PM": {"data": [], "startValidity": None, "endValidity": None},
        "NIGHT": {"data": [], "startValidity": None, "endValidity": None},
        "RAW": {"data": [data]},
        "datetime": datetime.now().isoformat(),
    }

    entries = data.get("data", [])
    if not isinstance(entries, list):
        logger.warning("Skipping payload with non-list data field: %r", entries)
        return parsed_results

    for entry in entries:
        if not isinstance(entry, dict):
            logger.warning("Skipping malformed forecast entry: %r", entry)
            continue

        start_validity = entry.get("startValidity")
        end_validity = entry.get("endValidity")
        raw_text = entry.get("text")
        if not isinstance(start_validity, str) or not isinstance(end_validity, str):
            logger.warning("Skipping entry with missing validity fields: %r", entry)
            continue
        if not isinstance(raw_text, str):
            logger.warning("Skipping entry with non-string text field: %r", entry)
            continue

        try:
            period = get_period(start_validity)
            if period is None:
                logger.warning(
                    "Skipping entry with unsupported period for startValidity=%s",
                    start_validity,
                )
                continue

            wind_data = parse_wind_data(raw_text)
        except (ValueError, TypeError) as e:
            logger.warning(
                "Skipping malformed wind payload for startValidity=%s: %s",
                start_validity,
                e,
            )
            continue

        parsed_results[period]["startValidity"] = start_validity
        parsed_results[period]["endValidity"] = end_validity
        parsed_results[period]["data"].extend(wind_data)

    for period in PERIOD_ORDER:
        parsed_results[period]["data"] = sorted(
            parsed_results[period]["data"], key=lambda x: x["altitude"]
        )

    return parsed_results


def store_data(
    client: pymongo.MongoClient[dict[str, Any]],
    data: ParsedResults,
    icao_code: str,
) -> None:
    """Store the parsed data into MongoDB."""
    try:
        db = client[icao_code]
        collection = db[COLLECTION_NAME]
        collection.insert_one(data)
        logger.info("Data for %s stored in MongoDB successfully.", icao_code)
        store_success.labels(icao=icao_code).inc()
    except pymongo.errors.ConnectionFailure as e:
        logger.error("Connection failure storing data for %s: %s", icao_code, e)
        store_failure.labels(icao=icao_code).inc()
    except pymongo.errors.OperationFailure as e:
        logger.error("Operation failure storing data for %s: %s", icao_code, e)
        store_failure.labels(icao=icao_code).inc()


def main() -> None:
    if not ICAO_CODES:
        logger.warning("No ICAO codes configured; skipping scrape run.")
        return

    logger.info("Starting upper winds data scraping at %s...", datetime.now(tz=pytz.timezone("America/Montreal")).isoformat())
    try:
        client = get_mongo_client()
    except pymongo.errors.PyMongoError:
        logger.error("Scrape run aborted because MongoDB is unavailable.")
        mongo_connection_failure.inc()
        return

    try:
        for icao_code in ICAO_CODES:
            logger.info("Fetching data for %s", icao_code)
            data = fetch_upper_winds(icao_code)
            if data is None:
                logger.warning("No data fetched for %s.", icao_code)
                continue

            try:
                results = parse_data(data)
                parse_success.labels(icao=icao_code).inc()
            except Exception:
                logger.exception("Unexpected error parsing data for %s", icao_code)
                parse_failure.labels(icao=icao_code).inc()
                continue

            logger.info("Fetched and parsed data for %s", icao_code)
            logger.debug(json.dumps(results, indent=4))
            store_data(client, results, icao_code)
    finally:
        client.close()


if __name__ == "__main__":
    # Validate configuration
    invalid_icaos = [code for code in ICAO_CODES if not validate_icao_code(code)]
    if invalid_icaos:
        logger.error("Invalid ICAO codes: %s", ", ".join(invalid_icaos))
        exit(1)

    if ICAO_CODES and not validate_api_endpoint(ICAO_CODES[0]):
        logger.error("API endpoint validation failed for %s", ICAO_CODES[0])
        exit(1)

    logger.info("Scraper started. ICAO codes: %s", ", ".join(ICAO_CODES))
    logger.info("MongoDB URI: %s", sanitize_mongo_uri(MONGO_URI))

    # Start Prometheus metrics server
    start_http_server(8000)
    logger.info("Prometheus metrics server started on port 8000")

    schedule.every().day.at("02:00", "America/Montreal").do(main)  # 00Z buffer
    schedule.every().day.at("08:00", "America/Montreal").do(main)  # 12Z buffer
    schedule.every().day.at("14:00", "America/Montreal").do(main)  # 18Z buffer
    schedule.every().day.at("20:00", "America/Montreal").do(main)  # 06Z buffer

    logger.info("Scheduler initialized. Waiting for scheduled times...")
    while True:
        schedule.run_pending()
        sleep(1)