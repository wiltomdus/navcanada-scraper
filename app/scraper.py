import json
import logging
import os
from datetime import datetime, time
from time import sleep

import requests
import schedule
import pymongo

# Configure logging for Docker
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Configuration
ICAO_CODES = os.getenv("ICAO_CODES", "CYYU").split(",")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_CONNECTION_TIMEOUT = int(os.getenv("MONGO_CONNECTION_TIMEOUT", "5000"))
DATABASE_NAME = "navcanada"
COLLECTION_NAME = "upper_winds"


def get_mongo_client():
    """
    Return a connected MongoClient.
    Raises immediately if connection fails.
    """
    try:
        client = pymongo.MongoClient(
            MONGO_URI, 
            serverSelectionTimeoutMS=MONGO_CONNECTION_TIMEOUT,
            connectTimeoutMS=MONGO_CONNECTION_TIMEOUT,
            retryWrites=False
        )
        client.admin.command("ping")
        logger.info("Connected to MongoDB")
        return client
    except pymongo.errors.PyMongoError as e:
        logger.error(f"MongoDB connection failed: {MONGO_URI} - {type(e).__name__}: {e}")
        raise


def fetch_upper_winds(icao_code) -> json:
    """Fetch upper winds data from the Nav Canada API for a specific ICAO code."""
    api_url = (
        f"https://plan.navcanada.ca/weather/api/alpha/?site={icao_code}&alpha=upperwind"
    )
    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data for {icao_code}: {e}")
        return None


def get_period(start) -> str:
    """Determine the period (AM, PM, NIGHT) based on the start and end times."""
    start_time = datetime.fromisoformat(start).time()

    if time(5, 0) <= start_time < time(9, 0):
        return "AM"
    if time(9, 0) <= start_time < time(18, 0):
        return "PM"
    if time(18, 0) <= start_time or start_time < time(5, 0):
        return "NIGHT"
    return None


def parse_data(data) -> json:
    """Parse and filter data for AM, PM and NIGHT periods"""
    parsed_results = {
        "AM": {"data": [], "startValidity": None, "endValidity": None},
        "PM": {"data": [], "startValidity": None, "endValidity": None},
        "NIGHT": {"data": [], "startValidity": None, "endValidity": None},
        "RAW": {"data": [data]},
        "datetime": datetime.now().isoformat(),
    }

    for entry in data.get("data", []):
        start_validity = entry["startValidity"]
        end_validity = entry["endValidity"]

        # Extract wind data from the text field
        wind_data = json.loads(entry["text"])[-1]

        # Determine the period
        period = get_period(start_validity)
        if period is None:
            continue

        # Set the start and end validity for the period
        parsed_results[period]["startValidity"] = start_validity
        parsed_results[period]["endValidity"] = end_validity

        # Combine wind data into a single list for the period
        for item in wind_data:
            altitude, heading, wind, temperature, _ = item
            parsed_results[period]["data"].append(
                {
                    "altitude": altitude,
                    "heading": heading if heading is not None else 0,
                    "wind": wind if wind is not None else 0,
                    "temperature": temperature if temperature is not None else 0,
                }
            )

    # Sort the results by altitude for each period
    for period in ["AM", "PM", "NIGHT"]:
        parsed_results[period]["data"] = sorted(
            parsed_results[period]["data"], key=lambda x: x["altitude"]
        )

    return parsed_results


def store_data(data, icao_code) -> None:
    """Store the parsed data into MongoDB."""
    try:
        client = get_mongo_client()
        db = client[icao_code]  # Use ICAO code as the database name
        collection = db[COLLECTION_NAME]

        # collection.insert_one(data)
        logger.info(f"Data for {icao_code} stored in MongoDB successfully.")
    except pymongo.errors.ConnectionFailure as e:
        logger.error(f"Connection failure storing data for {icao_code}: {e}")
    except pymongo.errors.OperationFailure as e:
        logger.error(f"Operation failure storing data for {icao_code}: {e}")


def main():
    logger.info("Starting upper winds data scraper...")
    for icao_code in ICAO_CODES:
        logger.info(f"Fetching data for {icao_code}")
        data = fetch_upper_winds(icao_code)
        if data:
            results = parse_data(data)
            logger.info(f"Fetched and parsed data for {icao_code}")
            logger.debug(json.dumps(results, indent=4))

            store_data(results, icao_code)
        else:
            logger.warning(f"No data fetched for {icao_code}.")


if __name__ == "__main__":
    logger.info(f"Scraper started. ICAO codes: {', '.join(ICAO_CODES)}")
    logger.info(f"MongoDB URI: {MONGO_URI}")
    
    schedule.every().day.at("02:00", "America/Montreal").do(main)  # 00Z buffer
    schedule.every().day.at("08:00", "America/Montreal").do(main)  # 12Z buffer
    schedule.every().day.at("14:00", "America/Montreal").do(main)  # 18Z buffer
    schedule.every().day.at("20:00", "America/Montreal").do(main)  # 06Z buffer
    
    logger.info("Scheduler initialized. Waiting for scheduled times...")
    while True:
        schedule.run_pending()
        sleep(1)


