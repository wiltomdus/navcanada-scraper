import json
import os
from datetime import datetime, time
from time import sleep

import requests
import schedule
import pymongo

# Configuration
ICAO_CODES = os.getenv("ICAO_CODES", "CYYU").split(",")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
DATABASE_NAME = "navcanada"
COLLECTION_NAME = "upper_winds"


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
        print(f"Error fetching data for {icao_code}: {e}")
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
        client = pymongo.MongoClient(MONGO_URI)
        db = client[icao_code]  # Use ICAO code as the database name
        collection = db[COLLECTION_NAME]

        collection.insert_one(data)
        print(f"Data for {icao_code} stored in MongoDB.")
    except pymongo.errors.ConnectionFailure as e:
        print(f"Connection failure storing data for {icao_code}: {e}")
    except pymongo.errors.OperationFailure as e:
        print(f"Operation failure storing data for {icao_code}: {e}")


def main():
    print("Starting upper winds data scraper...")
    for icao_code in ICAO_CODES:
        data = fetch_upper_winds(icao_code)
        if data:
            results = parse_data(data)
            print(json.dumps(results, indent=4))

            store_data(results, icao_code)
        else:
            print(f"No data fetched for {icao_code}.")


if __name__ == "__main__":
    schedule.every().day.at("06:00", "America/Montreal").do(main)

    while True:
        schedule.run_pending()
        sleep(1)
