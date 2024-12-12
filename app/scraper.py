import json
import os
from datetime import datetime, time
from time import sleep

import requests
import schedule
from pymongo import MongoClient

# Configuration
API_URL = "https://plan.navcanada.ca/weather/api/alpha/?site=CYYU&alpha=upperwind"

# Altitude categories
HIGH_ALTITUDES = {24000, 30000, 34000, 39000, 45000, 53000}
LOW_ALTITUDES = {3000, 6000, 9000, 12000, 18000}

# MongoDB
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
DATABASE_NAME = "navcanada"
COLLECTION_NAME = "upper_winds"


def fetch_upper_winds():
    """Fetch upper winds data from the Nav Canada API."""
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        data = response.json()
        return data
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None


def is_am_data(start, end):
    """Check if the time range falls within the AM period (3am to noon)."""
    start_time = datetime.fromisoformat(start).time()
    end_time = datetime.fromisoformat(end).time()
    return time(3, 0) <= start_time <= time(11, 59)


def is_pm_data(start, end):
    """Check if the time range falls within the PM period (noon to midnight)."""
    start_time = datetime.fromisoformat(start).time()
    end_time = datetime.fromisoformat(end).time()
    return time(12, 0) <= start_time <= time(23, 59)


def parse_data(data):
    """Parse and filter data for AM and PM periods, combining altitudes into a single list."""
    parsed_results = {"AM": [], "PM": []}
    parsed_results["datetime"] = datetime.now().isoformat()

    for entry in data.get("data", []):
        start_validity = entry["startValidity"]
        end_validity = entry["endValidity"]

        # Extract wind data from the text field
        wind_data = json.loads(entry["text"])[-1]

        # Determine if the entry belongs to AM or PM
        if is_am_data(start_validity, end_validity):
            period = "AM"
        elif is_pm_data(start_validity, end_validity):
            period = "PM"
        else:
            continue

        # Combine wind data into a single list for the period
        for item in wind_data:
            altitude, heading, wind, temperature, _ = item
            parsed_results[period].append(
                {
                    "altitude": altitude,
                    "heading": heading if heading is not None else 0,
                    "wind": wind if wind is not None else 0,
                    "temperature": temperature if temperature is not None else 0,
                }
            )

    # Sort the results by altitude for each period
    for period in parsed_results:
        if period != "datetime":
            parsed_results[period] = sorted(
                parsed_results[period], key=lambda x: x["altitude"]
            )

    return parsed_results


def store_data(data):
    """Store the parsed data into MongoDB."""
    try:
        client = MongoClient(MONGO_URI)
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]

        collection.insert_one(data)
    except Exception as e:
        print(f"Error storing data: {e}")


def main():

    print("Starting upper winds data scraper...")
    data = fetch_upper_winds()
    if data:
        results = parse_data(data)
        print(json.dumps(results, indent=4))

        store_data(results)
        print("Data stored in MongoDB.")
    else:
        print("No data fetched.")


if __name__ == "__main__":
    schedule.every().day.at("20:30", "America/Montreal").do(main)

    while True:
        schedule.run_pending()
        sleep(1)
