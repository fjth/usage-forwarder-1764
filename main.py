import os
import requests
import json
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import argparse

load_dotenv()

parser = argparse.ArgumentParser(description="Fetch and forward power usage data")
parser.add_argument(
    "--backfill-days", type=int, default=0,
    help="Number of days to backfill (0 = only yesterday)"
)
args = parser.parse_args()

# Project ID for Blockbax API
PROJECT_ID = os.getenv("BLOCKBAX_PROJECT_ID")

# Load secrets from environment
CLIENT_ID = os.getenv("HETMEETBEDRIJF_CLIENT_ID")
CLIENT_SECRET = os.getenv("HETMEETBEDRIJF_CLIENT_SECRET")
TOKEN_URL = os.getenv("HETMEETBEDRIJF_TOKEN_URL")
BLOCKBAX_API_KEY = os.getenv("BLOCKBAX_API_KEY")
BLOCKBAX_URL = os.getenv("BLOCKBAX_URL")


def get_yesterday():
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")


def get_access_token():
    payload = {
        "grant_type": "API",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    headers = {
        "Content-Type": "application/json-patch+json",
        "accept": "*/*"
    }
    response = requests.post(TOKEN_URL, json=payload, headers=headers)
    try:
        response.raise_for_status()
        token_data = response.json()
        token = token_data.get("token")
        if not token:
            raise ValueError("No token found in response.")
        return token
    except Exception as e:
        print(f"Failed to authenticate: HTTP {response.status_code}")
        raise e


def fetch_power_usage(token, date_str=None):
    # Determine the target date (YYYYMMDD)
    if not date_str:
        date = datetime.now(timezone.utc) - timedelta(days=1)
        date_str = date.strftime("%Y%m%d")
    
    common_headers = {
        "Authorization": f"Bearer {token}"
    }
    meters_headers = {**common_headers, "accept": "text/plain"}
    data_headers = {**common_headers, "accept": "text/plain"}

    # Step 1: Get all meters
    meters_url = "https://api.hetmeetbedrijf.nl/uwmeetdata/api/Meter/MyMeters"
    meters_response = requests.get(meters_url, headers=meters_headers)
    meters_response.raise_for_status()
    try:
        meters_data = meters_response.json()
        # Expect a dict with a "meters" list
        if isinstance(meters_data, dict) and "meters" in meters_data and isinstance(meters_data["meters"], list):
            meter_ids = [m["id"] for m in meters_data["meters"] if isinstance(m, dict) and "id" in m]
            # Deduplicate meter IDs
            seen = set()
            unique_meter_ids = []
            for mid in meter_ids:
                if mid not in seen:
                    seen.add(mid)
                    unique_meter_ids.append(mid)
            meter_ids = unique_meter_ids
        else:
            raise ValueError(f"Unexpected meters data format: {meters_data}")
    except Exception as e:
        print(f"Failed to parse meters data: HTTP {meters_response.status_code}")
        raise e

    # Step 2: Fetch data using GET for each meter
    all_data = []
    for meter_id in meter_ids:
        url = (
            "https://api.hetmeetbedrijf.nl/uwmeetdata/api/Data/GetDataRaw"
            f"?meterID={meter_id}"
            f"&channel=0"
            f"&date={date_str}"
            f"&rawInterval=true"
            f"&companyId=0"
            f"&inUTC=true"
        )
        response = requests.get(url, headers=data_headers)
        try:
            response.raise_for_status()
            raw_data = json.loads(response.text)
            all_data.append(raw_data)
        except Exception as e:
            print(f"Failed to fetch data for meter {meter_id}: HTTP {response.status_code}")
            raise e

    return all_data


def forward_to_blockbax(data):
    # Forward raw list payload as-is
    response = requests.post(
        BLOCKBAX_URL,
        json=data,
        headers={"Authorization": f"ApiKey {BLOCKBAX_API_KEY}"}
    )
    response.raise_for_status()


# Helper to check if yesterday's data is already in Blockbax
def check_run_yesterday():
    """Return True if any measurement for yesterday already exists in Blockbax."""
    # 1. Search all subjects in the project
    subjects_url = f"https://api.blockbax.com/v1/projects/{PROJECT_ID}/subjects"
    resp = requests.get(subjects_url,
                         json={}, 
                         headers={"Authorization": f"ApiKey {BLOCKBAX_API_KEY}"})
    resp.raise_for_status()
    subjects = resp.json().get("result", [])
    subject_ids = [s["id"] for s in subjects if "id" in s]

    # 2. Search measurements with toDate = yesterday at 23:59:59 UTC
    yesterday_end = (datetime.now(timezone.utc) - timedelta(days=1)).replace(
        hour=23, minute=59, second=59, microsecond=0
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    measurements_url = f"https://api.blockbax.com/v1/projects/{PROJECT_ID}/measurements"
    payload = {
        "subjectIds": subject_ids,
        "toDate": yesterday_end,
        "take": 1
    }
    resp2 = requests.post(measurements_url,
                          json=payload,
                          headers={"Authorization": f"ApiKey {BLOCKBAX_API_KEY}"})
    resp2.raise_for_status()
    return bool(resp2.json().get("result"))


def main():
    try:
        # Skip if yesterday's data already ingested
        if check_run_yesterday():
            print("Yesterday's data already submitted; skipping.")
            return
        else:
            print("No existing measurements for yesterday; proceeding with fetch and send.")
        print("Authenticating with HetMeetbedrijf...")
        token = get_access_token()
        if args.backfill_days > 0:
            print(f"Backfilling last {args.backfill_days} days...")
            for days_ago in range(args.backfill_days, 0, -1):
                date = datetime.now(timezone.utc) - timedelta(days=days_ago)
                date_str = date.strftime("%Y%m%d")
                print(f"Fetching power usage for {date_str}...")
                data = fetch_power_usage(token, date_str)
                print(f"Sending data for {date_str} to Blockbax...")
                forward_to_blockbax(data)
            print("Backfill complete.")
            return
        print("Fetching power usage for yesterday...")
        data = fetch_power_usage(token)
        print("Sending data to Blockbax...")
        forward_to_blockbax(data)
        print("Done.")
    except Exception as e:
        print(f"Error occurred: {e}")


if __name__ == "__main__":
    main()