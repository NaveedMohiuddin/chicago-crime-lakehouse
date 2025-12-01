import os
import json
import hashlib
import boto3
import requests
from datetime import datetime, timedelta, timezone

FIREHOSE_NAME = os.environ["FIREHOSE_NAME"]
SOCRATA_URL = os.environ["SOCRATA_URL"]
APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")

firehose = boto3.client("firehose")

def dedupe_key(rec: dict) -> str:
    """Generate unique key for deduplication"""
    base = f"{rec.get('id', '')}-{rec.get('date', '')}"
    return hashlib.md5(base.encode()).hexdigest()

def chunk_list(lst, chunk_size):
    """Split list into chunks"""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def lambda_handler(event, context):
    """
    Pulls Chicago crimes from Socrata API
    and sends to Kinesis Firehose in batches
    """
    print(f"Starting crimes data pull at {datetime.now(timezone.utc).isoformat()}")
    
    # Calculate time window (8-30 days ago to account for 7-day lag)
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(days=30)  # Pull last 30 days
    
    # Format date for Socrata API
    window_start_str = window_start.strftime('%Y-%m-%dT%H:%M:%S')
    
    # Query parameters for Socrata API
    params = {
        "$limit": "50000",
        "$order": "date DESC",
        "$where": f"date >= '{window_start_str}'"
    }
    
    # Add app token if available
    headers = {}
    if APP_TOKEN:
        headers["X-App-Token"] = APP_TOKEN
    
    # Call crimes API
    try:
        print(f"Calling Socrata API with window_start: {window_start_str}")
        r = requests.get(SOCRATA_URL, params=params, headers=headers, timeout=60)
        r.raise_for_status()
        rows = r.json()
        print(f"Retrieved {len(rows)} crime records from API")
    except requests.exceptions.RequestException as e:
        print(f"Error calling API: {str(e)}")
        return {
            "statusCode": 500,
            "error": str(e)
        }
    
    if not rows:
        print("No new records found")
        return {"statusCode": 200, "count": 0}
    
    # Add ingestion metadata to each record
    for rec in rows:
        rec["_ingested_at"] = now.isoformat()
        rec["_dedupe_id"] = dedupe_key(rec)
    
    # Split into batches of 200 records each (to stay under 1MB limit)
    batch_size = 200
    batches = list(chunk_list(rows, batch_size))
    
    print(f"Sending {len(batches)} batches to Firehose")
    
    # Send each batch to Firehose
    total_sent = 0
    for i, batch in enumerate(batches):
        ndjson = "\n".join(json.dumps(x) for x in batch) + "\n"
        
        try:
            response = firehose.put_record(
                DeliveryStreamName=FIREHOSE_NAME,
                Record={"Data": ndjson.encode("utf-8")}
            )
            total_sent += len(batch)
            print(f"Batch {i+1}/{len(batches)}: Sent {len(batch)} records. RecordId: {response['RecordId']}")
        except Exception as e:
            print(f"Error sending batch {i+1} to Firehose: {str(e)}")
            # Continue with other batches even if one fails
            continue
    
    print(f"Successfully sent {total_sent} total records to Firehose")
    
    return {
        "statusCode": 200,
        "count": total_sent,
        "batches": len(batches),
        "window_start": window_start_str
    }