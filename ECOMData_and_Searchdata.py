#!/usr/bin/env python
# coding: utf-8

from pymongo import MongoClient
import logging
from datetime import datetime, timedelta
from pathlib import Path
import os
import re
import pytz

# Set up logging for console output
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Create a custom logger to add file output
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Add file handler for appending logs
file_handler = logging.FileHandler("data_extraction.log", mode="a")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(file_handler)

# File-based lock
LOCK_FILE = Path("data_extraction.lock")

# Check if another instance is running
if LOCK_FILE.exists():
    print("Another instance is running. Exiting.")
    logger.info("Another instance is running. Exiting.")
    exit(0)

# MongoDB Connection
try:
    client = MongoClient("mongodb://10.240.0.131:27017/", serverSelectionTimeoutMS=5000)
    client.server_info()  # Test connection
    logger.info("Connected to MongoDB successfully")
except Exception as e:
    logger.error(f"Failed to connect to MongoDB: {e}")
    raise

# Create lock file
LOCK_FILE.touch()
logger.info("Lock file created")

try:
    # Database and collections
    source_db = client["DSAnalysis"]
    target_db = client["CloudLogsDB"]
    source_collection = source_db["ECOMData"]
    destination_collection = target_db["NewECOMData"]

    # No indexing on bookingid; relying on default _id uniqueness
    logger.info("No custom indexing enforced; using default _id uniqueness")

    # List of required columns
    columns_to_extract = [
        "app", "adt", "triptype", "brand", "room", "bookingDate", "chd", "travelDate",
        "bookingid", "portal", "inserted_time", "inserted_date", "_id", "utmsource",
        "inf", "discount", "uid", "class", "timezone", "price", "currcode",
        "domain", "product", "source", "destination", "location_type", "loginkey",
        "destination_fullname", "source_fullname",
        "total_price", "operator_discount", "base_price", "convenience_fee",
        "addOn_price", "airline_fullname", "tax", "addOn_type",
        "coupon"
    ]

    # Projection for MongoDB query
    projection = {col: 1 for col in columns_to_extract}

    # Processing time in UTC
    utc = pytz.UTC
    processing_time = datetime.now(utc)

    # Log start of run
    separator = f"--- Start of Run at {processing_time.strftime('%Y-%m-%d %H:%M:%S UTC')} ---"
    logger.info(separator)

    # Find the latest Processing_Time in destination
    latest_record = destination_collection.find_one(sort=[("Processing_Time", -1)])
    if latest_record and "Processing_Time" in latest_record:
        start_time = latest_record["Processing_Time"]
        logger.info(f"Latest Processing_Time found: {start_time.isoformat()}")
    else:
        start_time = processing_time - timedelta(minutes=10)
        logger.info("No previous Processing_Time found, using default 10-minute range")

    # Set end time to current processing time
    end_time = processing_time

    # IST time range for logging and storage
    ist_tz = pytz.timezone("Asia/Kolkata")
    ist_start_time = start_time.astimezone(ist_tz)
    ist_end_time = end_time.astimezone(ist_tz)
    time_range = f"{ist_start_time.strftime('%H:%M:%S')} - {ist_end_time.strftime('%H:%M:%S')} (IST)"
    logger.info(f"Time range for query: {ist_start_time.isoformat()} to {ist_end_time.isoformat()} (IST)")

    # Filter condition based on inserted_date and inserted_time
    filter_condition = {
        "$or": [
            {"inserted_date": {"$gt": start_time.strftime("%Y-%m-%d")}},
            {
                "inserted_date": start_time.strftime("%Y-%m-%d"),
                "inserted_time": {"$gt": start_time.strftime("%H:%M:%S")}
            }
        ]
    }

    # Process data in batches
    batch_size = 30000
    processed_count = 0
    skipped_count = 0

    # Count total documents to process
    total_docs = source_collection.count_documents(filter_condition)
    logger.info(f"Processing collection: ECOMData with {total_docs} documents (Time Range: {time_range})")

    if total_docs == 0:
        logger.info("No documents to process in ECOMData for the time range")
        print("No documents to process in ECOMData for the time range")
    else:
        cursor = source_collection.find(filter_condition, projection).batch_size(batch_size)
        batch = []

        # Check for existing records in destination to track duplicates
        existing_ids = set(
            doc["_id"] for doc in destination_collection.find(
                {"_id": {"$in": [doc["_id"] for doc in source_collection.find(filter_condition, {"_id": 1})]}},
                {"_id": 1}
            )
        )
        logger.info(f"Found {len(existing_ids)} existing records in destination_collection")

        # Function to standardize travelDate to DD-MM-YYYY
        def standardize_date(date_str):
            if not date_str:
                return None
            try:
                formats = [
                    "%a-%d%b%Y",              # 'Sat-12Apr2025'
                    "%m/%d/%Y %I:%M:%S %p",   # '2/17/2025 12:00:00 AM'
                    "%a %b %d %H:%M:%S GMT%z %Y",  # 'Fri Feb 14 00:00:00 GMT+05:30 2025'
                    "%Y-%m-%d",               # '2025-02-02'
                    "%m-%d-%Y",               # '02-13-2025'
                    "%d-%m-%Y"                # '05-07-2025'
                ]
                if re.match(r"^[A-Za-z]{3}-\d{2}[A-Za-z]{3}\d{4}$", date_str):
                    date_str = date_str.replace("-", "")
                for fmt in formats:
                    try:
                        dt = datetime.strptime(date_str, fmt)
                        return dt.strftime("%d-%m-%Y")
                    except ValueError:
                        continue
                logger.warning(f"Unable to parse date: {date_str}")
                return date_str
            except Exception as e:
                logger.error(f"Error parsing date {date_str}: {e}")
                return date_str

        for doc in cursor:
            if doc["_id"] in existing_ids:
                skipped_count += 1
                continue

            if not doc.get("inserted_date") or not doc.get("inserted_time"):
                logger.warning(f"Skipping record with missing timestamp: {doc.get('_id')}")
                skipped_count += 1
                continue

            # Transform the document
            transformed_doc = {field: doc.get(field) for field in columns_to_extract}

            # Data Cleaning and Preprocessing
            class_mapping = {"0": "Economy", "4": "Premium Economy", "2": "Business", "1": "First"}
            if transformed_doc.get("class") in class_mapping:
                transformed_doc["class"] = class_mapping[transformed_doc["class"]]

            if transformed_doc.get("travelDate"):
                transformed_doc["travelDate"] = standardize_date(transformed_doc["travelDate"])

            if transformed_doc.get("coupon"):
                transformed_doc["coupon"] = transformed_doc["coupon"].upper()

            # Add new fields
            transformed_doc["Processing_Time"] = processing_time  # UTC datetime
            transformed_doc["time_range"] = time_range  # IST string
            transformed_doc["record_date"] = ist_end_time.strftime("%Y-%m-%d")  # IST date string

            batch.append(transformed_doc)

            if len(batch) >= batch_size:
                try:
                    result = destination_collection.insert_many(batch, ordered=False)
                    processed_count += len(result.inserted_ids)
                    logger.info(f"Processed batch: {len(result.inserted_ids)} records")
                except pymongo.errors.BulkWriteError as bwe:
                    logger.warning(f"Bulk write error: {bwe.details}")
                    successful_inserts = len(batch) - len(bwe.details.get("writeErrors", []))
                    processed_count += successful_inserts
                    batch = []
                except Exception as e:
                    logger.error(f"Unexpected error in batch insert: {e}")
                    batch = []

        # Insert remaining records
        if batch:
            try:
                result = destination_collection.insert_many(batch, ordered=False)
                processed_count += len(result.inserted_ids)
                logger.info(f"Processed remaining batch: {len(result.inserted_ids)} records")
            except pymongo.errors.BulkWriteError as bwe:
                logger.warning(f"Bulk write error in remaining batch: {bwe.details}")
                successful_inserts = len(batch) - len(bwe.details.get("writeErrors", []))
                processed_count += successful_inserts
            except Exception as e:
                logger.error(f"Unexpected error in remaining batch: {e}")

        # Summary
        logger.info("Processing complete")
        logger.info(f"Processed: {processed_count}, Skipped: {skipped_count}")
        logger.info(f"--- End of Run at {datetime.now(utc).strftime('%Y-%m-%d %H:%M:%S UTC')} ---")
        print(f"Processed: {processed_count}, Skipped: {skipped_count}")

except Exception as e:
    logger.error(f"Error during processing: {e}")
    print(f"Error occurred, check logs")
finally:
    client.close()
    logger.info("MongoDB connection closed")
    if LOCK_FILE.exists():
        LOCK_FILE.unlink()
        logger.info("Lock file removed")

if __name__ == "__main__":
    main()


# # 2. SearchData Code-->

# In[2]:


from pymongo import MongoClient
import logging
from datetime import datetime, timedelta

# Clear existing handlers to avoid overlap in notebook
logging.getLogger().handlers = []

# Set up logging with a specific logger for Search
search_logger = logging.getLogger('search_logger')
search_logger.setLevel(logging.INFO)
search_handler = logging.FileHandler("search_data_extraction.log")
search_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
search_logger.addHandler(search_handler)

# MongoDB Connection
try:
    client = MongoClient("mongodb://10.240.0.131:27017/", serverSelectionTimeoutMS=5000)
    client.server_info()  # Test connection
    search_logger.info("Connected to MongoDB successfully")
except Exception as e:
    search_logger.error(f"Failed to connect to MongoDB: {e}")
    raise

db = client["DSAnalysis"]
source_collection = db["SearchData"]
destination_collection = db["Newsearchdataa"]

# Updated list of required columns
columns_to_extract = [
    "triptype", "app", "page", "product", "domain", "class", "_id", "utmmedium",
    "inserted_date", "inserted_time", "utmcampaign", "currcode", "faretype", "airline",
    "clicktype", "uid", "coupon", "utmsource", "bookingid", "event",
    "eventname", "destination", "source", "portal", "loginkey"
]

# Projection for MongoDB query
projection = {col: 1 for col in columns_to_extract}

# Data Cleaning Function
def clean_document(doc):
    """
    Cleans a single document and adds a new 'airline_name' column.
    Returns the cleaned document or None if it should be skipped.
    """
    cleaned_doc = {}
   
    # Default values for missing fields
    defaults = {
        "triptype": "unknown",
        "app": "unknown",
        "page": "unknown",
        "product": "unknown",
        "domain": "unknown",
        "class": "unknown",
        "utmmedium": "none",
        "utmcampaign": "none",
        "currcode": "USD",
        "faretype": "unknown",
        "airline": "unknown",
        "clicktype": "unknown",
        "uid": None,
        "coupon": "none",
        "utmsource": "none",
        "bookingid": None,
        "event": "unknown",
        "eventname": "unknown",
        "destination": "unknown",
        "source": "unknown",
        "portal": "unknown",
        "loginkey": None
    }

    # Copy and clean required fields
    for field in columns_to_extract:
        value = doc.get(field)

        # Handle missing or None values
        if value is None or value == "":
            cleaned_doc[field] = defaults.get(field, None)
        elif field == "coupon":
            # Convert coupon to uppercase
            cleaned_doc[field] = str(value).strip().upper()
        elif isinstance(value, str):
            # Strip whitespace and lowercase other strings
            cleaned_doc[field] = value.strip().lower()
        else:
            cleaned_doc[field] = value

    # Create new 'airline_name' column from 'airline'
    airline_value = cleaned_doc.get("airline", "unknown")
    if isinstance(airline_value, str) and " " in airline_value:
        cleaned_doc["airline_name"] = airline_value.split(" ")[0].upper()  # e.g., "QR 134" -> "QR"
    else:
        cleaned_doc["airline_name"] = airline_value.upper()  # If no space, use the whole value

    # Skip if _id is missing
    if cleaned_doc["_id"] is None:
        search_logger.warning(f"Skipping document with missing _id: {doc}")
        return None

    return cleaned_doc

# Fetch Logic
try:
    script_start_time = datetime.now()
    current_date = script_start_time.strftime("%Y-%m-%d")
    search_logger.info(f"Script started at: {current_date} {script_start_time.strftime('%H:%M:%S')}")

    latest_record = destination_collection.find_one(
        sort=[("inserted_date", -1), ("inserted_time", -1)]
    )
   
    if latest_record:
        latest_date = latest_record["inserted_date"]
        latest_time = latest_record["inserted_time"]
        search_logger.info(f"Using last processed time as start: {latest_date} {latest_time}")
    else:
        start_time = script_start_time - timedelta(minutes=10)
        latest_date = start_time.strftime("%Y-%m-%d")
        latest_time = start_time.strftime("%H:%M:%S")
        search_logger.info(f"No records in destination yet; starting from {latest_date} {latest_time}")

    filter_condition = {
        "$or": [
            {"inserted_date": {"$gt": latest_date}},
            {
                "inserted_date": latest_date,
                "inserted_time": {"$gt": latest_time}
            }
        ]
    }

except Exception as e:
    search_logger.error(f"Error determining fetch window: {e}")
    print(f"❌ Error determining fetch window: {e}")
    client.close()
    raise

# Process data in batches with duplicate prevention and cleaning
batch_size = 50000
processed_count = 0
skipped_count = 0

try:
    total_docs = source_collection.count_documents(filter_condition)
    search_logger.info(f"Initial estimate of documents (open-ended): {total_docs}")

    if total_docs == 0:
        print("✅ No new records to process.")
        search_logger.info("No new records found")
    else:
        cursor = source_collection.find(filter_condition, projection).batch_size(10000)
        batch = []

        existing_ids = set(
            doc["_id"] for doc in destination_collection.find(
                {"_id": {"$in": [doc["_id"] for doc in source_collection.find(filter_condition, {"_id": 1})]}},
                {"_id": 1}
            )
        )
        search_logger.info(f"Found {len(existing_ids)} existing records in destination_collection")

        script_run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for doc in cursor:
            if doc["_id"] in existing_ids:
                skipped_count += 1
                continue

            # Clean the document
            cleaned_doc = clean_document(doc)
            if cleaned_doc is None:
                skipped_count += 1
                continue

            # Add script_run_time
            cleaned_doc["script_run_time"] = script_run_time
            batch.append(cleaned_doc)

            if len(batch) >= batch_size:
                destination_collection.insert_many(batch, ordered=False)
                processed_count += len(batch)
                batch = []
                search_logger.info(f"Processed {processed_count} records (skipped {skipped_count} duplicates)")
                print(f"✅ Processed {processed_count} records (skipped {skipped_count} duplicates)...")

        if batch:
            destination_collection.insert_many(batch, ordered=False)
            processed_count += len(batch)
            search_logger.info(f"Processed {processed_count} records (skipped {skipped_count} duplicates)")
            print(f"✅ Processed {processed_count} records (skipped {skipped_count} duplicates)...")

        current_time = datetime.now()
        current_time_str = current_time.strftime("%H:%M:%S")
        current_date = current_time.strftime("%Y-%m-%d")
        final_filter_condition = {
            "inserted_date": current_date,
            "inserted_time": {"$gte": latest_time, "$lte": current_time_str}
        }
        search_logger.info(f"Final filter condition: {final_filter_condition}")

        final_count = source_collection.count_documents(final_filter_condition)
        search_logger.info(f"Final count of documents in the window: {final_count}")
        print(f"🎉 Data extraction and cleaning completed! Total records processed: {processed_count}, Skipped: {skipped_count}, Final count: {final_count}")

        # Log the last processed time
        search_logger.info(f"Saved last processed time: {current_time_str}")

except Exception as e:
    search_logger.error(f"Error during processing: {e}")
    print(f"❌ An error occurred: {e}")
    raise

finally:
    client.close()
    search_logger.info("MongoDB connection closed")


# ## Done hai maamla

# In[ ]:




