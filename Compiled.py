#!/usr/bin/env python
# coding: utf-8

# # Merged_API(ROBUST)

# In[ ]:





# In[1]:


import pymongo
from pymongo import MongoClient
import math
from datetime import datetime, timedelta
import pytz
import logging

# Set up logging for console output
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Create a custom logger to add file output
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Ensure logger level matches basicConfig

# Add file handler for appending logs
file_handler = logging.FileHandler("Merged_API_Airline_processing.log", mode="a")  # "a" for append
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(file_handler)

# MongoDB connection details
SOURCE_MONGO_URI = "mongodb://10.240.0.46:27017/"  # Source server
TARGET_MONGO_URI = "mongodb://10.240.0.131:27017/"  # Target server
DATABASE_NAME = "CloudLogsDB"
BATCH_SIZE = 50000

# List of collection names provided
collection_list = [
    "AirArabia_RQ_RS", "AirArabia3L_RQ_RS", "AirAsiaIntl_RQ_RS", "AirIndiaExpress_RQ_RS",
    "Akasa_RQ_RS", "AllianceAir_RQ_RS", "Amadeus_RQ_RS", "American_RQ_RS", "Emirates_RQ_RS",
    "Etihad_RQ_RS", "Fly91_RQ_RS", "FlyArystan_RQ_RS", "FlyToDubai_RQ_RS", "FlyaDeal_RQ_RS",
    "Flybig_RQ_RS", "Flynas_RQ_RS", "Indigo_RQ_RS", "Jazeera_RQ_RS", "Jetstar_RQ_RS",
    "Malindo_RQ_RS", "NokAir_RQ_RS", "OmanAir_RQ_RS", "Sabre_RQ_RS", "Salam_RQ_RS",
    "Scoot_RQ_RS", "Singapore_RQ_RS", "Spicjet_RQ_RS", "StarAir_RQ_RS", "TravelPort_RQ_RS",
    "Travelopedia_RQ_RS", "TripShope_RQ_RS", "Verteil_RQ_RS"
]

# Fields to extract from root and Message (flattened)
FIELDS_TO_EXTRACT = {
    "root": ["InsertOn", "level"],
    "message": [
        "response_time", "segcount", "org", "des", "dep_date", "ret_Date", "paxcount", "cabin",
        "req_time", "traceid", "request_name", "request", "searchid", "elapsed_time", "exception",
        "requesttype", "IsIntl", "AgencyID", "Airline_elapsed_time", "Process_elapsed_time",
        "Cache_elapsed_time", "IsCache", "Remarks"
    ]
}

# Connect to MongoDB servers
source_client = MongoClient(SOURCE_MONGO_URI)
target_client = MongoClient(TARGET_MONGO_URI)
source_db = source_client[DATABASE_NAME]
target_db = target_client[DATABASE_NAME]

# Target collections on the target server
merged_collection = target_db["Merged_API_Airline"]
issue_collection = target_db["Merged_API_Airline_Issue"]

# Ensure an index on Processing_Time in Merged_API_Airline for efficient querying
merged_collection.create_index([("Processing_Time", pymongo.DESCENDING)])

# Global counters for total processed and not processed documents
total_processed = 0
total_not_processed = 0

def extract_airline_name(collection_name):
    """Extract airline name by removing '_RQ_RS' suffix."""
    return collection_name.replace("_RQ_RS", "")

def process_document(doc, airline_name, time_range, processing_time):
    """Process a document, flatten fields, convert elapsed_time to seconds, and add new fields including FlightType."""
    try:
        # Extract root fields
        new_doc = {field: doc.get(field) for field in FIELDS_TO_EXTRACT["root"]}
        
        # Extract and flatten Message fields
        message = doc.get("Message", {})
        for field in FIELDS_TO_EXTRACT["message"]:
            value = message.get(field)
            # Convert elapsed_time from milliseconds to seconds
            if field == "elapsed_time" and value is not None:
                try:
                    new_doc[field] = float(value) / 1000.0  # Convert ms to seconds
                except (ValueError, TypeError):
                    logger.warning(f"Failed to convert elapsed_time to float in document {doc.get('_id', 'unknown')}: {value}")
                    new_doc[field] = 0.0  # Fallback to 0.0 if conversion fails
            else:
                new_doc[field] = value
        
        # Add additional fields
        new_doc["airline_name"] = airline_name
        new_doc["is_issue"] = False
        new_doc["issue"] = None
        new_doc["time_range"] = time_range
        new_doc["record_date"] = doc["InsertOn"].strftime("%Y-%m-%d")  # Extract date from InsertOn
        new_doc["Processing_Time"] = processing_time  # Add UTC processing time
        
        # Add FlightType based on IsIntl
        is_intl = new_doc.get("IsIntl", False)  # Default to False if IsIntl is missing
        new_doc["FlightType"] = "International" if is_intl else "Domestic"
        
        return new_doc, True
    except Exception as e:
        # If processing fails, mark as issue
        new_doc = {
            "_id": doc.get("_id"),
            "original_doc": doc,
            "airline_name": airline_name,
            "is_issue": True,
            "issue": f"Processing failed: {str(e)}",
            "time_range": time_range,
            "record_date": doc.get("InsertOn", datetime.now(pytz.UTC)).strftime("%Y-%m-%d"),  # Fallback to now if InsertOn missing
            "Processing_Time": processing_time,  # Add UTC processing time even in error case
            "FlightType": "Domestic"  # Default to Domestic in error case
        }
        # Include Message fields with elapsed_time conversion in error case
        message = doc.get("Message", {})
        for field in FIELDS_TO_EXTRACT["message"]:
            value = message.get(field)
            if field == "elapsed_time" and value is not None:
                try:
                    new_doc[field] = float(value) / 1000.0  # Convert ms to seconds
                except (ValueError, TypeError):
                    logger.warning(f"Failed to convert elapsed_time to float in error case for document {doc.get('_id', 'unknown')}: {value}")
                    new_doc[field] = 0.0  # Fallback to 0.0 if conversion fails
            else:
                new_doc[field] = value
        return new_doc, False

def process_collection(collection_name, start_time, end_time, processing_time):
    """Process a single collection in batches for the specified time range."""
    global total_processed, total_not_processed
    
    collection = source_db[collection_name]
    airline_name = extract_airline_name(collection_name)
    
    # Query documents from the specified time range (in UTC)
    query = {"InsertOn": {"$gt": start_time, "$lte": end_time}}  # Use $gt to avoid reprocessing exact start_time
    total_docs = collection.count_documents(query)
    
    # Adjust time range for IST (UTC+5:30) for display only
    ist_start_time = start_time + timedelta(hours=5, minutes=30)
    ist_end_time = end_time + timedelta(hours=5, minutes=30)
    time_range = f"{ist_start_time.strftime('%H:%M:%S')} - {ist_end_time.strftime('%H:%M:%S')} (IST)"
    
    logging.info(f"Processing collection: {collection_name} with {total_docs} documents (Time Range: {time_range})")
    
    if total_docs == 0:
        logging.info(f"No documents to process in {collection_name} for the time range")
        return
    
    # Calculate number of batches
    num_batches = math.ceil(total_docs / BATCH_SIZE)
    
    successful_count = 0
    issue_count = 0
    
    # Process in batches
    for batch in range(num_batches):
        skip = batch * BATCH_SIZE
        docs = collection.find(query).skip(skip).limit(BATCH_SIZE)
        
        merged_docs = []
        issue_docs = []
        
        for doc in docs:
            processed_doc, success = process_document(doc, airline_name, time_range, processing_time)
            if success:
                merged_docs.append(processed_doc)
            else:
                issue_docs.append(processed_doc)
        
        # Bulk insert into respective collections on target server
        if merged_docs:
            merged_collection.insert_many(merged_docs)
            successful_count += len(merged_docs)
        
        if issue_docs:
            issue_collection.insert_many(issue_docs)
            issue_count += len(issue_docs)
    
    # Update global counters
    total_processed += successful_count
    total_not_processed += issue_count
    
    # Print summary for this collection
    logging.info(f"Collection {collection_name} processed:")
    logging.info(f" - Successfully stored in Merged_API_Airline: {successful_count}")
    logging.info(f" - Stored in Merged_API_Airline_Issue: {issue_count}")

def main():
    # Set UTC timezone
    utc = pytz.UTC
    
    # Capture the processing time in UTC when the script starts
    processing_time = datetime.now(utc)
    
    # Log a separator for this run
    separator = f"--- Start of Run at {processing_time.strftime('%Y-%m-%d %H:%M:%S UTC')} ---"
    logging.info(separator)
    
    # Determine the start time based on the latest Processing_Time in Merged_API_Airline
    latest_doc = merged_collection.find_one(sort=[("Processing_Time", pymongo.DESCENDING)])
    if latest_doc and "Processing_Time" in latest_doc:
        start_time = latest_doc["Processing_Time"]
        logging.info(f"Latest Processing_Time found: {start_time.isoformat()}")
    else:
        # Fallback to last 10 minutes for the first run
        start_time = processing_time - timedelta(minutes=10)
        logging.info("No previous documents found in Merged_API_Airline, using default 10-minute range")

    # Set end_time to current processing time
    end_time = processing_time
    
    # Adjust for IST (UTC+5:30) for display
    ist_tz = pytz.timezone("Asia/Kolkata")
    ist_start_time = start_time.astimezone(ist_tz)
    ist_end_time = end_time.astimezone(ist_tz)
    logging.info(f"Time range for query: {ist_start_time.isoformat()} to {ist_end_time.isoformat()} (IST)")
    
    # Get list of collections in the source database
    db_collections = source_db.list_collection_names()
    
    # Process only matching collections for the specified time range
    for collection_name in collection_list:
        if collection_name in db_collections:
            process_collection(collection_name, start_time, end_time, processing_time)
    
    # Print final summary of total processed and not processed documents
    logging.info("Processing complete!")
    logging.info(f"Total documents processed (stored in Merged_API_Airline): {total_processed}")
    logging.info(f"Total documents not processed (stored in Merged_API_Airline_Issue): {total_not_processed}")
    
    # Log an end separator
    logging.info(f"--- End of Run at {datetime.now(utc).strftime('%Y-%m-%d %H:%M:%S UTC')} ---")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    finally:
        source_client.close()
        target_client.close()


# In[ ]:





# # Third_pary(ROBUST)

# In[ ]:





# In[2]:


import pymongo
from pymongo import MongoClient
import math
from datetime import datetime, timedelta
import pytz
import logging

# Set up logging for console output
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Create a custom logger to add file output
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Ensure logger level matches basicConfig

# Add file handler for appending logs
file_handler = logging.FileHandler("Processed_Thirdpary_processing.log", mode="a")  # "a" for append
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(file_handler)

# MongoDB connection details
SOURCE_MONGO_URI = "mongodb://10.240.0.46:27017/"  # Source server
TARGET_MONGO_URI = "mongodb://10.240.0.131:27017/"  # Target server
SOURCE_DATABASE_NAME = "IN_logger_flight_data1"
TARGET_DATABASE_NAME = "CloudLogsDB"
BATCH_SIZE = 50000

# Collection names
SOURCE_COLLECTION = "fs_thirdpary_req_log"
TARGET_COLLECTION = "Processed_Thirdpary"

# Fields to extract from root and Message (flattened)
FIELDS_TO_EXTRACT = {
    "root": ["Date", "level", "countrycode", "citycode"],
    "message": [
        "method_name", "URL", "traceid", "vid", "req_time", "elapsed_time",
        "user_name", "apptype", "insertedon", "iserror"
    ]
}

# Connect to MongoDB servers
source_client = MongoClient(SOURCE_MONGO_URI)
target_client = MongoClient(TARGET_MONGO_URI)
source_db = source_client[SOURCE_DATABASE_NAME]
target_db = target_client[TARGET_DATABASE_NAME]

# Target collection on the target server
processed_collection = target_db[TARGET_COLLECTION]

# Ensure an index on Processing_Time for efficient querying
processed_collection.create_index([("Processing_Time", pymongo.DESCENDING)])

# Global counters for total processed documents
total_processed = 0

def determine_portal(user_name):
    """Determine the Portal value based on user_name."""
    if not user_name:
        return "B2C"
    user_name = user_name.upper()
    if "B2B" in user_name or "EMTB2BIN" in user_name:
        return "B2B"
    elif "CORPORATE" in user_name or "EMTCORPORATEIN" in user_name:
        return "CORPORATE"
    else:
        return "B2C"

def process_document(doc, time_range, processing_time):
    """Process a document, flatten fields, and add new fields with IST-adjusted record_date and Processing_Time."""
    new_doc = {}
    try:
        # Extract root fields
        new_doc = {field: doc.get(field) for field in FIELDS_TO_EXTRACT["root"]}
        
        # Extract and flatten Message fields
        message = doc.get("Message")
        if isinstance(message, dict):
            for field in FIELDS_TO_EXTRACT["message"]:
                new_doc[field] = message.get(field)
        elif isinstance(message, list) and message:
            for field in FIELDS_TO_EXTRACT["message"]:
                new_doc[field] = message[0].get(field) if message else None
        else:
            # If Message is missing or invalid, set fields to None
            for field in FIELDS_TO_EXTRACT["message"]:
                new_doc[field] = None
        
        # Convert elapsed_time from milliseconds to seconds
        if new_doc.get("elapsed_time") is not None:
            new_doc["elapsed_time"] = new_doc["elapsed_time"] / 1000.0
        
        # Add additional fields
        new_doc["Portal"] = determine_portal(new_doc.get("user_name"))
        new_doc["time_range"] = time_range
        new_doc["Processing_Time"] = processing_time  # Add UTC processing time
        
        # Adjust record_date to IST (UTC+5:30)
        if "Date" not in doc or not isinstance(doc["Date"], datetime):
            raise ValueError(f"Invalid or missing Date field in document {doc.get('_id', 'unknown')}")
        ist_date = doc["Date"] + timedelta(hours=5, minutes=30)
        new_doc["record_date"] = ist_date.strftime("%Y-%m-%d")
        
    except Exception as e:
        # Log error and return partial document with fallback values
        logging.error(f"Error processing document {doc.get('_id', 'unknown')}: {str(e)}")
        ist_fallback_time = datetime.now(pytz.UTC) + timedelta(hours=5, minutes=30)
        new_doc["error"] = f"Processing failed: {str(e)}"
        new_doc["time_range"] = time_range
        new_doc["Processing_Time"] = processing_time  # Add UTC processing time even in error case
        new_doc["record_date"] = (doc.get("Date", datetime.now(pytz.UTC)) + timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d")
        new_doc["Portal"] = determine_portal(new_doc.get("user_name"))
        
    return new_doc

def process_collection(collection_name, start_time, end_time, processing_time):
    """Process the collection in batches for the specified time range."""
    global total_processed
    
    collection = source_db[collection_name]
    
    # Query documents from the specified time range (in UTC) based on Date
    query = {"Date": {"$gt": start_time, "$lte": end_time}}  # Use $gt to avoid reprocessing the exact start_time
    total_docs = collection.count_documents(query)
    
    # Adjust time range for IST (UTC+5:30) for display and storage
    ist_start_time = start_time + timedelta(hours=5, minutes=30)
    ist_end_time = end_time + timedelta(hours=5, minutes=30)
    time_range = f"{ist_start_time.strftime('%H:%M:%S')} - {ist_end_time.strftime('%H:%M:%S')} (IST)"
    
    logging.info(f"Processing collection: {collection_name} with {total_docs} documents (Time Range: {time_range})")
    
    if total_docs == 0:
        logging.info(f"No documents to process in {collection_name} for the time range")
        return
    
    # Calculate number of batches
    num_batches = math.ceil(total_docs / BATCH_SIZE)
    
    processed_count = 0
    
    # Process in batches
    for batch in range(num_batches):
        skip = batch * BATCH_SIZE
        docs = collection.find(query).skip(skip).limit(BATCH_SIZE)
        
        processed_docs = []
        
        for doc in docs:
            processed_doc = process_document(doc, time_range, processing_time)
            processed_docs.append(processed_doc)
        
        # Bulk insert into the target collection
        if processed_docs:
            processed_collection.insert_many(processed_docs)
            processed_count += len(processed_docs)
    
    # Update global counter
    total_processed += processed_count
    
    # Print summary for this collection
    logging.info(f"Collection {collection_name} processed:")
    logging.info(f" - Successfully stored in Processed_Thirdpary: {processed_count}")

def main():
    # Set UTC timezone
    utc = pytz.UTC
    
    # Capture the processing time in UTC when the script starts
    processing_time = datetime.now(utc)
    
    # Log a separator for this run
    separator = f"--- Start of Run at {processing_time.strftime('%Y-%m-%d %H:%M:%S UTC')} ---"
    logging.info(separator)
    
    # Determine the start time based on the latest Processing_Time in Processed_Thirdpary
    latest_doc = processed_collection.find_one(sort=[("Processing_Time", pymongo.DESCENDING)])
    if latest_doc and "Processing_Time" in latest_doc:
        start_time = latest_doc["Processing_Time"]
        logging.info(f"Latest Processing_Time found: {start_time.isoformat()}")
    else:
        # Fallback to last 10 minutes for the first run
        start_time = processing_time - timedelta(minutes=10)
        logging.info("No previous documents found in Processed_Thirdpary, using default 10-minute range")

    # Set end_time to current processing time
    end_time = processing_time
    
    # Adjust for IST (UTC+5:30) for display
    ist_tz = pytz.timezone("Asia/Kolkata")
    ist_start_time = start_time.astimezone(ist_tz)
    ist_end_time = end_time.astimezone(ist_tz)
    logging.info(f"Time range for query: {ist_start_time.isoformat()} to {ist_end_time.isoformat()} (IST)")
    
    # Process the specified collection, passing the processing_time
    process_collection(SOURCE_COLLECTION, start_time, end_time, processing_time)
    
    # Print final summary of total processed documents
    logging.info("Processing complete!")
    logging.info(f"Total documents processed (stored in Processed_Thirdpary): {total_processed}")
    
    # Log an end separator
    logging.info(f"--- End of Run at {datetime.now(utc).strftime('%Y-%m-%d %H:%M:%S UTC')} ---")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    finally:
        source_client.close()
        target_client.close()


# In[ ]:





# # Reprice(ROBUST)

# In[ ]:





# In[3]:


import pymongo
from pymongo import MongoClient
import math
from datetime import datetime, timedelta
import pytz
import logging

# Set up logging for console output
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Create a custom logger to add file output
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Ensure logger level matches basicConfig

# Add file handler for appending logs
file_handler = logging.FileHandler("Processed_Repricing_processing.log", mode="a")  # "a" for append
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(file_handler)

# MongoDB connection details
SOURCE_MONGO_URI = "mongodb://10.240.0.46:27017/"  # Source server
TARGET_MONGO_URI = "mongodb://10.240.0.131:27017/"  # Target server
SOURCE_DATABASE_NAME = "IN_logger_flight_data1"
TARGET_DATABASE_NAME = "CloudLogsDB"  # Changed to CloudLogsDB
BATCH_SIZE = 50000

# Collection names
SOURCE_COLLECTION = "fs_reprice_rs"
TARGET_COLLECTION = "Processed_Repricing"

# Fields to extract from root and Message (flattened)
FIELDS_TO_EXTRACT = {
    "root": ["Date", "level", "useragent", "countrycode", "citycode"],
    "message": [
        "traceid", "reppos", "response_time", "username", 
        "requestedfare", "responsefare", "faredifference", "elapsed_time"
    ]
}

# Connect to MongoDB servers
source_client = MongoClient(SOURCE_MONGO_URI)
target_client = MongoClient(TARGET_MONGO_URI)
source_db = source_client[SOURCE_DATABASE_NAME]
target_db = target_client[TARGET_DATABASE_NAME]

# Target collection on the target server
processed_collection = target_db[TARGET_COLLECTION]

# Ensure an index on Processing_Time for efficient querying
processed_collection.create_index([("Processing_Time", pymongo.DESCENDING)])

# Global counters for total processed documents
total_processed = 0

# List of Meta Search usernames
META_SEARCH_USERNAMES = {
    "acloud", "adcanopus", "adgama", "couponzguru", "google", "googleemt", "grabon",
    "hexaweb", "hexaweb1", "hexaweb2", "kayak", "OCTAADS", "octaads700", "prudentads",
    "rag", "reclame", "SEARCHMYJOURNEY", "SNAP", "utmdigital", "wegom", "xlnc", "XLNCECOMMERCE"
}

def determine_portal(username):
    """Determine the Portal value based on username."""
    if not username:  # Handle None or empty username
        return "B2C"
    if username == "B2B":
        return "B2B"
    if username == "CORPORATE":
        return "CORPORATE"
    if username in META_SEARCH_USERNAMES:
        return "Meta Search"
    return "B2C"  # Default case

def process_document(doc, time_range, processing_time):
    """Process a document, flatten fields, and add new fields including Actual_Reprice, Portal, and Processing_Time."""
    new_doc = {}
    try:
        # Extract root fields
        new_doc = {field: doc.get(field) for field in FIELDS_TO_EXTRACT["root"]}
        
        # Extract and flatten Message fields
        message = doc.get("Message", {})
        for field in FIELDS_TO_EXTRACT["message"]:
            value = message.get(field)
            # Convert specific fields to numeric format
            if field in ["requestedfare", "responsefare", "faredifference"]:
                try:
                    new_doc[field] = float(value) if value is not None else 0.0
                except (ValueError, TypeError):
                    logging.warning(f"Failed to convert {field} to float in document {doc.get('_id', 'unknown')}: {value}")
                    new_doc[field] = 0.0  # Fallback to 0.0 if conversion fails
            else:
                new_doc[field] = value
        
        # Add additional fields
        new_doc["time_range"] = time_range
        new_doc["Processing_Time"] = processing_time  # UTC processing time
        
        # Calculate Actual_Reprice based on faredifference
        faredifference = new_doc.get("faredifference", 0.0)
        new_doc["Actual_Reprice"] = bool(faredifference > 0)  # True if faredifference > 0, False otherwise
        
        # Add Portal based on username
        new_doc["Portal"] = determine_portal(new_doc.get("username"))
        
        # Adjust record_date to IST (UTC+5:30)
        if "Date" not in doc or not isinstance(doc["Date"], datetime):
            raise ValueError(f"Invalid or missing Date field in document {doc.get('_id', 'unknown')}")
        ist_date = doc["Date"] + timedelta(hours=5, minutes=30)
        new_doc["record_date"] = ist_date.strftime("%Y-%m-%d")
        
    except Exception as e:
        # Log error and return partial document with fallback values
        logging.error(f"Error processing document {doc.get('_id', 'unknown')}: {str(e)}")
        new_doc = {
            "error": f"Processing failed: {str(e)}",
            "time_range": time_range,
            "Processing_Time": processing_time,
            "record_date": (doc.get("Date", datetime.now(pytz.UTC)) + timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d"),
            "Actual_Reprice": False,  # Default to False in error case
            "Portal": determine_portal(message.get("username"))  # Still determine Portal in error case
        }
        # Include any available fields from root or Message
        for field in FIELDS_TO_EXTRACT["root"]:
            new_doc[field] = doc.get(field)
        message = doc.get("Message", {})
        for field in FIELDS_TO_EXTRACT["message"]:
            value = message.get(field)
            if field in ["requestedfare", "responsefare", "faredifference"]:
                try:
                    new_doc[field] = float(value) if value is not None else 0.0
                except (ValueError, TypeError):
                    new_doc[field] = 0.0
            else:
                new_doc[field] = value
        
    return new_doc

def process_collection(collection_name, start_time, end_time, processing_time):
    """Process the collection in batches for the specified time range."""
    global total_processed
    
    collection = source_db[collection_name]
    
    # Query documents from the specified time range (in UTC) based on Date
    query = {"Date": {"$gt": start_time, "$lte": end_time}}  # Use $gt to avoid reprocessing the exact start_time
    total_docs = collection.count_documents(query)
    
    # Adjust time range for IST (UTC+5:30) for display and storage
    ist_start_time = start_time + timedelta(hours=5, minutes=30)
    ist_end_time = end_time + timedelta(hours=5, minutes=30)
    time_range = f"{ist_start_time.strftime('%H:%M:%S')} - {ist_end_time.strftime('%H:%M:%S')} (IST)"
    
    logging.info(f"Processing collection: {collection_name} with {total_docs} documents (Time Range: {time_range})")
    
    if total_docs == 0:
        logging.info(f"No documents to process in {collection_name} for the time range")
        return
    
    # Calculate number of batches
    num_batches = math.ceil(total_docs / BATCH_SIZE)
    
    processed_count = 0
    
    # Process in batches
    for batch in range(num_batches):
        skip = batch * BATCH_SIZE
        docs = collection.find(query).skip(skip).limit(BATCH_SIZE)
        
        processed_docs = []
        
        for doc in docs:
            processed_doc = process_document(doc, time_range, processing_time)
            processed_docs.append(processed_doc)
        
        # Bulk insert into the target collection
        if processed_docs:
            processed_collection.insert_many(processed_docs)
            processed_count += len(processed_docs)
    
    # Update global counter
    total_processed += processed_count
    
    # Print summary for this collection
    logging.info(f"Collection {collection_name} processed:")
    logging.info(f" - Successfully stored in Processed_Repricing: {processed_count}")

def main():
    # Set UTC timezone
    utc = pytz.UTC
    
    # Capture the processing time in UTC when the script starts
    processing_time = datetime.now(utc)
    
    # Log a separator for this run
    separator = f"--- Start of Run at {processing_time.strftime('%Y-%m-%d %H:%M:%S UTC')} ---"
    logging.info(separator)
    
    # Determine the start time based on the latest Processing_Time in Processed_Repricing
    latest_doc = processed_collection.find_one(sort=[("Processing_Time", pymongo.DESCENDING)])
    if latest_doc and "Processing_Time" in latest_doc:
        start_time = latest_doc["Processing_Time"]
        logging.info(f"Latest Processing_Time found: {start_time.isoformat()}")
    else:
        # Fallback to last 10 minutes for the first run
        start_time = processing_time - timedelta(minutes=10)
        logging.info("No previous documents found in Processed_Repricing, using default 10-minute range")

    # Set end_time to current processing time
    end_time = processing_time
    
    # Adjust for IST (UTC+5:30) for display
    ist_tz = pytz.timezone("Asia/Kolkata")
    ist_start_time = start_time.astimezone(ist_tz)
    ist_end_time = end_time.astimezone(ist_tz)
    logging.info(f"Time range for query: {ist_start_time.isoformat()} to {ist_end_time.isoformat()} (IST)")
    
    # Process the specified collection
    process_collection(SOURCE_COLLECTION, start_time, end_time, processing_time)
    
    # Print final summary of total processed documents
    logging.info("Processing complete!")
    logging.info(f"Total documents processed (stored in Processed_Repricing): {total_processed}")
    
    # Log an end separator
    logging.info(f"--- End of Run at {datetime.now(utc).strftime('%Y-%m-%d %H:%M:%S UTC')} ---")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    finally:
        source_client.close()
        target_client.close()


# In[ ]:




