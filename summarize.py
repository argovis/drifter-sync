from pymongo import MongoClient
from bson.son import SON
import datetime, json, copy, re

client = MongoClient('mongodb://database/argo')
db = client.argo

# rate limiter metadata

def get_timestamp_range(db, collection_name):
    collection = db[collection_name]
    
    # Find the earliest timestamp
    filter = {}
    if 'qc' in datasets[collection_name]:
        filter[datasets[collection_name]['qc']] = 1
    earliest_doc = collection.find_one(filter, sort=[("timestamp", 1)])
    if earliest_doc and "timestamp" in earliest_doc:
        earliest_timestamp = earliest_doc["timestamp"]
    else:
        return None, None  # Return None if no timestamps are found

    # Find the latest timestamp or current time, whichever is earlier
    filter = {}
    if 'qc' in datasets[collection_name]:
        filter[datasets[collection_name]['qc']] = 1
    latest_doc = collection.find_one(filter, sort=[("timestamp", -1)])
    current_time = datetime.datetime.utcnow()

    if latest_doc and "timestamp" in latest_doc:
        latest_timestamp = min(latest_doc["timestamp"], current_time)
    else:
        latest_timestamp = current_time  # If no documents, default to current time

    # Convert timestamps to ISO 8601 format
    try:
        earliest_iso = earliest_timestamp.isoformat() + "Z"
        latest_iso = latest_timestamp.isoformat() + "Z"
        return earliest_iso, latest_iso
    except:
        return None, None

datasets = {
    # metagroups: indexed fields to allow rate limiter cost discounts for; corresponds more or less to the special fields listed in each dataset's service's local_filter and metafilter
    'drifter': {'metagroups': ['_id', 'metadata', 'wmo', 'platform'], 'startDate': None, 'endDate': None}, # drifters live in an independent deployment, do this over there

}

for dataset in datasets:
    startDate, endDate = get_timestamp_range(db, dataset)
    datasets[dataset]['startDate'] = startDate
    datasets[dataset]['endDate'] = endDate

try:
    db.summaries.replace_one({"_id": 'ratelimiter'}, {"_id": 'ratelimiter', "metadata":datasets}, upsert=True)
except BaseException as err:
    print('error: db write failure')
    print(err)
    print(datasets)





