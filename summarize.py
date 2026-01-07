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
    'drifters': {'metagroups': ['id', 'metadata', 'wmo', 'platform'], 'startDate': None, 'endDate': None}, # drifters live in an independent deployment, do this over there

}

for dataset in datasets:
    startDate, endDate = get_timestamp_range(db, dataset)
    datasets[dataset]['startDate'] = startDate
    datasets[dataset]['endDate'] = endDate

try:
    db.summaries.replace_one({"_id": 'ratelimiter'}, {"_id": 'ratelimiter', "metadata":datasets}, upsert=True)
except BaseException as err:
    print('error: db write failure - ratelimiter')
    print(err)
    print(datasets)

# generic metadata doc
driftersGenericMeta = {
    "_id":"driftersGenericMeta",
    "data_info":[
        ["ve", "vn", "err_lon", "err_lat", "err_ve", "err_vn", "gap", "sst", "sst1", "sst2", "err_sst", "err_sst1", "err_sst2", "flg_sst", "flg_sst1", "flg_sst2"],
        ['units', 'long_name'],
        [["m/s", "Eastward velocity",],["m/s", "Northward velocity",],["degrees_east", "95% confidence interval in longitude",],["degrees_north", "95% confidence interval in latitude",],["m/s", "95% confidence interval in eastward velocity",],["m/s", "95% confidence interval in northward velocity",],["seconds", "time interval between previous and next location fix",],["Kelvin", "fitted sea water temperature",],["Kelvin", "fitted non-diurnal sea water temperature",],["Kelvin", "fitted diurnal sea water temperature anomaly",],["Kelvin", "standard uncertainty of fitted sea water temperature",],["Kelvin", "standard uncertainty of fitted non-diurnal sea water temperature",],["Kelvin", "standard uncertainty of fitted diurnal sea water temperature anomaly",],["None", "fitted sea water temperature quality flag",],["None", "fitted non-diurnal sea water temperature quality flag",],["None","fitted diurnal sea water temperature anomaly quality flag"]]
    ]
}

try:
    db.summaries.replace_one({"_id": 'driftersGenericMeta'}, driftersGenericMeta, upsert=True)
except BaseException as err:
    print('error: db write failure - generic meta')
    print(err)
    print(datasets)

