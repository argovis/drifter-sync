from pymongo import MongoClient
from geopy import distance
from itertools import compress
import wget, xarray, time, re, datetime, math, os, glob, warnings, numpy
warnings.simplefilter(action='ignore', category=FutureWarning)

client = MongoClient('mongodb://database/argo')
db = client.argo

def getprop(ds, var, prop):
    try:
        return getattr(ds[var],prop)
    except:
        return None

def metamatch(nc, meta, nc_key, mongo_key, munge=None):

	match = True
	if munge:
		match = munge(nc[nc_key].data[0]) == meta[mongo_key]
	else:
		match = nc[nc_key].data[0] == meta[mongo_key]

	if not match:
		print(nc_key, nc[nc_key].data[0], type(nc[nc_key].data[0]), 'mismatches', mongo_key, meta[mongo_key], type(meta[mongo_key]))

def parse_date(timestamp):
	# given an integer number of seconds since 1970-01-01T00:00:00Z <timestamp>,
	# return a corresponding datetime if possible, or None.

	try:
		return datetime.datetime.utcfromtimestamp(int(timestamp)/1e9)
	except:
		#print('raw', timestamp, type(timestamp))
		return None

def stringparse(s):
	return s.decode("utf-8").strip()


metaIDs = list(db.drifterMeta.distinct('_id'))

for metaID in metaIDs:
	# get metadata doc and all corresponding data docs
	drifters = list(db.drifter.find({"metadata": metaID}))
	m = list(db.drifterMeta.find({"_id": metaID}))[0]
	message = ''
	suppressMessage = False

	# get upstream netcdf file
	fileOpenFail = False
	nc = {}
	try:
		filename = wget.download(m['source'][0]['url'])
		nc = {
			"source": m['source'][0]['url'],
			"filename": filename,
			"data": xarray.open_dataset(filename)
		}
	except:
		print('failed to download and open', m['source'][0]['url'])
		fileOpenFail = True
	if fileOpenFail:
		continue

	# check file matches mongo
	## metadata
	print(f'Checking metadata ID {metaID}')
	metamatch(nc['data'], m, 'rowsize', 'rowsize')
	metamatch(nc['data'], m, 'WMO', 'wmo')
	metamatch(nc['data'], m, 'expno', 'expno')
	metamatch(nc['data'], m, 'deploy_lon', 'deploy_lon')
	metamatch(nc['data'], m, 'deploy_lat', 'deploy_lat')
	metamatch(nc['data'], m, 'end_date', 'end_date', parse_date)
	metamatch(nc['data'], m, 'end_lon', 'end_lon')
	metamatch(nc['data'], m, 'end_lat', 'end_lat')
	metamatch(nc['data'], m, 'drogue_lost_date', 'drogue_lost_date', parse_date)
	metamatch(nc['data'], m, 'typedeath', 'typedeath')
	metamatch(nc['data'], m, 'typebuoy', 'typebuoy', stringparse)
	metamatch(nc['data'], m, 'deploy_date', 'deploy_date', parse_date)
	units = [getprop(nc['data'],v,'units') for v in m['data_keys']]
	if units != m['units']:
		print(f'Units mismatch: netcdf: {units}, mongo: {m["units"]}')
	long_name = [getprop(nc['data'],v,'long_name') for v in m['data_keys']]
	if long_name != m['long_name']:
		print(f'Long name mismatch: netcdf: {long_name}, mongo: {m["long_name"]}')

	# tbd source.url, units, long_name

	## data
	for d in drifters:
		message+= f'Checking data record {d["_id"]}\n'
		try:
			index = int(d['_id'].split('_')[1])
			data_keys = ["ve", "vn", "err_lon", "err_lat", "err_ve", "err_vn", "gap", "sst", "sst1", "sst2", "err_sst", "err_sst1", "err_sst2", "flg_sst", "flg_sst1", "flg_sst2"]
			casts = [numpy.float32,numpy.float32,numpy.float32,numpy.float32,numpy.float32,numpy.float32,numpy.float64,numpy.float64,numpy.float64,numpy.float64,numpy.float64,numpy.float64,numpy.float64,numpy.float64,numpy.float64,numpy.float64] 
			measurements = [nc['data'][x].data[0][index] for x in data_keys]
			measurements[6] = numpy.float64(measurements[6])/1000000000
			for i in range(len(measurements)):
				if not numpy.isnan(casts[i](d['data'][0][i])) and measurements[i] != casts[i](d['data'][0][i]):
					message += f'{data_keys[i]} doesnt match. netCDF: {measurements[i]}, {type(measurements[i])}, mongo: {casts[i](d["data"][0][i])}, {type(casts[i](d["data"][0][i]))}/n'
					suppressMessage = False
				elif numpy.isnan(casts[i](d['data'][0][i])):
					if measurements[i] != -1e34:
						message += f'Got numpy.nan from mongo but saw {measurements[i]} in netcdf'
						suppressMessage = False
		except Exception as e:
			print(e)

	if not suppressMessage:
		print(message)

	for f in glob.glob("*.nc"):
		os.remove(f)

	time.sleep(1)
