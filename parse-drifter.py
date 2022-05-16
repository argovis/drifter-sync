import xarray, sys, datetime, math
from geopy import distance

def find_basin(lon, lat):
    # for a given lon, lat,
    # identify the basin from the lookup table.
    # choose the nearest non-nan grid point.

    gridspacing = 0.5
    basins = xarray.open_dataset('parameters/basinmask_01.nc')

    basin = basins['BASIN_TAG'].sel(LONGITUDE=lon, LATITUDE=lat, method="nearest").to_dict()['data']
    if math.isnan(basin):
        # nearest point was on land - find the nearest non nan instead.
        lonplus = math.ceil(lon / gridspacing)*gridspacing
        lonminus = math.floor(lon / gridspacing)*gridspacing
        latplus = math.ceil(lat / gridspacing)*gridspacing
        latminus = math.floor(lat / gridspacing)*gridspacing
        grids = [(basins['BASIN_TAG'].sel(LONGITUDE=lonminus, LATITUDE=latminus, method="nearest").to_dict()['data'], distance.distance((lat, lon), (latminus, lonminus)).miles),
                 (basins['BASIN_TAG'].sel(LONGITUDE=lonminus, LATITUDE=latplus, method="nearest").to_dict()['data'], distance.distance((lat, lon), (latplus, lonminus)).miles),
                 (basins['BASIN_TAG'].sel(LONGITUDE=lonplus, LATITUDE=latplus, method="nearest").to_dict()['data'], distance.distance((lat, lon), (latplus, lonplus)).miles),
                 (basins['BASIN_TAG'].sel(LONGITUDE=lonplus, LATITUDE=latminus, method="nearest").to_dict()['data'], distance.distance((lat, lon), (latminus, lonplus)).miles)]

        grids = [x for x in grids if not math.isnan(x[0])]
        if len(grids) == 0:
            # all points on land
            print('warning: all surrounding basin grid points are NaN')
            basin = -1
        else:
            grids.sort(key=lambda tup: tup[1])
            basin = grids[0][0]
    basins.close()
    return int(basin)

ds = xarray.open_dataset(sys.argv[1], decode_times=False)

# generate metadata object
meta = {
	"driftID": int(ds.ID.data[0]), # primary key
	"rowsize": ds.rowsize.data[0],
	"WMO": ds.WMO.data[0],
	"expno": ds.expno.data[0],
	"deploy_date": datetime.datetime.fromtimestamp(int(ds.deploy_date.data[0]), datetime.timezone.utc),
	"deploy_lon": ds.deploy_lon.data[0],
	"deploy_lat": ds.deploy_lat.data[0],
	"end_date": datetime.datetime.fromtimestamp(int(ds.end_date.data[0]), datetime.timezone.utc),
	"end_lon": ds.end_lon.data[0],
	"end_lat": ds.end_lat.data[0],
	"drogue_lost_date": datetime.datetime.fromtimestamp(int(ds.drogue_lost_date.data[0]), datetime.timezone.utc),
	"typedeath": ds.typedeath.data[0],
	"typebuoy": ds.typebuoy.data[0].decode("utf-8").strip()
}

print(meta)

# generate point data objects
data = []
for i in range(meta['rowsize']):
	point = {
		"driftID": int(ds.ID.data[0]), # foreign key to meta table
		"geolocation": {
			"type": "Point",
			"coordinates": [ds.longitude.data[0][i], ds.latitude.data[0][i]]
		},
		"basin": find_basin(ds.longitude.data[0][i], ds.latitude.data[0][i]),
		"data_type": "drifter",
		"date_updated_argovis": datetime.datetime.now(),
		"source_info": {
			"source": ["TBD"]
		},
		"timestamp": datetime.datetime.fromtimestamp(int(ds.time.data[0][i]), datetime.timezone.utc),
		"data_keys": ["ve", "vn", "err_lon", "err_lat", "err_ve", "err_vn", "gap", "sst", "sst1", "sst2", "err_sst", "err_sst1", "err_sst2", "flg_sst", "flg_sst1", "flg_sst2"],
		"data": [ds.ve.data[0][i], ds.vn.data[0][i], ds.err_lon.data[0][i], ds.err_lat.data[0][i], ds.err_ve.data[0][i], ds.err_vn.data[0][i], ds.err_vn.data[0][i], ds.gap.data[0][i], ds.sst.data[0][i], ds.sst.data[0][i], ds.sst1.data[0][i], ds.sst2.data[0][i],ds.err_sst.data[0][i], ds.err_sst1.data[0][i], ds.err_sst2.data[0][i], ds.flg_sst.data[0][i], ds.flg_sst1.data[0][i], ds.flg_sst2.data[0][i]]
	}
	print(point)








