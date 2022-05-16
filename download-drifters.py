import urllib.request
import numpy as np
import re
import concurrent.futures 
from os.path import isfile, join
from tqdm import tqdm

# --- raw file retrieval -----------------------------------
# inherited from https://github.com/Cloud-Drift/earthcube-meeting-2022/blob/main/PM_05_Accelerating_Lagrangian_analyses_of_oceanic_data_benchmarking_typical_workflows.ipynb
subset_nb_drifters = 17324  # you can scale up/down this number (maximum value of 17324)
# output folder and official GDP https server
# Note: If you are running this notebook on a local computer and have already downloaded the individual NetCDF files 
# independently of this notebook, you can move/copy these files to the folder destination shown below, 
# or alternatively change the variable 'folder' to your folder with the data
folder =  '/data/raw/'
input_url = 'https://www.aoml.noaa.gov/ftp/pub/phod/lumpkin/hourly/v2.00/netcdf/'

# load the complete list of drifter IDs from the AOML https
urlpath = urllib.request.urlopen(input_url)
string = urlpath.read().decode('utf-8')
pattern = re.compile('drifter_[0-9]*.nc')
filelist = pattern.findall(string)
list_id = np.unique([int(f.split('_')[-1][:-3]) for f in filelist])

# Here we "randomly" select a subset of ID numbers but produce reproducible results
# by actually setting the seed of the random generator
rng = np.random.RandomState(42)  # reproducible results
subset_id = sorted(rng.choice(list_id, subset_nb_drifters, replace=False))

def fetch_netcdf(url, file):
    '''
    Download and save file from the given url (if not present)
    '''
    if not isfile(file):
        req = urllib.request.urlretrieve(url, file)

# Typically it should take ~2 min for 500 drifters
print(f'Fetching the {subset_nb_drifters} requested netCDF files (as a reference ~2min for 500 files).')
with concurrent.futures.ThreadPoolExecutor() as exector:
    # create list of urls and paths
    urls = []
    files = []
    for i in subset_id:
        file = f'drifter_{i}.nc'
        urls.append(join(input_url, file))
        files.append(join(folder, file))
    
    # parallel retrieving of individual netCDF files
    list(tqdm(exector.map(fetch_netcdf, urls, files), total=len(files)))