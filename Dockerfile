FROM python:3.9

RUN apt-get update -y && apt-get install -y nano
RUN pip install numpy tqdm xarray netcdf4 geopy

WORKDIR /app
COPY *.py .
COPY parameters/basinmask_01.nc parameters/basinmask_01.nc