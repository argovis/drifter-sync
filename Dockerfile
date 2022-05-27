FROM python:3.9

RUN apt-get update -y && apt-get install -y nano
RUN apt-get upgrade -y subversion
RUN pip install numpy tqdm xarray netcdf4 geopy pymongo

WORKDIR /app
COPY *.py .
COPY *.sh .
COPY parameters/basinmask_01.nc parameters/basinmask_01.nc
RUN chown -R 1000660000 /app