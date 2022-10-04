This repo manages the parsing and upload of Global Drifter Program data to Argovis.

## Rebuilding from scratch

 - Make sure the drifter collections are set up in MongoDB per `drifters.py` in [https://github.com/argovis/db-schema](https://github.com/argovis/db-schema)
 - Build the image decribed in `Dockerfile`.
 - Run a container or pod based on this image, mounting appropriate storage for the raw drifter data to `/tmp/drifters` inside the container; in the container, run `python download-drifter.py`. Once complete, you'll have the raw drifter data downlaoded and ready to parse into MongoDB.
 - Make another container or pod from the same image with the same storage mount, make sure it's in the appropriate Kube namespace or docker container network to talk to your MongoDB, and run `bash loaddata.sh` inside it to repopulate MongoDB.
 - [Optional] See the `roundtrip` content for some scripts to proofread everything that got written to MongoDB.