for f in $(ls /tmp/drifters)
do
  python parse-drifter.py /tmp/drifters/$f
done
