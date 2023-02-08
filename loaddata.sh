for f in $(ls /tmp/drifters)
do
  python -u parse-drifter.py /tmp/drifters/$f
done
