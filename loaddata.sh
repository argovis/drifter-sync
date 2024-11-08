for f in $(ls /tmp/drifters/drifter*.nc)
do
  python -u parse-drifter.py /tmp/drifters/$f
done

python summarize.py