# check to make sure rowsize in each drifterMeta document matches the number of documents pointing to that meta doc from the drifters collection

from pymongo import MongoClient

client = MongoClient('mongodb://database/argo')
db = client.argo

metaids = list(db.drifterMeta.distinct('_id'))

for metaid in metaids:
	#print('metadata id:', metaid)
	rowsize = list(db.drifterMeta.find({'_id':metaid}))[0]['rowsize']
	#print('rowsize:', rowsize)
	count = db.drifters.count_documents({'platform':metaid})
	#print('drifter doc count:', count)
	if rowsize != count:
		print('doc count mismatch on metadata id', metaid, '; rowsize =', rowsize, ', count =', count)