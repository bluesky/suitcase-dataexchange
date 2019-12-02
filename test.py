from intake import open_catalog
raw_catalog = open_catalog('/home/dallan/Downloads/raw-data/fxi.yml')
repaired_catalog = open_catalog('/home/dallan/Downloads/repaired-data/fxi.yml')
run = repaired_catalog['fxi'][-1]
import suitcase.dataexchange
with suitcase.dataexchange.Serializer('/tmp/test_hdf5') as serializer:
    for name, doc in run.canonical(fill='yes'):
        serializer(name, doc)
