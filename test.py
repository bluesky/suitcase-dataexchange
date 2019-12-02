from intake import open_catalog
raw_catalog = open_catalog('/home/dallan/Downloads/raw-data/fxi.yml')
repaired_catalog = open_catalog('/home/dallan/Downloads/repaired-data/fxi.yml')
run = repaired_catalog['fxi'][-1]
import suitcase.dataexchange
print('entering loop', flush=True)
with suitcase.dataexchange.Serializer('/tmp/test_hdf5') as serializer:
    print('serializer made')
    for name, doc in run.canonical(fill='yes'):
        print(f'sending in {name}', flush=True)
        serializer(name, doc)
    print('fat chance we see this')
