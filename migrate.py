import event_model
import uuid


class Migration(event_model.DocumentRouter):

    def __init__(self, image_field, timestamp_field, serializer):
        self.image_field = image_field
        self.timestamp_field = timestamp_field
        self.serializer = serializer
        self.new_res_uids = {}
        self.new_datum_ids = {}

    def resource(self, doc):
        assert 'uid' in doc
        resource_copy = doc.copy()
        new_res_uid = str(uuid.uuid4())
        self.new_res_uids[doc['uid']] = new_res_uid
        resource_copy['uid'] = new_res_uid
        resource_copy['spec'] = 'AD_HDF5_TS'
        self.serializer('resource', resource_copy)

    def datum_page(self, doc):
        datum_page_copy = doc.copy()
        new_res_uid = self.new_res_uids[doc['resource']]
        for i, datum_id in enumerate(doc['datum_id']):
            new_datum_id = f'{new_res_uid}/{i}'
            datum_page_copy['datum_id'][i] = new_datum_id
            self.new_datum_ids[datum_id] = new_datum_id
        self.serializer('datum_page', datum_page_copy)

    def event_page(self, doc):
        data = doc['data']
        if self.image_field in data:
            for i, datum_id in enumerate(data[self.image_field]):
                new_datum_id = self.new_datum_ids[datum_id]
                data[self.timestamp_field] = new_datum_id

    def __call__(self, name, doc):
        self.serializer(name, doc)
        super().__call__(name, doc)


if __name__ == '__main__':
    # TODO Strip out local paths, of course.
    from tqdm import tqdm
    from intake import open_catalog
    catalog = open_catalog('/home/dallan/Downloads/raw-data/fxi.yml')
    run = catalog['fxi'][-1]
    import suitcase.msgpack
    with suitcase.msgpack.Serializer('/home/dallan/Downloads/repaired-data') as serializer:
        migration = Migration('Andor_image', 'Andor_timestamp', serializer)
        for name, doc in tqdm(run.canonical(fill='no')):
            migration(name, doc)
