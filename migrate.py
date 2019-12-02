import copy
import uuid

import event_model


class Migration(event_model.DocumentRouter):

    def __init__(self, image_field, timestamp_field, serializer):
        self.image_field = image_field
        self.timestamp_field = timestamp_field
        self.serializer = serializer
        self.new_res_uids = {}
        self.new_datum_ids = {}

    def descriptor(self, doc):
        # Mutate in palce to add a data key for the timestamp.
        try:
            data_key = doc['data_keys'][self.image_field].copy()
        except KeyError:
            return
        data_key['shape'] = data_key['shape'][:1]
        doc['data_keys'][self.timestamp_field] = data_key

    def resource(self, doc):
        # Make a second resource and serialize it. Do not mutate original.
        assert 'uid' in doc
        resource_copy = doc.copy()
        new_res_uid = str(uuid.uuid4())
        self.new_res_uids[doc['uid']] = new_res_uid
        resource_copy['uid'] = new_res_uid
        resource_copy['spec'] = 'AD_HDF5_TS'
        self.serializer('resource', resource_copy)

    def datum_page(self, doc):
        # Make a second datum_page and serialize it. Do not mutate original.
        datum_page_copy = copy.deepcopy(doc)
        new_res_uid = self.new_res_uids[doc['resource']]
        for i, datum_id in enumerate(doc['datum_id']):
            new_datum_id = f'{new_res_uid}/{i}'
            datum_page_copy['datum_id'][i] = new_datum_id
            self.new_datum_ids[datum_id] = new_datum_id
        self.serializer('datum_page', datum_page_copy)

    def event(self, doc):
        data = doc['data']
        if self.image_field in data:
            datum_id = data[self.image_field]
            new_datum_id = self.new_datum_ids[datum_id]
            data[self.timestamp_field] = new_datum_id
            if 'filled' in doc:
                doc['filled'][self.timestamp_field] = False

    def event_page(self, doc):
        raise NotImplementedError

    def __call__(self, name, doc):
        # Give the methods above the opportunity the mutate the document before
        # it is serialize. Note that some make a *copy* and serialize that as
        # well.
        super().__call__(name, doc)
        self.serializer(name, doc)


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
