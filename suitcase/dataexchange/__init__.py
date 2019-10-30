# Suitcase subpackages should follow strict naming and interface conventions.
# The public API must include Serializer and should include export if it is
# intended to be user-facing. They should accept the parameters sketched here,
# but may also accpet additional required or optional keyword arguments, as
# needed.
import event_model
import h5py
from pathlib import Path
import suitcase.utils
from ._version import get_versions

__version__ = get_versions()['version']
del get_versions


def export(gen, directory, file_prefix='{uid}-', **kwargs):
    """
    Export a stream of documents to dataexchange.

    .. note::

        This can alternatively be used to write data to generic buffers rather
        than creating files on disk. See the documentation for the
        ``directory`` parameter below.

    Parameters
    ----------
    gen : generator
        expected to yield ``(name, document)`` pairs

    directory : string, Path or Manager.
        For basic uses, this should be the path to the output directory given
        as a string or Path object. Use an empty string ``''`` to place files
        in the current working directory.

        In advanced applications, this may direct the serialized output to a
        memory buffer, network socket, or other writable buffer. It should be
        an instance of ``suitcase.utils.MemoryBufferManager`` and
        ``suitcase.utils.MultiFileManager`` or any object implementing that
        interface. See the suitcase documentation at
        https://nsls-ii.github.io/suitcase for details.

    file_prefix : str, optional
        The first part of the filename of the generated output files. This
        string may include templates as in ``{proposal_id}-{sample_name}-``,
        which are populated from the RunStart document. The default value is
        ``{uid}-`` which is guaranteed to be present and unique. A more
        descriptive value depends on the application and is therefore left to
        the user.

    **kwargs : kwargs
        Keyword arugments to be passed through to the underlying I/O library.

    Returns
    -------
    artifacts : dict
        dict mapping the 'labels' to lists of file names (or, in general,
        whatever resources are produced by the Manager)

    Examples
    --------

    Generate files with unique-identifier names in the current directory.

    >>> export(gen, '')

    Generate files with more readable metadata in the file names.

    >>> export(gen, '', '{plan_name}-{motors}-')

    Include the experiment's start time formatted as YYYY-MM-DD_HH-MM.

    >>> export(gen, '', '{time:%Y-%m-%d_%H:%M}-')

    Place the files in a different directory, such as on a mounted USB stick.

    >>> export(gen, '/path/to/my_usb_stick')
    """
    with Serializer(directory, file_prefix, **kwargs) as serializer:
        for item in gen:
            serializer(*item)

    return serializer.artifacts


class Serializer(event_model.DocumentRouter):
    """
    Serialize a stream of documents to dataexchange.

    .. note::

        This can alternatively be used to write data to generic buffers rather
        than creating files on disk. See the documentation for the
        ``directory`` parameter below.

    Parameters
    ----------
    directory : string, Path, or Manager
        For basic uses, this should be the path to the output directory given
        as a string or Path object. Use an empty string ``''`` to place files
        in the current working directory.

        In advanced applications, this may direct the serialized output to a
        memory buffer, network socket, or other writable buffer. It should be
        an instance of ``suitcase.utils.MemoryBufferManager`` and
        ``suitcase.utils.MultiFileManager`` or any object implementing that
        interface. See the suitcase documentation at
        https://nsls-ii.github.io/suitcase for details.

    file_prefix : str, optional
        The first part of the filename of the generated output files. This
        string may include templates as in ``{proposal_id}-{sample_name}-``,
        which are populated from the RunStart document. The default value is
        ``{uid}-`` which is guaranteed to be present and unique. A more
        descriptive value depends on the application and is therefore left to
        the user.

    **kwargs : kwargs
        Keyword arugments to be passed through to the underlying I/O library.

    Attributes
    ----------
    artifacts
        dict mapping the 'labels' to lists of file names (or, in general,
        whatever resources are produced by the Manager)
    """
    def __init__(self, directory, file_prefix='{uid}-', **kwargs):

        self._file_prefix = file_prefix
        self._kwargs = kwargs
        self._templated_file_prefix = ''  # set when we get a 'start' document

        if isinstance(directory, (str, Path)):
            # The user has given us a filepath; they want files.
            # Set up a MultiFileManager for them.
            self._manager = suitcase.utils.MultiFileManager(directory)
        else:
            # The user has given us their own Manager instance. Use that.
            self._manager = directory

        # Finally, we usually need some state related to stashing file
        # handles/buffers. For a Serializer that only needs *one* file
        # this may be:
        #
        self._output_file = None 
        self._baseline_added = False
        #
        # For a Serializer that writes a separate file per stream:
        #
        # self._files = {}

    @property
    def artifacts(self):
        # The 'artifacts' are the manager's way to exposing to the user a
        # way to get at the resources that were created. For
        # `MultiFileManager`, the artifacts are filenames.  For
        # `MemoryBuffersManager`, the artifacts are the buffer objects
        # themselves. The Serializer, in turn, exposes that to the user here.
        #
        # This must be a property, not a plain attribute, because the
        # manager's `artifacts` attribute is also a property, and we must
        # access it anew each time to be sure to get the latest contents.
        return self._manager.artifacts

    def close(self):
        """
        Close all of the resources (e.g. files) allocated.
        """
        self._manager.close()

    # These methods enable the Serializer to be used as a context manager:
    #
    # with Serializer(...) as serializer:
    #     ...
    #
    # which always calls close() on exit from the with block.

    def __enter__(self):
        return self

    def __exit__(self, *exception_details):
        self.close()

    # Each of the methods below corresponds to a document type. As
    # documents flow in through Serializer.__call__, the DocumentRouter base
    # class will forward them to the method with the name corresponding to
    # the document's type: RunStart documents go to the 'start' method,
    # etc.
    #
    # In each of these methods:
    #
    # - If needed, obtain a new file/buffer from the manager and stash it
    #   on instance state (self._files, etc.) if you will need it again
    #   later. Example:
    #
    #   filename = f'{self._templated_file_prefix}-primary.csv'
    #   file = self._manager.open('stream_data', filename, 'xt')
    #   self._files['primary'] = file
    #
    #   See the manager documentation below for more about the arguments to open().
    #
    # - Write data into the file, usually something like:
    #
    #   content = my_function(doc)
    #   file.write(content)
    #
    #   or
    #
    #   my_function(doc, file)

    def start(self, doc):
        # Fill in the file_prefix with the contents of the RunStart document.
        # As in, '{uid}' -> 'c1790369-e4b2-46c7-a294-7abfa239691a'
        # or 'my-data-from-{plan-name}' -> 'my-data-from-scan'

        self._templated_file_prefix = self._file_prefix.format(**doc)
        
        filename = f'{self._templated_file_prefix}.h5'
        file = self._manager.open('stream_data', filename, 'xb')
        self._output_file = h5py.File(file)

        x_eng = doc.get('XEng', doc['x_ray_energy'])
        chunk_size = doc['chunk_size']

        self._output_file.create_dataset('note', data = doc['note'])
        self._output_file.create_dataset('uid', data = doc['uid'])
        self._output_file.create_dataset('scan_id', data = doc['scan_id'])
        self._output_file.create_dataset('scan_time', data = doc['scan_time'])
        self._output_file.create_dataset('X_eng', data = x_eng)
        
        
        

        self._output_file.create_dataset('img_bkg', data = np.array(img_bkg, dtype=np.int16))
        self._output_file.create_dataset('img_dark', data = np.array(img_dark, dtype=np.int16))
        self._output_file.create_dataset('img_bkg_avg', data = np.array(img_bkg_avg, dtype=np.float32))
        hf.create_dataset('img_dark_avg', data = np.array(img_dark_avg, dtype=np.float32))
        hf.create_dataset('img_tomo', data = np.array(img_tomo, dtype=np.int16))
        hf.create_dataset('angle', data = img_angle)

    
    def descriptor(self, doc):
        if doc['name'] == 'baseline':
            self._baseline_descriptor_uid = doc['uid']
        
        elif doc['name'] == 'primary':
            img_shape = doc['data_keys']['Andor_image']['shape'][:2]
            self._output_file.create_dataset('/exchange/data_white', 
                                             shape = img_shape, data = None)
            self._output_file.create_dataset('/exchange/data_dark',
                                             shape = img_shape, data = None)
            self._output_file.create_dataset('/exchange/data',
                                             maxshape=(None, *img_shape),
                                             chunks=(5, *img_shape),
                                             shape=(0, *img_shape), data = None)
        elif doc['name'] == "zps_pi_r_monitor":
            self._output_file.create_dataset('/exchange/theta', 
                                             maxshape=(None,),
                                             chunks=(1500,),
                                             shape = (0,), data = None)
            #self._output_file.create_dataset('img_bkg_avg', data = None)
            #self._output_file.create_dataset('img_dark_avg', data = np.array(img_dark_avg, dtype=np.float32))
            #self._output_file.create_dataset('img_tomo', data = np.array(img_tomo, dtype=np.int16))
            #self._output_file.create_dataset('angle', data = img_angle)


    def event_page(self, doc):
        # There are other representations of Event data -- 'event' and
        # 'bulk_events' (deprecated). But that does not concern us because
        # DocumentRouter will convert this representations to 'event_page'
        # then route them through here.
        if not self._baseline_added and 
                (doc['descriptor'] == self._baseline_descriptor_uid):
            x_pos =  doc['data']['zps_sx'][0]
            y_pos =  doc['data']['zps_sy'][0]
            z_pos =  doc['data']['zps_sz'][0]
            r_pos =  doc['data']['zps_pi_r'][0] 
            self._output_file.create_dataset('x_ini', data = x_pos)
            self._output_file.create_dataset('y_ini', data = y_pos)
            self._output_file.create_dataset('z_ini', data = z_pos)
            self._output_file.create_dataset('r_ini', data = r_pos)
            self._baseline_added = True
        

    def stop(self, doc):
        ...


def export_fly_scan(h):      
    uid = h.start['uid']
    note = h.start['note']
    scan_type = 'fly_scan'
    scan_id = h.start['scan_id']   
    scan_time = h.start['time'] 
    x_pos =  h.table('baseline')['zps_sx'][1]
    y_pos =  h.table('baseline')['zps_sy'][1]
    z_pos =  h.table('baseline')['zps_sz'][1]
    r_pos =  h.table('baseline')['zps_pi_r'][1]   
    
    
    try:
        x_eng = h.start['XEng']
    except:
        x_eng = h.start['x_ray_energy']
    chunk_size = h.start['chunk_size']
 
 # sanity check: make sure we remembered the right stream name
    assert 'zps_pi_r_monitor' in h.stream_names
    pos = h.table('zps_pi_r_monitor')
    imgs = np.array(list(h.data('Andor_image')))
    img_dark = imgs[0]
    img_bkg = imgs[-1]
    s = img_dark.shape
    img_dark_avg = np.mean(img_dark, axis=0).reshape(1, s[1], s[2])
    img_bkg_avg = np.mean(img_bkg, axis=0).reshape(1, s[1], s[2])

    imgs = imgs[1:-1]
    s1 = imgs.shape
    imgs =imgs.reshape([s1[0]*s1[1], s1[2], s1[3]])

    with db.reg.handler_context({'AD_HDF5': AreaDetectorHDF5TimestampHandler}):
        chunked_timestamps = list(h.data('Andor_image'))
    
    chunked_timestamps = chunked_timestamps[1:-1]
    raw_timestamps = []
    for chunk in chunked_timestamps:
        raw_timestamps.extend(chunk.tolist())

    timestamps = convert_AD_timestamps(pd.Series(raw_timestamps))
    pos['time'] = pos['time'].dt.tz_localize('US/Eastern')

    img_day, img_hour = timestamps.dt.day, timestamps.dt.hour, 
    img_min, img_sec, img_msec = timestamps.dt.minute, timestamps.dt.second, timestamps.dt.microsecond
    img_time = img_day * 86400 + img_hour * 3600 + img_min * 60 + img_sec + img_msec * 1e-6
    img_time = np.array(img_time)

    mot_day, mot_hour = pos['time'].dt.day, pos['time'].dt.hour, 
    mot_min, mot_sec, mot_msec = pos['time'].dt.minute, pos['time'].dt.second, pos['time'].dt.microsecond
    mot_time = mot_day * 86400 + mot_hour * 3600 + mot_min * 60 + mot_sec + mot_msec * 1e-6
    mot_time =  np.array(mot_time)

    mot_pos = np.array(pos['zps_pi_r'])
    offset = np.min([np.min(img_time), np.min(mot_time)])
    img_time -= offset
    mot_time -= offset
    mot_pos_interp = np.interp(img_time, mot_time, mot_pos)
    
    pos2 = mot_pos_interp.argmax() + 1
    img_angle = mot_pos_interp[:pos2-chunk_size] # rotation angles
    img_tomo = imgs[:pos2-chunk_size]  # tomo images
    
    fname = scan_type + '_id_' + str(scan_id) + '.h5'  

    with h5py.File(fname, 'w') as hf:

    try:
        write_lakeshore_to_file(h, fname)
    except:
        print('fails to write lakeshore info into {fname}')
    
    del img_tomo
    del img_dark
    del img_bkg
    del imgs
