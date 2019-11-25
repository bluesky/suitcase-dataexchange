# Suitcase subpackages should follow strict naming and interface conventions.
# The public API must include Serializer and should include export if it is
# intended to be user-facing. They should accept the parameters sketched here,
# but may also accpet additional required or optional keyword arguments, as
# needed.
import event_model
import h5py
import numpy as np
from pathlib import Path
import suitcase.utils
from ._version import get_versions
from collections import defaultdict
from area_detector_handlers.handlers import (
    AreaDetectorHDF5TimestampHandler, AreaDetectorHDF5Handler, HandlerBase)

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
        self._descriptor_uids = {}
        self._baseline_added = False
        self._stream_count = defaultdict(lambda: 0)
        self._buffered_thetas = []
        self._theta_timestamps = []
        self._image_timestamps= []
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

        # x_eng = doc.get('XEng', doc['x_ray_energy'])
        self._chunk_size = doc['chunk_size']

        # self._output_file.create_dataset('note', data = doc['note'])
        # self._output_file.create_dataset('uid', data = doc['uid'])
        # self._output_file.create_dataset('scan_id', data = doc['scan_id'])
        # self._output_file.create_dataset('scan_time', data = doc['scan_time'])
        # self._output_file.create_dataset('X_eng', data = x_eng)


    def descriptor(self, doc):

        if doc['name'] == 'baseline':
            self._descriptor_uids['baseline'] = doc['uid']

        elif doc['name'] == 'primary':
            self._descriptor_uids['primary'] = doc['uid']
            self._img_shape = (doc['data_keys']['Andor_image']['shape'][1],
                               doc['data_keys']['Andor_image']['shape'][0])
            self._output_file.create_dataset('/exchange/data_white',
                                             shape = self._img_shape,
                                             data = None)
            self._output_file.create_dataset('/exchange/data_dark',
                                             shape = self._img_shape,
                                             data = None)
            self._output_file.create_dataset('/exchange/data',
                                             maxshape=(None, *self._img_shape),
                                             chunks=(5, *self._img_shape),
                                             shape=(0, *self._img_shape), data = None)
        elif doc['name'] == "zps_pi_r_monitor":
            self._descriptor_uids['zps_pi_r_monitor'] = doc['uid']
            self._output_file.create_dataset('/exchange/theta',
                                             maxshape=(None,),
                                             chunks=(1500,),
                                             shape = (0,), data = None)
            #self._output_file.create_dataset('img_bkg_avg', data = None)
            #self._output_file.create_dataset('img_dark_avg',
            #          data = np.array(img_dark_avg, dtype=np.float32))
            #self._output_file.create_dataset('img_tomo',
            #          data = np.array(img_tomo, dtype=np.int16))
            #self._output_file.create_dataset('angle', data = img_angle)


    def event_page(self, doc):
        # There are other representations of Event data -- 'event' and
        # 'bulk_events' (deprecated). But that does not concern us because
        # DocumentRouter will convert this representations to 'event_page'
        # then route them through here.
        self._stream_count[doc['descriptor']] += 1

        if not self._baseline_added and doc['descriptor'] == self._descriptor_uids.get('baseline'):
            x_pos =  doc['data']['zps_sx'][0]
            y_pos =  doc['data']['zps_sy'][0]
            z_pos =  doc['data']['zps_sz'][0]
            r_pos =  doc['data']['zps_pi_r'][0]
            self._output_file.create_dataset('x_ini', data = x_pos)
            self._output_file.create_dataset('y_ini', data = y_pos)
            self._output_file.create_dataset('z_ini', data = z_pos)
            self._output_file.create_dataset('r_ini', data = r_pos)
            self._baseline_added = True

        elif doc['descriptor'] == self._descriptor_uids.get('primary'):

    #pos = h.table('zps_pi_r_monitor')
    #imgs = np.array(list(h.data('Andor_image')))
    #img_dark = imgs[0]
    #img_bkg = imgs[-1]
    #s = img_dark.shape
    #img_dark_avg = np.mean(img_dark, axis=0).reshape(1, s[1], s[2])
    #img_bkg_avg = np.mean(img_bkg, axis=0).reshape(1, s[1], s[2])

            #self._output_file['/exchange/data_white'][:] = None

            if self._stream_count[doc['descriptor']] == 1:
                dark_avg = np.mean(doc['data']['Andor_image'][0], axis=0, keepdims=True)
                self._output_file['/exchange/data_dark'][:] = dark_avg
                start_from = 1
            else:
                start_from = 0
            dataset = self._output_file['/exchange/data']
            for image in doc['data']['Andor_image'][start_from:]:
                dataset.resize((dataset.shape[0] + self._chunk_size, *dataset.shape[1:]))
                dataset[-self._chunk_size:,:,:] = image
            self._image_timestamps.extend(doc['timestamps']['Andor_image'][start_from:])

        elif doc['descriptor'] == self._descriptor_uids.get('zps_pi_r_monitor'):
            self._buffered_thetas.extend(doc['data']['zps_pi_r'])
            self._theta_timestamps.extend(doc['timestamps']['zps_pi_r'])

    def stop(self, doc):
        # Pop off the white frame (the last frame written)
        dataset = self._output_file['/exchange/data']
        white_image = dataset[-self._chunk_size:,:,:]
        white_avg = np.mean(white_image, axis=0, keepdims=True)
        self._output_file['/exchange/data_dark'][:] = white_avg
        # and the junk frame (second to last). It is a junk frame because the
        # motor stopped moving somewhere in the middle.
        dataset.resize((dataset.shape[0] - 2 * self._chunk_size, *dataset.shape[1:]))
        del self._image_timestamps[-2 * self._chunk_size:]

        theta = np.interp(
            self._image_timestamps,
            self._theta_timestamps,
            self._buffered_theta)

        self._output_file.create_dataset('/exchange/theta', data=theta)


class MVPHandler(HandlerBase):
    def __init__(self, filename, frame_per_point=1):
        self.image_handler = AreaDetectorHDF5Handler(
            filename, frame_per_point=frame_per_point)
        self.timestamp_handler = AreaDetectorHDF5TimestampHandler(
            filename, frame_per_point=frame_per_point)

    def __call__(self, point_number):
        return (self.timestamp_handler(point_number),
                self.image_handler(point_number))
