# -*- coding: utf-8 -*-
#
# Copyright 2015 Twitter Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""luigi bindings for Google Cloud Storage"""

import io
import logging
import mimetypes
import os
import tempfile
import time
from urllib.parse import urlsplit
from io import BytesIO

from tenacity import retry
from tenacity import retry_if_exception
from tenacity import retry_if_exception_type
from tenacity import wait_exponential
from tenacity import stop_after_attempt
from tenacity import after_log
from luigi.contrib import gcp
import luigi.target
from luigi.format import FileWrapper

logger = logging.getLogger('luigi-interface')

# Retry when following errors happened
RETRYABLE_ERRORS = None

try:
    import httplib2

    from googleapiclient import errors
    from googleapiclient import discovery
    from googleapiclient import http
except ImportError:
    logger.warning("Loading GCS module without the python packages googleapiclient & google-auth. \
        This will crash at runtime if GCS functionality is used.")
else:
    RETRYABLE_ERRORS = (httplib2.HttpLib2Error, IOError)

try:
    from google.cloud import storage
    from google.cloud.exceptions import NotFound
    import gzip  # used by NewGCSClient, only needed if above succeed
except ImportError:
    logger.warning("Loading GCS module without the python packages google-cloud-storage. \
        This will crash at runtime if NewGCSClient is used.")

# Number of bytes to send/receive in each request.
CHUNKSIZE = 10 * 1024 * 1024

# Mimetype to use if one can't be guessed from the file extension.
DEFAULT_MIMETYPE = 'application/octet-stream'

# Time to sleep while waiting for eventual consistency to finish.
EVENTUAL_CONSISTENCY_SLEEP_INTERVAL = 0.1

# Maximum number of sleeps for eventual consistency.
EVENTUAL_CONSISTENCY_MAX_SLEEPS = 300

# Uri for batch requests
GCS_BATCH_URI = 'https://storage.googleapis.com/batch/storage/v1'

# Used for decompressive transcoding, when enabled (NewGCSClient only.)
GZIPPABLE_MIMETYPES = [
    'application/javascript',
    'application/json',
    'application/jsonlines',
    'application/rss+xml',
    'application/x-javascript',
    'application/x-jsonlines',
    'application/x-ndjson',
    'application/xhtml+xml',
    'application/xml',
    'text/css',
    'text/csv',
    'text/html',
    'text/javascript',
    'text/plain',
    'text/tab-separated-values',
    'text/tsv',
    'text/xml'
]

# Supplement gaps in the mimetypes package for common data formats (NewGCSClient only.)
MIMETYPE_MAP = {
    'avro': 'application/avro',
    'ndjson': 'application/x-jsonlines'
}


def update_mimetypes(mapping):
    for type, ext in mapping.items():
        mimetypes.add_type(type, ext)


# Retry configurations. For more details, see https://tenacity.readthedocs.io/en/latest/
def is_error_5xx(err):
    return isinstance(err, errors.HttpError) and err.resp.status >= 500


gcs_retry = retry(retry=(retry_if_exception(is_error_5xx) | retry_if_exception_type(RETRYABLE_ERRORS)),
                  wait=wait_exponential(multiplier=1, min=1, max=10),
                  stop=stop_after_attempt(5),
                  reraise=True,
                  after=after_log(logger, logging.WARNING))


def _wait_for_consistency(checker):
    """Eventual consistency: wait until GCS reports something is true.

    This is necessary for e.g. create/delete where the operation might return,
    but won't be reflected for a bit.
    """
    for _ in range(EVENTUAL_CONSISTENCY_MAX_SLEEPS):
        if checker():
            return

        time.sleep(EVENTUAL_CONSISTENCY_SLEEP_INTERVAL)

    logger.warning('Exceeded wait for eventual GCS consistency - this may be a'
                   'bug in the library or something is terribly wrong.')


class InvalidDeleteException(luigi.target.FileSystemException):
    pass


class GCSClient(luigi.target.FileSystem):
    """An implementation of a FileSystem over Google Cloud Storage.

       There are several ways to use this class. By default it will use the app
       default credentials, as described at https://developers.google.com/identity/protocols/application-default-credentials .
       Alternatively, you may pass an google-auth credentials object. e.g. to use a service account::

         credentials = google.auth.jwt.Credentials.from_service_account_info(
             '012345678912-ThisIsARandomServiceAccountEmail@developer.gserviceaccount.com',
             'These are the contents of the p12 file that came with the service account',
             scope='https://www.googleapis.com/auth/devstorage.read_write')
         client = GCSClient(oauth_credentials=credentails)

        The chunksize parameter specifies how much data to transfer when downloading
        or uploading files.

    .. warning::
      By default this class will use "automated service discovery" which will require
      a connection to the web. The google api client downloads a JSON file to "create" the
      library interface on the fly. If you want a more hermetic build, you can pass the
      contents of this file (currently found at https://www.googleapis.com/discovery/v1/apis/storage/v1/rest )
      as the ``descriptor`` argument.
    """
    def __init__(self, oauth_credentials=None, descriptor='', http_=None,
                 chunksize=CHUNKSIZE, **discovery_build_kwargs):
        self.chunksize = chunksize
        authenticate_kwargs = gcp.get_authenticate_kwargs(oauth_credentials, http_)

        build_kwargs = authenticate_kwargs.copy()
        build_kwargs.update(discovery_build_kwargs)

        if descriptor:
            self.client = discovery.build_from_document(descriptor, **build_kwargs)
        else:
            build_kwargs.setdefault('cache_discovery', False)
            self.client = discovery.build('storage', 'v1', **build_kwargs)

    def _path_to_bucket_and_key(self, path):
        (scheme, netloc, path, _, _) = urlsplit(path)
        assert scheme == 'gs'
        path_without_initial_slash = path[1:]
        return netloc, path_without_initial_slash

    def _is_root(self, key):
        return len(key) == 0 or key == '/'

    def _add_path_delimiter(self, key):
        return key if key[-1:] == '/' else key + '/'

    @gcs_retry
    def _obj_exists(self, bucket, obj):
        try:
            self.client.objects().get(bucket=bucket, object=obj).execute()
        except errors.HttpError as ex:
            if ex.resp['status'] == '404':
                return False
            raise
        else:
            return True

    def _list_iter(self, bucket, prefix):
        request = self.client.objects().list(bucket=bucket, prefix=prefix)
        response = request.execute()

        while response is not None:
            for it in response.get('items', []):
                yield it

            request = self.client.objects().list_next(request, response)
            if request is None:
                break

            response = request.execute()

    @gcs_retry
    def _do_put(self, media, dest_path):
        bucket, obj = self._path_to_bucket_and_key(dest_path)

        request = self.client.objects().insert(bucket=bucket, name=obj, media_body=media)
        if not media.resumable():
            return request.execute()

        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                logger.debug('Upload progress: %.2f%%', 100 * status.progress())

        _wait_for_consistency(lambda: self._obj_exists(bucket, obj))
        return response

    def exists(self, path):
        bucket, obj = self._path_to_bucket_and_key(path)
        if self._obj_exists(bucket, obj):
            return True

        return self.isdir(path)

    def isdir(self, path):
        bucket, obj = self._path_to_bucket_and_key(path)
        if self._is_root(obj):
            try:
                self.client.buckets().get(bucket=bucket).execute()
            except errors.HttpError as ex:
                if ex.resp['status'] == '404':
                    return False
                raise

        obj = self._add_path_delimiter(obj)
        if self._obj_exists(bucket, obj):
            return True

        # Any objects with this prefix
        resp = self.client.objects().list(bucket=bucket, prefix=obj, maxResults=20).execute()
        lst = next(iter(resp.get('items', [])), None)
        return bool(lst)

    def remove(self, path, recursive=True):
        (bucket, obj) = self._path_to_bucket_and_key(path)

        if self._is_root(obj):
            raise InvalidDeleteException(
                'Cannot delete root of bucket at path {}'.format(path))

        if self._obj_exists(bucket, obj):
            self.client.objects().delete(bucket=bucket, object=obj).execute()
            _wait_for_consistency(lambda: not self._obj_exists(bucket, obj))
            return True

        if self.isdir(path):
            if not recursive:
                raise InvalidDeleteException(
                    'Path {} is a directory. Must use recursive delete'.format(path))

            req = http.BatchHttpRequest(batch_uri=GCS_BATCH_URI)
            for it in self._list_iter(bucket, self._add_path_delimiter(obj)):
                req.add(self.client.objects().delete(bucket=bucket, object=it['name']))
            req.execute()

            _wait_for_consistency(lambda: not self.isdir(path))
            return True

        return False

    def put(self, filename, dest_path, mimetype=None, chunksize=None):
        chunksize = chunksize or self.chunksize
        resumable = os.path.getsize(filename) > 0

        mimetype = mimetype or mimetypes.guess_type(dest_path)[0] or DEFAULT_MIMETYPE
        media = http.MediaFileUpload(filename, mimetype=mimetype, chunksize=chunksize, resumable=resumable)

        self._do_put(media, dest_path)

    def _forward_args_to_put(self, kwargs):
        return self.put(**kwargs)

    def put_multiple(self, filepaths, remote_directory, mimetype=None, chunksize=None, num_process=1):
        if isinstance(filepaths, str):
            raise ValueError(
                'filenames must be a list of strings. If you want to put a single file, '
                'use the `put(self, filename, ...)` method'
            )

        put_kwargs_list = [
            {
                'filename': filepath,
                'dest_path': os.path.join(remote_directory, os.path.basename(filepath)),
                'mimetype': mimetype,
                'chunksize': chunksize,
            }
            for filepath in filepaths
        ]

        if num_process > 1:
            from multiprocessing import Pool
            from contextlib import closing
            with closing(Pool(num_process)) as p:
                return p.map(self._forward_args_to_put, put_kwargs_list)
        else:
            for put_kwargs in put_kwargs_list:
                self._forward_args_to_put(put_kwargs)

    def put_string(self, contents, dest_path, mimetype=None):
        mimetype = mimetype or mimetypes.guess_type(dest_path)[0] or DEFAULT_MIMETYPE
        assert isinstance(mimetype, str)
        if not isinstance(contents, bytes):
            contents = contents.encode("utf-8")
        media = http.MediaIoBaseUpload(BytesIO(contents), mimetype, resumable=bool(contents))
        self._do_put(media, dest_path)

    def mkdir(self, path, parents=True, raise_if_exists=False):
        if self.exists(path):
            if raise_if_exists:
                raise luigi.target.FileAlreadyExists()
            elif not self.isdir(path):
                raise luigi.target.NotADirectory()
            else:
                return

        self.put_string(b"", self._add_path_delimiter(path), mimetype='text/plain')

    def copy(self, source_path, destination_path):
        src_bucket, src_obj = self._path_to_bucket_and_key(source_path)
        dest_bucket, dest_obj = self._path_to_bucket_and_key(destination_path)

        if self.isdir(source_path):
            src_prefix = self._add_path_delimiter(src_obj)
            dest_prefix = self._add_path_delimiter(dest_obj)

            source_path = self._add_path_delimiter(source_path)
            copied_objs = []
            for obj in self.listdir(source_path):
                suffix = obj[len(source_path):]

                self.client.objects().copy(
                    sourceBucket=src_bucket,
                    sourceObject=src_prefix + suffix,
                    destinationBucket=dest_bucket,
                    destinationObject=dest_prefix + suffix,
                    body={}).execute()
                copied_objs.append(dest_prefix + suffix)

            _wait_for_consistency(
                lambda: all(self._obj_exists(dest_bucket, obj)
                            for obj in copied_objs))
        else:
            self.client.objects().copy(
                sourceBucket=src_bucket,
                sourceObject=src_obj,
                destinationBucket=dest_bucket,
                destinationObject=dest_obj,
                body={}).execute()
            _wait_for_consistency(lambda: self._obj_exists(dest_bucket, dest_obj))

    def rename(self, *args, **kwargs):
        """
        Alias for ``move()``
        """
        self.move(*args, **kwargs)

    def move(self, source_path, destination_path):
        """
        Rename/move an object from one GCS location to another.
        """
        self.copy(source_path, destination_path)
        self.remove(source_path)

    def listdir(self, path):
        """
        Get an iterable with GCS folder contents.
        Iterable contains paths relative to queried path.
        """
        bucket, obj = self._path_to_bucket_and_key(path)

        obj_prefix = self._add_path_delimiter(obj)
        if self._is_root(obj_prefix):
            obj_prefix = ''

        obj_prefix_len = len(obj_prefix)
        for it in self._list_iter(bucket, obj_prefix):
            yield self._add_path_delimiter(path) + it['name'][obj_prefix_len:]

    def list_wildcard(self, wildcard_path):
        """Yields full object URIs matching the given wildcard.

        Currently only the '*' wildcard after the last path delimiter is supported.

        (If we need "full" wildcard functionality we should bring in gsutil dependency with its
        https://github.com/GoogleCloudPlatform/gsutil/blob/master/gslib/wildcard_iterator.py...)
        """
        path, wildcard_obj = wildcard_path.rsplit('/', 1)
        assert '*' not in path, "The '*' wildcard character is only supported after the last '/'"
        wildcard_parts = wildcard_obj.split('*')
        assert len(wildcard_parts) == 2, "Only one '*' wildcard is supported"

        for it in self.listdir(path):
            if it.startswith(path + '/' + wildcard_parts[0]) and it.endswith(wildcard_parts[1]) and \
                   len(it) >= len(path + '/' + wildcard_parts[0]) + len(wildcard_parts[1]):
                yield it

    @gcs_retry
    def download(self, path, chunksize=None, chunk_callback=lambda _: False):
        """Downloads the object contents to local file system.

        Optionally stops after the first chunk for which chunk_callback returns True.
        """
        chunksize = chunksize or self.chunksize
        bucket, obj = self._path_to_bucket_and_key(path)

        with tempfile.NamedTemporaryFile(delete=False) as fp:
            # We can't return the tempfile reference because of a bug in python: http://bugs.python.org/issue18879
            return_fp = _DeleteOnCloseFile(fp.name, 'r')

            # Special case empty files because chunk-based downloading doesn't work.
            result = self.client.objects().get(bucket=bucket, object=obj).execute()
            if int(result['size']) == 0:
                return return_fp

            request = self.client.objects().get_media(bucket=bucket, object=obj)
            downloader = http.MediaIoBaseDownload(fp, request, chunksize=chunksize)

            done = False
            while not done:
                _, done = downloader.next_chunk()
                if chunk_callback(fp):
                    done = True

        return return_fp


class NewGCSClient(luigi.target.FileSystem):
    """An(other) implementation of a FileSystem over Google Cloud Storage.

       This implementation supports on-the-fly decompressive transcoding, which can
       reduce at-rest storage of non-compressed data by 90%+. put_multiple() is
       limited to a single process as the underlying Google client cannot be pickled.
    """

    def __init__(self, credentials=None, descriptor='', http_=None, chunksize=CHUNKSIZE, gcs_client_info=None,
                 gcp_project=None, mimetype_map=MIMETYPE_MAP, gzippable_mimetypes=GZIPPABLE_MIMETYPES,
                 decompressive_transcoding=False, **client_options_kwargs):
        self.chunksize = chunksize
        self.gzippable_mimetypes = gzippable_mimetypes
        self.decompressive_transcoding = decompressive_transcoding
        self.client = storage.Client(project=gcp_project, credentials=credentials, _http=http_,
                                     client_info=gcs_client_info, client_options=client_options_kwargs)
        update_mimetypes(mimetype_map)

    def _path_to_bucket_and_key(self, path):
        (scheme, netloc, path, _, _) = urlsplit(path)
        assert scheme == 'gs'
        path_without_initial_slash = path[1:]
        return netloc, path_without_initial_slash

    def _is_root(self, key):
        return len(key) == 0 or key == '/'

    def _add_path_delimiter(self, key):
        return key if key[-1:] == '/' else key + '/'

    @gcs_retry
    def _obj_exists(self, bucket, obj):
        bucket = self.client.get_bucket(bucket)
        blob = bucket.blob(obj)
        return blob.exists()

    def _list_iter(self, bucket, prefix, return_blobs=False):
        blobs = self.client.list_blobs(bucket_or_name=bucket, prefix=prefix)
        for blob in blobs:
            if not return_blobs:
                yield {'name': blob.name, 'updated': blob.updated}
            else:
                yield blob

    @gcs_retry
    def _do_put(self, file_or_filename, dest_path, mimetype, chunksize, content_encoding=None, from_file=False):
        bucket, obj = self._path_to_bucket_and_key(dest_path)
        bucket_obj = self.client.get_bucket(bucket)
        blob = bucket_obj.blob(obj, chunk_size=chunksize)
        blob.content_encoding = content_encoding
        blob.content_type = mimetype
        if from_file:
            blob.upload_from_file(
                file_or_filename, size=file_or_filename.tell(), rewind=True)
        else:
            blob.upload_from_filename(file_or_filename)
        _wait_for_consistency(lambda: self._obj_exists(bucket, obj))

    def exists(self, path):
        bucket, obj = self._path_to_bucket_and_key(path)
        if self._obj_exists(bucket, obj):
            return True

        return self.isdir(path)

    def isdir(self, path):
        bucket, obj = self._path_to_bucket_and_key(path)
        if self._is_root(obj):
            try:
                self.client.get_bucket(bucket)
            except NotFound:
                return False
            except Exception:
                raise

        obj = self._add_path_delimiter(obj)
        if self._obj_exists(bucket, obj):
            return True

        # Any objects with this prefix
        blobs = self.client.list_blobs(
            bucket_or_name=bucket, prefix=obj, max_results=20)
        return bool(list(blobs))

    def remove(self, path, recursive=True):
        (bucket, obj) = self._path_to_bucket_and_key(path)

        if self._is_root(obj):
            raise InvalidDeleteException(
                'Cannot delete root of bucket at path {}'.format(path))

        if self._obj_exists(bucket, obj):
            self.client.bucket(bucket).delete_blob(obj)
            _wait_for_consistency(lambda: not self._obj_exists(bucket, obj))
            return True

        if self.isdir(path):
            if not recursive:
                raise InvalidDeleteException(
                    'Path {} is a directory. Must use recursive delete'.format(path))

            self.client.bucket(bucket).delete_blobs([d['name'] for d in list(
                self._list_iter(bucket, self._add_path_delimiter(obj)))])
            _wait_for_consistency(lambda: not self.isdir(path))
            return True

        return False

    def put(self, filename, dest_path, mimetype=None, chunksize=None, decompressive_transcoding=None):
        chunksize = chunksize or self.chunksize
        content_encoding = None
        mimetype = mimetype or mimetypes.guess_type(dest_path)[0] or DEFAULT_MIMETYPE
        if decompressive_transcoding is None:
            decompressive_transcoding = self.decompressive_transcoding
        if decompressive_transcoding and mimetype in self.gzippable_mimetypes:
            content_encoding = 'gzip'
            with tempfile.NamedTemporaryFile(delete=True, mode='wb') as fp:
                with open(filename, 'rb') as f_in, gzip.open(fp, 'wb') as f_out:
                    f_out.writelines(f_in)
                self._do_put(fp.name, dest_path, mimetype, chunksize, content_encoding)
        else:
            self._do_put(filename, dest_path, mimetype, chunksize, content_encoding)

    def _forward_args_to_put(self, kwargs):
        return self.put(**kwargs)

    def put_multiple(self, filepaths, remote_directory, mimetype=None, chunksize=None,
                     decompressive_transcoding=None, num_process=1):
        if isinstance(filepaths, str):
            raise ValueError(
                'filenames must be a list of strings. If you want to put a single file, '
                'use the `put(self, filename, ...)` method'
            )

        put_kwargs_list = [
            {
                'filename': filepath,
                'dest_path': os.path.join(remote_directory, os.path.basename(filepath)),
                'mimetype': mimetype,
                'chunksize': chunksize,
                'decompressive_transcoding': decompressive_transcoding
            }
            for filepath in filepaths
        ]
        if num_process > 1:
            logger.warning("NewGCSClient is unable to be pickled. Running with num_process=1.")
        for put_kwargs in put_kwargs_list:
            self._forward_args_to_put(put_kwargs)

    def put_string(self, contents, dest_path, mimetype=None, decompressive_transcoding=None):
        content_encoding = None
        mimetype = mimetype or mimetypes.guess_type(dest_path)[0] or DEFAULT_MIMETYPE
        assert isinstance(mimetype, str)
        if not isinstance(contents, bytes):
            contents = contents.encode("utf-8")
        if decompressive_transcoding is None:
            decompressive_transcoding = self.decompressive_transcoding
        with BytesIO() as f:
            if decompressive_transcoding and mimetype in self.gzippable_mimetypes:
                content_encoding = 'gzip'
                with gzip.GzipFile(fileobj=f, mode='wb', compresslevel=9) as fgz:
                    fgz.write(contents)
            else:
                f.write(contents)
            self._do_put(f, dest_path, mimetype, chunksize=None, content_encoding=content_encoding, from_file=True)

    def mkdir(self, path, parents=True, raise_if_exists=False):
        if self.exists(path):
            if raise_if_exists:
                raise luigi.target.FileAlreadyExists()
            elif not self.isdir(path):
                raise luigi.target.NotADirectory()
            else:
                return

        self.put_string(b"", self._add_path_delimiter(
            path), mimetype='text/plain')

    def copy(self, source_path, destination_path):
        src_bucket_str, src_obj = self._path_to_bucket_and_key(source_path)
        dest_bucket_str, dest_obj = self._path_to_bucket_and_key(
            destination_path)

        src_bucket = self.client.bucket(src_bucket_str)
        dest_bucket = self.client.bucket(dest_bucket_str)

        if self.isdir(source_path):
            src_prefix = self._add_path_delimiter(src_obj)
            dest_prefix = self._add_path_delimiter(dest_obj)

            source_path = self._add_path_delimiter(source_path)
            copied_objs = []
            for obj in self.listdir(source_path):
                suffix = obj[len(source_path):]
                src_bucket.copy_blob(
                    src_bucket.blob(
                        src_prefix + suffix), dest_bucket, dest_prefix + suffix
                )
                copied_objs.append(dest_prefix + suffix)

            _wait_for_consistency(
                lambda: all(self._obj_exists(dest_bucket, obj)
                            for obj in copied_objs))
        else:
            src_bucket.copy_blob(
                src_bucket.blob(src_obj), dest_bucket, dest_obj
            )
            _wait_for_consistency(
                lambda: self._obj_exists(dest_bucket, dest_obj))

    def rename(self, *args, **kwargs):
        """
        Alias for ``move()``
        """
        self.move(*args, **kwargs)

    def move(self, source_path, destination_path):
        """
        Rename/move an object from one GCS location to another.
        """
        self.copy(source_path, destination_path)
        self.remove(source_path)

    def listdir(self, path):
        """
        Get an iterable with GCS folder contents.
        Iterable contains paths relative to queried path.
        """
        bucket, obj = self._path_to_bucket_and_key(path)

        obj_prefix = self._add_path_delimiter(obj)
        if self._is_root(obj_prefix):
            obj_prefix = ''

        obj_prefix_len = len(obj_prefix)
        for it in self._list_iter(bucket, obj_prefix):
            yield self._add_path_delimiter(path) + it['name'][obj_prefix_len:]

    def list_wildcard(self, wildcard_path):
        """Yields full object URIs matching the given wildcard.
        Currently only the '*' wildcard after the last path delimiter is supported.
        (If we need "full" wildcard functionality we should bring in gsutil dependency with its
        https://github.com/GoogleCloudPlatform/gsutil/blob/master/gslib/wildcard_iterator.py...)
        """
        path, wildcard_obj = wildcard_path.rsplit('/', 1)
        assert '*' not in path, "The '*' wildcard character is only supported after the last '/'"
        wildcard_parts = wildcard_obj.split('*')
        assert len(wildcard_parts) == 2, "Only one '*' wildcard is supported"

        for it in self.listdir(path):
            if it.startswith(path + '/' + wildcard_parts[0]) and it.endswith(wildcard_parts[1]) and \
                    len(it) >= len(path + '/' + wildcard_parts[0]) + len(wildcard_parts[1]):
                yield it

    @gcs_retry
    def download(self, path, chunksize=None, chunk_callback=lambda _: False):
        """Downloads the object contents to local file system.
        Optionally stops after the first chunk for which chunk_callback returns True.
        """
        chunksize = chunksize or self.chunksize

        bucket_str, obj_str = self._path_to_bucket_and_key(path)

        bucket = self.client.bucket(bucket_str)
        blob = bucket.blob(obj_str)
        data_to_write = []
        with tempfile.NamedTemporaryFile(delete=False, mode='ab') as fp:
            for _ in (True,):
                with blob.open("rb", chunk_size=chunksize) as f:
                    data_to_write = f.read(chunksize)
                    fp.write(data_to_write)
                    if chunk_callback(fp):
                        break
            return_fp = _DeleteOnCloseFile(fp.name, 'rb')
        return return_fp


class _DeleteOnCloseFile(io.FileIO):
    def close(self):
        super(_DeleteOnCloseFile, self).close()
        try:
            os.remove(self.name)
        except OSError:
            # Catch a potential threading race condition and also allow this
            # method to be called multiple times.
            pass

    def readable(self):
        return True

    def writable(self):
        return False

    def seekable(self):
        return True


class AtomicGCSFile(luigi.target.AtomicLocalFile):
    """
    A GCS file that writes to a temp file and put to GCS on close.
    """

    def __init__(self, path, gcs_client):
        self.gcs_client = gcs_client
        super(AtomicGCSFile, self).__init__(path)

    def move_to_final_destination(self):
        self.gcs_client.put(self.tmp_path, self.path)


class GCSTarget(luigi.target.FileSystemTarget):
    fs = None

    def __init__(self, path, format=None, client=None):
        super(GCSTarget, self).__init__(path)
        if format is None:
            format = luigi.format.get_default_format()

        self.format = format
        self.fs = client or GCSClient()

    def open(self, mode='r'):
        if mode == 'r':
            return self.format.pipe_reader(
                FileWrapper(io.BufferedReader(self.fs.download(self.path))))
        elif mode == 'w':
            return self.format.pipe_writer(AtomicGCSFile(self.path, self.fs))
        else:
            raise ValueError("Unsupported open mode '{}'".format(mode))


class GCSFlagTarget(GCSTarget):
    """
    Defines a target directory with a flag-file (defaults to `_SUCCESS`) used
    to signify job success.

    This checks for two things:

    * the path exists (just like the GCSTarget)
    * the _SUCCESS file exists within the directory.

    Because Hadoop outputs into a directory and not a single file,
    the path is assumed to be a directory.

    This is meant to be a handy alternative to AtomicGCSFile.

    The AtomicFile approach can be burdensome for GCS since there are no directories, per se.

    If we have 1,000,000 output files, then we have to rename 1,000,000 objects.
    """

    fs = None

    def __init__(self, path, format=None, client=None, flag='_SUCCESS'):
        """
        Initializes a GCSFlagTarget.

        :param path: the directory where the files are stored.
        :type path: str
        :param client:
        :type client:
        :param flag:
        :type flag: str
        """
        if format is None:
            format = luigi.format.get_default_format()

        if path[-1] != "/":
            raise ValueError("GCSFlagTarget requires the path to be to a "
                             "directory.  It must end with a slash ( / ).")
        super(GCSFlagTarget, self).__init__(path, format=format, client=client)
        self.format = format
        self.fs = client or GCSClient()
        self.flag = flag

    def exists(self):
        flag_target = self.path + self.flag
        return self.fs.exists(flag_target)
