# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
from apiclient.discovery import build
from apiclient.http import MediaFileUpload
from googleapiclient import errors

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.exceptions import AirflowException

import gzip as gz
import shutil
import re
import os


class GoogleCloudStorageHook(GoogleCloudBaseHook):
    """
    Interact with Google Cloud Storage. This hook uses the Google Cloud Platform
    connection.
    """

    def __init__(self,
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None):
        super(GoogleCloudStorageHook, self).__init__(google_cloud_storage_conn_id,
                                                     delegate_to)

    def get_conn(self):
        """
        Returns a Google Cloud Storage service object.
        """
        http_authorized = self._authorize()
        return build(
            'storage', 'v1', http=http_authorized, cache_discovery=False)

    # pylint:disable=redefined-builtin
    def copy(self, source_bucket, source_object, destination_bucket=None,
             destination_object=None):
        """
        Copies an object from a bucket to another, with renaming if requested.

        destination_bucket or destination_object can be omitted, in which case
        source bucket/object is used, but not both.

        :param source_bucket: The bucket of the object to copy from.
        :type source_bucket: str
        :param source_object: The object to copy.
        :type source_object: str
        :param destination_bucket: The destination of the object to copied to.
            Can be omitted; then the same bucket is used.
        :type destination_bucket: str
        :param destination_object: The (renamed) path of the object if given.
            Can be omitted; then the same name is used.
        :type destination_object: str
        """
        destination_bucket = destination_bucket or source_bucket
        destination_object = destination_object or source_object
        if source_bucket == destination_bucket and \
                source_object == destination_object:

            raise ValueError(
                'Either source/destination bucket or source/destination object '
                'must be different, not both the same: bucket=%s, object=%s' %
                (source_bucket, source_object))
        if not source_bucket or not source_object:
            raise ValueError('source_bucket and source_object cannot be empty.')

        service = self.get_conn()
        try:
            service \
                .objects() \
                .copy(sourceBucket=source_bucket, sourceObject=source_object,
                      destinationBucket=destination_bucket,
                      destinationObject=destination_object, body='') \
                .execute()
            return True
        except errors.HttpError as ex:
            if ex.resp['status'] == '404':
                return False
            raise

    def rewrite(self, source_bucket, source_object, destination_bucket,
                destination_object=None):
        """
        Has the same functionality as copy, except that will work on files
        over 5 TB, as well as when copying between locations and/or storage
        classes.

        destination_object can be omitted, in which case source_object is used.

        :param source_bucket: The bucket of the object to copy from.
        :type source_bucket: str
        :param source_object: The object to copy.
        :type source_object: str
        :param destination_bucket: The destination of the object to copied to.
        :type destination_bucket: str
        :param destination_object: The (renamed) path of the object if given.
            Can be omitted; then the same name is used.
        :type destination_object: str
        """
        destination_object = destination_object or source_object
        if (source_bucket == destination_bucket and
                source_object == destination_object):
            raise ValueError(
                'Either source/destination bucket or source/destination object '
                'must be different, not both the same: bucket=%s, object=%s' %
                (source_bucket, source_object))
        if not source_bucket or not source_object:
            raise ValueError('source_bucket and source_object cannot be empty.')

        service = self.get_conn()
        request_count = 1
        try:
            result = service.objects() \
                .rewrite(sourceBucket=source_bucket, sourceObject=source_object,
                         destinationBucket=destination_bucket,
                         destinationObject=destination_object, body='') \
                .execute()
            self.log.info('Rewrite request #%s: %s', request_count, result)
            while not result['done']:
                request_count += 1
                result = service.objects() \
                    .rewrite(sourceBucket=source_bucket, sourceObject=source_object,
                             destinationBucket=destination_bucket,
                             destinationObject=destination_object,
                             rewriteToken=result['rewriteToken'], body='') \
                    .execute()
                self.log.info('Rewrite request #%s: %s', request_count, result)
            return True
        except errors.HttpError as ex:
            if ex.resp['status'] == '404':
                return False
            raise

    # pylint:disable=redefined-builtin
    def download(self, bucket, object, filename=None):
        """
        Get a file from Google Cloud Storage.

        :param bucket: The bucket to fetch from.
        :type bucket: str
        :param object: The object to fetch.
        :type object: str
        :param filename: If set, a local file path where the file should be written to.
        :type filename: str
        """
        service = self.get_conn()
        downloaded_file_bytes = service \
            .objects() \
            .get_media(bucket=bucket, object=object) \
            .execute()

        # Write the file to local file path, if requested.
        if filename:
            write_argument = 'wb' if isinstance(downloaded_file_bytes, bytes) else 'w'
            with open(filename, write_argument) as file_fd:
                file_fd.write(downloaded_file_bytes)

        return downloaded_file_bytes

    # pylint:disable=redefined-builtin
    def upload(self, bucket, object, filename,
               mime_type='application/octet-stream', gzip=False,
               multipart=False, num_retries=0):
        """
        Uploads a local file to Google Cloud Storage.

        :param bucket: The bucket to upload to.
        :type bucket: str
        :param object: The object name to set when uploading the local file.
        :type object: str
        :param filename: The local file path to the file to be uploaded.
        :type filename: str
        :param mime_type: The MIME type to set when uploading the file.
        :type mime_type: str
        :param gzip: Option to compress file for upload
        :type gzip: bool
        :param multipart: If True, the upload will be split into multiple HTTP requests. The
                          default size is 256MiB per request. Pass a number instead of True to
                          specify the request size, which must be a multiple of 262144 (256KiB).
        :type multipart: bool or int
        :param num_retries: The number of times to attempt to re-upload the file (or individual
                            chunks, in the case of multipart uploads). Retries are attempted
                            with exponential backoff.
        :type num_retries: int
        """
        service = self.get_conn()

        if gzip:
            filename_gz = filename + '.gz'

            with open(filename, 'rb') as f_in:
                with gz.open(filename_gz, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
                    filename = filename_gz

        try:
            if multipart:
                if multipart is True:
                    chunksize = 256 * 1024 * 1024
                else:
                    chunksize = multipart

                if chunksize % (256 * 1024) > 0 or chunksize < 0:
                    raise ValueError("Multipart size is not a multiple of 262144 (256KiB)")

                media = MediaFileUpload(filename, mimetype=mime_type,
                                        chunksize=chunksize, resumable=True)

                request = service.objects().insert(bucket=bucket, name=object, media_body=media)
                response = None
                while response is None:
                    status, response = request.next_chunk(num_retries=num_retries)
                    if status:
                        self.log.info("Upload progress %.1f%%", status.progress() * 100)

            else:
                media = MediaFileUpload(filename, mime_type)

                service \
                    .objects() \
                    .insert(bucket=bucket, name=object, media_body=media) \
                    .execute(num_retries=num_retries)

        except errors.HttpError as ex:
            if ex.resp['status'] == '404':
                return False
            raise

        finally:
            if gzip:
                os.remove(filename)

        return True

    # pylint:disable=redefined-builtin
    def exists(self, bucket, object):
        """
        Checks for the existence of a file in Google Cloud Storage.

        :param bucket: The Google cloud storage bucket where the object is.
        :type bucket: str
        :param object: The name of the object to check in the Google cloud
            storage bucket.
        :type object: str
        """
        service = self.get_conn()
        try:
            service \
                .objects() \
                .get(bucket=bucket, object=object) \
                .execute()
            return True
        except errors.HttpError as ex:
            if ex.resp['status'] == '404':
                return False
            raise

    # pylint:disable=redefined-builtin
    def is_updated_after(self, bucket, object, ts):
        """
        Checks if an object is updated in Google Cloud Storage.

        :param bucket: The Google cloud storage bucket where the object is.
        :type bucket: str
        :param object: The name of the object to check in the Google cloud
            storage bucket.
        :type object: str
        :param ts: The timestamp to check against.
        :type ts: datetime
        """
        service = self.get_conn()
        try:
            response = (service
                        .objects()
                        .get(bucket=bucket, object=object)
                        .execute())

            if 'updated' in response:
                import dateutil.parser
                import dateutil.tz

                if not ts.tzinfo:
                    ts = ts.replace(tzinfo=dateutil.tz.tzutc())

                updated = dateutil.parser.parse(response['updated'])
                self.log.info("Verify object date: %s > %s", updated, ts)

                if updated > ts:
                    return True

        except errors.HttpError as ex:
            if ex.resp['status'] != '404':
                raise

        return False

    def delete(self, bucket, object, generation=None):
        """
        Delete an object if versioning is not enabled for the bucket, or if generation
        parameter is used.

        :param bucket: name of the bucket, where the object resides
        :type bucket: str
        :param object: name of the object to delete
        :type object: str
        :param generation: if present, permanently delete the object of this generation
        :type generation: str
        :return: True if succeeded
        """
        service = self.get_conn()

        try:
            service \
                .objects() \
                .delete(bucket=bucket, object=object, generation=generation) \
                .execute()
            return True
        except errors.HttpError as ex:
            if ex.resp['status'] == '404':
                return False
            raise

    def list(self, bucket, versions=None, maxResults=None, prefix=None, delimiter=None):
        """
        List all objects from the bucket with the give string prefix in name

        :param bucket: bucket name
        :type bucket: str
        :param versions: if true, list all versions of the objects
        :type versions: bool
        :param maxResults: max count of items to return in a single page of responses
        :type maxResults: int
        :param prefix: prefix string which filters objects whose name begin with
            this prefix
        :type prefix: str
        :param delimiter: filters objects based on the delimiter (for e.g '.csv')
        :type delimiter: str
        :return: a stream of object names matching the filtering criteria
        """
        service = self.get_conn()

        ids = list()
        pageToken = None
        while True:
            response = service.objects().list(
                bucket=bucket,
                versions=versions,
                maxResults=maxResults,
                pageToken=pageToken,
                prefix=prefix,
                delimiter=delimiter
            ).execute()

            if 'prefixes' not in response:
                if 'items' not in response:
                    self.log.info("No items found for prefix: %s", prefix)
                    break

                for item in response['items']:
                    if item and 'name' in item:
                        ids.append(item['name'])
            else:
                for item in response['prefixes']:
                    ids.append(item)

            if 'nextPageToken' not in response:
                # no further pages of results, so stop the loop
                break

            pageToken = response['nextPageToken']
            if not pageToken:
                # empty next page token
                break
        return ids

    def get_size(self, bucket, object):
        """
        Gets the size of a file in Google Cloud Storage.

        :param bucket: The Google cloud storage bucket where the object is.
        :type bucket: str
        :param object: The name of the object to check in the Google cloud storage bucket.
        :type object: str

        """
        self.log.info('Checking the file size of object: %s in bucket: %s',
                      object,
                      bucket)
        service = self.get_conn()
        try:
            response = service.objects().get(
                bucket=bucket,
                object=object
            ).execute()

            if 'name' in response and response['name'][-1] != '/':
                # Remove Directories & Just check size of files
                size = response['size']
                self.log.info('The file size of %s is %s bytes.', object, size)
                return size
            else:
                raise ValueError('Object is not a file')
        except errors.HttpError as ex:
            if ex.resp['status'] == '404':
                raise ValueError('Object Not Found')

    def get_crc32c(self, bucket, object):
        """
        Gets the CRC32c checksum of an object in Google Cloud Storage.

        :param bucket: The Google cloud storage bucket where the object is.
        :type bucket: str
        :param object: The name of the object to check in the Google cloud
            storage bucket.
        :type object: str
        """
        self.log.info('Retrieving the crc32c checksum of '
                      'object: %s in bucket: %s', object, bucket)
        service = self.get_conn()
        try:
            response = service.objects().get(
                bucket=bucket,
                object=object
            ).execute()

            crc32c = response['crc32c']
            self.log.info('The crc32c checksum of %s is %s', object, crc32c)
            return crc32c

        except errors.HttpError as ex:
            if ex.resp['status'] == '404':
                raise ValueError('Object Not Found')

    def get_md5hash(self, bucket, object):
        """
        Gets the MD5 hash of an object in Google Cloud Storage.

        :param bucket: The Google cloud storage bucket where the object is.
        :type bucket: str
        :param object: The name of the object to check in the Google cloud
            storage bucket.
        :type object: str
        """
        self.log.info('Retrieving the MD5 hash of '
                      'object: %s in bucket: %s', object, bucket)
        service = self.get_conn()
        try:
            response = service.objects().get(
                bucket=bucket,
                object=object
            ).execute()

            md5hash = response['md5Hash']
            self.log.info('The md5Hash of %s is %s', object, md5hash)
            return md5hash

        except errors.HttpError as ex:
            if ex.resp['status'] == '404':
                raise ValueError('Object Not Found')

    def create_bucket(self,
                      bucket_name,
                      storage_class='MULTI_REGIONAL',
                      location='US',
                      project_id=None,
                      labels=None
                      ):
        """
        Creates a new bucket. Google Cloud Storage uses a flat namespace, so
        you can't create a bucket with a name that is already in use.

        .. seealso::
            For more information, see Bucket Naming Guidelines:
            https://cloud.google.com/storage/docs/bucketnaming.html#requirements

        :param bucket_name: The name of the bucket.
        :type bucket_name: str
        :param storage_class: This defines how objects in the bucket are stored
            and determines the SLA and the cost of storage. Values include

            - ``MULTI_REGIONAL``
            - ``REGIONAL``
            - ``STANDARD``
            - ``NEARLINE``
            - ``COLDLINE``.
            If this value is not specified when the bucket is
            created, it will default to STANDARD.
        :type storage_class: str
        :param location: The location of the bucket.
            Object data for objects in the bucket resides in physical storage
            within this region. Defaults to US.

            .. seealso::
                https://developers.google.com/storage/docs/bucket-locations

        :type location: str
        :param project_id: The ID of the GCP Project.
        :type project_id: str
        :param labels: User-provided labels, in key/value pairs.
        :type labels: dict
        :return: If successful, it returns the ``id`` of the bucket.
        """

        project_id = project_id if project_id is not None else self.project_id
        storage_classes = [
            'MULTI_REGIONAL',
            'REGIONAL',
            'NEARLINE',
            'COLDLINE',
            'STANDARD',  # alias for MULTI_REGIONAL/REGIONAL, based on location
        ]

        self.log.info('Creating Bucket: %s; Location: %s; Storage Class: %s',
                      bucket_name, location, storage_class)
        if storage_class not in storage_classes:
            raise ValueError(
                'Invalid value ({}) passed to storage_class. Value should be '
                'one of {}'.format(storage_class, storage_classes))

        if not re.match('[a-zA-Z0-9]+', bucket_name[0]):
            raise ValueError('Bucket names must start with a number or letter.')

        if not re.match('[a-zA-Z0-9]+', bucket_name[-1]):
            raise ValueError('Bucket names must end with a number or letter.')

        service = self.get_conn()
        bucket_resource = {
            'name': bucket_name,
            'location': location,
            'storageClass': storage_class
        }

        self.log.info('The Default Project ID is %s', self.project_id)

        if labels is not None:
            bucket_resource['labels'] = labels

        try:
            response = service.buckets().insert(
                project=project_id,
                body=bucket_resource
            ).execute()

            self.log.info('Bucket: %s created successfully.', bucket_name)

            return response['id']

        except errors.HttpError as ex:
            raise AirflowException(
                'Bucket creation failed. Error was: {}'.format(ex.content)
            )


def _parse_gcs_url(gsurl):
    """
    Given a Google Cloud Storage URL (gs://<bucket>/<blob>), returns a
    tuple containing the corresponding bucket and blob.
    """
    # Python 3
    try:
        from urllib.parse import urlparse
    # Python 2
    except ImportError:
        from urlparse import urlparse

    parsed_url = urlparse(gsurl)
    if not parsed_url.netloc:
        raise AirflowException('Please provide a bucket name')
    else:
        bucket = parsed_url.netloc
        # Remove leading '/' but NOT trailing one
        blob = parsed_url.path.lstrip('/')
        return bucket, blob
