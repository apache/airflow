# -*- coding: utf-8 -*-
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
from apiclient.discovery import build
from apiclient.http import MediaFileUpload
from googleapiclient import errors

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook


class GoogleCloudStorageHook(GoogleCloudBaseHook):
    """
    Interact with Google Cloud Storage. This hook uses the Google Cloud Platform
    connection.
    """

    def __init__(self,
                 google_cloud_storage_conn_id='google_cloud_storage_default',
                 delegate_to=None):
        super(GoogleCloudStorageHook, self).__init__(google_cloud_storage_conn_id,
                                                     delegate_to)

    def get_conn(self):
        """
        Returns a Google Cloud Storage service object.
        """
        http_authorized = self._authorize()
        return build('storage', 'v1', http=http_authorized)

    # pylint:disable=redefined-builtin
    def copy(self, source_bucket, source_object, destination_bucket=None,
             destination_object=None):
        """
        Copies an object from a bucket to another, with renaming if requested.

        destination_bucket or destination_object can be omitted, in which case
        source bucket/object is used, but not both.

        :param source_bucket: The bucket of the object to copy from.
        :type source_bucket: string
        :param source_object: The object to copy.
        :type source_object: string
        :param destination_bucket: The destination of the object to copied to.
            Can be omitted; then the same bucket is used.
        :type destination_bucket: string
        :param destination_object: The (renamed) path of the object if given.
            Can be omitted; then the same name is used.
        """
        destination_bucket = destination_bucket or source_bucket
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


    # pylint:disable=redefined-builtin
    def download(self, bucket, object, filename=None):
        """
        Get a file from Google Cloud Storage.

        :param bucket: The bucket to fetch from.
        :type bucket: string
        :param object: The object to fetch.
        :type object: string
        :param filename: If set, a local file path where the file should be written to.
        :type filename: string
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
    def upload(self, bucket, object, filename, mime_type='application/octet-stream'):
        """
        Uploads a local file to Google Cloud Storage.

        :param bucket: The bucket to upload to.
        :type bucket: string
        :param object: The object name to set when uploading the local file.
        :type object: string
        :param filename: The local file path to the file to be uploaded.
        :type filename: string
        :param mime_type: The MIME type to set when uploading the file.
        :type mime_type: string
        """
        service = self.get_conn()
        media = MediaFileUpload(filename, mime_type)
        response = service \
            .objects() \
            .insert(bucket=bucket, name=object, media_body=media) \
            .execute()

    # pylint:disable=redefined-builtin
    def exists(self, bucket, object):
        """
        Checks for the existence of a file in Google Cloud Storage.

        :param bucket: The Google cloud storage bucket where the object is.
        :type bucket: string
        :param object: The name of the object to check in the Google cloud
            storage bucket.
        :type object: string
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
        :type bucket: string
        :param object: The name of the object to check in the Google cloud
            storage bucket.
        :type object: string
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
        :type bucket: string
        :param object: name of the object to delete
        :type object: string
        :param generation: if present, permanently delete the object of this generation
        :type generation: string
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
        :type bucket: string
        :param versions: if true, list all versions of the objects
        :type versions: boolean
        :param maxResults: max count of items to return in a single page of responses
        :type maxResults: integer
        :param prefix: prefix string which filters objects whose name begin with this prefix
        :type prefix: string
        :param delimiter: filters objects based on the delimiter (for e.g '.csv')
        :type delimiter: string
        :return: a stream of object names matching the filtering criteria
        """
        service = self.get_conn()

        ids = list()
        pageToken = None
        while(True):
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
        :type bucket: string
        :param object: The name of the object to check in the Google cloud storage bucket.
        :type object: string

        """
        self.log.info('Checking the file size of object: %s in bucket: %s', object, bucket)
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
        :type bucket: string
        :param object: The name of the object to check in the Google cloud
            storage bucket.
        :type object: string
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
        :type bucket: string
        :param object: The name of the object to check in the Google cloud
            storage bucket.
        :type object: string
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
