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

import logging

from apiclient.discovery import build
from apiclient.http import MediaFileUpload
from googleapiclient import errors

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook

logging.getLogger("google_cloud_storage").setLevel(logging.INFO)


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
    def download(self, bucket, object, filename=False):
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
                logging.log(logging.INFO, "Verify object date: " + str(updated)
                            + " > " + str(ts))

                if updated > ts:
                    return True

        except errors.HttpError as ex:
            if ex.resp['status'] != '404':
                raise

        return False
