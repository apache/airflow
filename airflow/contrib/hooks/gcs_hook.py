import httplib2
import logging

from airflow.contrib.hooks.gc_base_hook import GoogleCloudBaseHook
from airflow.hooks.base_hook import BaseHook
from apiclient.discovery import build
from apiclient.http import MediaFileUpload
from oauth2client.client import SignedJwtAssertionCredentials

logging.getLogger("google_cloud_storage").setLevel(logging.INFO)

class GoogleCloudStorageHook(GoogleCloudBaseHook):
    """
    Interact with Google Cloud Storage. Connections must be defined with an
    extras JSON field containing:

    {
        "project": "<google project ID>",
        "service_account": "<google service account email>",
        "key_path": "<p12 key path>"
    }

    If you have used ``gcloud auth`` to authenticate on the machine that's
    running Airflow, you can exclude the service_account and key_path
    parameters.
    """
    conn_name_attr = 'google_cloud_storage_conn_id'

    def __init__(self,
                 scope='https://www.googleapis.com/auth/devstorage.read_only',
                 google_cloud_storage_conn_id='google_cloud_storage_default',
                 delegate_to=None):
        """
        :param scope: The scope of the hook (read only, read write, etc). See:
            https://cloud.google.com/storage/docs/authentication?hl=en#oauth-scopes
        :type scope: string
        """
        super(GoogleCloudStorageHook, self).__init__(scope, google_cloud_storage_conn_id, delegate_to)

    def get_conn(self):
        """
        Returns a Google Cloud Storage service object.
        """
        http_authorized = self._authorize()
        return build('storage', 'v1', http=http_authorized)

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
            with open(filename, 'w') as file_fd:
                file_fd.write(downloaded_file_bytes)

        return downloaded_file_bytes

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
