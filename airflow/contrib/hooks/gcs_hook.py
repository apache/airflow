import httplib2
import logging

from airflow.hooks.base_hook import BaseHook
from apiclient.discovery import build
from oauth2client.client import SignedJwtAssertionCredentials

logging.getLogger("google_cloud_storage").setLevel(logging.INFO)

class GoogleCloudStorageHook(BaseHook):
    """
    Interact with Google Cloud Storage. Connections must be defined with an extras JSON field containing:

    {
        "project": "<google project ID>",
        "service_account": "<google service account email>",
        "key_path": "<p12 key path>"
    }
    """
    conn_name_attr = 'google_cloud_storage_conn_id'

    def __init__(self, scope='https://www.googleapis.com/auth/devstorage.read_only', google_cloud_storage_conn_id='google_cloud_storage_default'):
        """
        :param scope: The scope of the hook (read only, read write, etc). See:
            https://cloud.google.com/storage/docs/authentication?hl=en#oauth-scopes
        :type scope: string
        """
        self.scope = scope
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id

    def get_conn(self):
        """
        Returns a Google Cloud Storage service object.
        """
        connection_info = self.get_connection(self.google_cloud_storage_conn_id)
        connection_extras = connection_info.extra_dejson
        service_account = connection_extras['service_account']
        key_path = connection_extras['key_path']

        with file(key_path, 'rb') as key_file:
            key = key_file.read()

        credentials = SignedJwtAssertionCredentials(
            service_account,
            key,
            scope=self.scope)
            # TODO Support domain delegation, which will allow us to set a sub-account to execute as. We can then
            # pass DAG owner emails into the connection_info, and use it here.
            # sub='some@email.com')

        http = httplib2.Http()
        http_authorized = credentials.authorize(http)
        service = build('storage', 'v1', http=http_authorized)

        return service

    def download(self, bucket, object, file_fd=False):
        """
        Get a file from Google Cloud Storage.

        :param bucket: The bucket to fetch from.
        :type bucket: string
        :param object: The object to fetch.
        :type object: string
        :param file_fd: If set, a local file descriptor where the file should be written to.
        :type file_fd: file
        """
        service = self.get_conn()
        downloaded_file_bytes = service \
            .objects() \
            .get_media(bucket=bucket, object=object) \
            .execute()

        # Write the file to local file path, if requested.
        if file_fd:
            file_fd.write(downloaded_file_bytes)

        return downloaded_file_bytes
