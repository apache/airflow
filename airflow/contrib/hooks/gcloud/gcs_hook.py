from future.standard_library import install_aliases
install_aliases()

from airflow.contrib.hooks.gcloud.base_hook import GCPBaseHook

from urllib.parse import urlparse
from airflow.utils import AirflowException

import gcloud.storage

def parse_gcs_url(gsurl):
    """
    Given a Google Cloud Storage URL (gs://<bucket>/<blob>), returns a
    tuple containing the corresponding bucket and blob.
    """
    parsed_url = urlparse(gsurl)
    if not parsed_url.netloc:
        raise AirflowException('Please provide a bucket name')
    else:
        bucket = parsed_url.netloc
        if parsed_url.path.startswith('/'):
            blob = parsed_url.path[1:]
        else:
            blob = parsed_url.path
        return (bucket, blob)

class GCSHook(GCPBaseHook):
    
    client_class = gcloud.storage.Client

    def bucket_exists(self, bucket):
        return self.client.bucket(bucket).exists()

    def get_bucket(self, bucket):
        return self.client.get_bucket(bucket)

    def list_blobs(
            self,
            bucket,
            max_results=None,
            page_token=None,
            prefix=None,
            delimiter=None):
        return self.client.bucket(bucket).list_blobs(
            max_results=max_results,
            page_token=page_token,
            prefix=prefix,
            delimiter=delimiter)

    def get_blob(self, blob, bucket=None):
        """
        Returns None if the blob does not exist
        """
        if not bucket:
            bucket, blob = parse_gcs_url(blob)
        return self.client.bucket(bucket).get_blob(blob)

    def blob_exists(self, blob, bucket=None):
        if not bucket:
            bucket, blob = parse_gcs_url(blob)
        return self.client.bucket(bucket).blob(blob).exists()

    def upload_from_file(
            self,
            file_obj,
            blob,
            bucket=None,
            replace=False):
        if not bucket:
            bucket, blob = parse_gcs_url(blob)
        gcs_blob = self.client.bucket(bucket).blob(blob)
        if gcs_blob.exists() and not replace:
            raise ValueError(
                'The blob {bucket}/{blob} already exists.'.format(**locals()))
        gcs_blob.upload_from_file(file_obj)

    def upload_from_filename(
            self,
            filename,
            blob,
            bucket=None,
            replace=False):
        if not bucket:
            bucket, blob = parse_gcs_url(blob)
        gcs_blob = self.client.bucket(bucket).blob(blob)
        if gcs_blob.exists() and not replace:
            raise ValueError(
                'The blob {bucket}/{blob} already exists.'.format(**locals()))
        gcs_blob.upload_from_filename(filename)

    def upload_from_string(
            self,
            string,
            blob,
            bucket=None,
            replace=False):
        if not bucket:
            bucket, blob = parse_gcs_url(blob)
        gcs_blob = self.client.bucket(bucket).blob(blob)
        if gcs_blob.exists() and not replace:
            raise ValueError(
                'The blob {bucket}/{blob} already exists.'.format(**locals()))
        gcs_blob.upload_from_string(string)

    def download_as_string(
            self,
            blob,
            bucket=None):
        if not bucket:
            bucket, blob = parse_gcs_url(blob)
        gcs_blob = self.client.bucket(bucket).get_blob(blob)
        if not gcs_blob:
            raise ValueError(
                'Blob does not exist: {bucket}/{blob}'.format(**locals()))
        return gcs_blob.download_as_string()

    def download_to_file(
            self,
            file_obj,
            blob,
            bucket=None):
        if not bucket:
            bucket, blob = parse_gcs_url(blob)
        gcs_blob = self.client.bucket(bucket).get_blob(blob)
        if not gcs_blob:
            raise ValueError(
                'Blob does not exist: {bucket}/{blob}'.format(**locals()))
        return gcs_blob.download_to_file(file_obj)

    def download_to_filename(
            self,
            filename,
            blob,
            bucket=None):
        if not bucket:
            bucket, blob = parse_gcs_url(blob)
        gcs_blob = self.client.bucket(bucket).get_blob(blob)
        if not gcs_blob:
            raise ValueError(
                'Blob does not exist: {bucket}/{blob}'.format(**locals()))
        return gcs_blob.download_to_filename(filename)

    # Compatibility methods

    def download(
            self,
            bucket,
            object,
            filename=False):
        """
        This method is provided for compatibility with
        contrib/GoogleCloudStorageHook.
        """
        if filename:
            return self.download_to_filename(
                filename=filename, blob=object, bucket=bucket)
        else:
            return self.download_as_string(blob=object, bucket=bucket)

    def upload(
            self,
            bucket,
            object,
            filename,
            mime_type='application/octet-stream'):
        """
        This method is provided for compatibility with
        contrib/GoogleCloudStorageHook.

        Warning: acts as if replace == True!
        """
        self.upload_from_filename(
                filename=filename, blob=object, bucket=bucket, replace=True)
