"""
Class used for communication with Google Cloud Storage.
"""

import logging
import os
import tempfile
from typing import List, Optional, Union

import google.auth
from google.api_core.exceptions import NotFound
from google.cloud import storage
from google.cloud.storage.bucket import Bucket
from pandas import DataFrame

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


class StorageClient:
    """
    Class dedicated for communication with Google Cloud Storage.
    """

    def __init__(self, project_id: Optional[str] = None) -> None:
        """
        Creates an instance of StorageClient.

        :param project_id: Google Cloud project.
            If not provided, then it will be taken from default credentials.
            Project id only determines where bucket will be created in case it does not exist.
        :type project_id: str
        """

        credentials, adc_project_id = google.auth.default()

        if project_id is None:
            project_id = adc_project_id
            if adc_project_id is None:
                log.warning(
                    "No project id was provided and the one returned from ADC is also None. This "
                    "will cause uploading results to fail if the collected bucket does not exist."
                )

        self.client = storage.Client(credentials=credentials, project=project_id)

    def upload_dag_files(
        self, dag_file_paths: Union[str, List[str]], bucket_name: str, dags_folder: str
    ) -> None:
        """
        Uploads dag files to the dedicated bucket on Cloud Storage.

        :param dag_file_paths: a single path or a list of paths to dag files.
        :type dag_file_paths: Union[str, List[str]]
        :param bucket_name: name of the bucket where dag files should be uploaded to.
        :type bucket_name: str
        :param dags_folder: path to the folder dedicated for dag_files on given bucket.
        :type dags_folder: str
        """

        if isinstance(dag_file_paths, str):
            dag_file_paths = [dag_file_paths]

        bucket = self.get_bucket(bucket_name)
        log.info("Uploading %d file(s) to bucket %s", len(dag_file_paths), bucket.name)
        for dag_file_path in dag_file_paths:
            self.upload_data_to_gcs_from_file(
                bucket=bucket,
                blob_name=os.path.join(dags_folder, os.path.basename(dag_file_path)),
                filename=dag_file_path,
                content_type=None,
            )

    def upload_results(self, results_df: DataFrame, bucket_name: str, blob_name: str) -> None:
        """
        Dumps the dataframe with the test results to csv format and uploads it to given bucket.

        :param results_df: pandas Dataframe containing information about tested environment
            configuration and performance metrics.
        :type results_df: DataFrame
        :param bucket_name: name of the bucket where the results should be stored.
        :type bucket_name: str
        :param blob_name: name of the blob under which the results should be stored.
        :type blob_name: str
        """

        with tempfile.NamedTemporaryFile() as temp_file:
            results_df.to_csv(temp_file.name, index=False)
            bucket = self.get_bucket(bucket_name)
            self.upload_data_to_gcs_from_file(
                bucket=bucket,
                blob_name=blob_name,
                filename=temp_file.name,
                content_type="text/csv",
            )

    def upload_report_dir(self, report_dir_path: str, bucket_name: str) -> None:
        """
        Uploads directory containing performance test reports to provided bucket.

        :param report_dir_path: local path to the report directory.
        :type report_dir_path: str
        :param bucket_name: name of the bucket where the reports should be stored.
        :type bucket_name: str
        """

        bucket = self.get_bucket(bucket_name)
        self.upload_directory_to_gcs_bucket(
            local_path=report_dir_path,
            bucket=bucket,
            gcs_path=os.path.basename(report_dir_path),
        )

    def get_bucket(self, bucket_name: str) -> Bucket:
        """
        Returns bucket from Google Cloud Storage, creating it if it does not exist.

        :param bucket_name: name of the bucket which you want to get.
        :type bucket_name: str

        :return: the bucket with the provided name.
        :rtype: Bucket

        :raises: ValueError: if the bucket does not exist
            and no project id was provided to service client.
        """

        log.info("Retrieving bucket %s...", bucket_name)
        try:
            bucket = self.client.get_bucket(bucket_name)
        except NotFound:
            if self.client.project is None:
                raise ValueError(
                    f"Bucket {bucket_name} was not found and no project id was "
                    f"provided either, making bucket creation impossible."
                )
            log.info("Bucket %s not found. Creating the bucket.", bucket_name)
            bucket = self.client.create_bucket(bucket_name)
        return bucket

    def upload_directory_to_gcs_bucket(
        self, local_path: str, bucket: Bucket, gcs_path: Optional[str] = None
    ) -> None:
        """
        Recursive method that uploads directory with all its contents to provided bucket.

        :param local_path: local path to the directory or file which should be uploaded.
        :type local_path: str
        :param bucket: bucket where the local_path contents should be stored.
        :type bucket: Bucket
        :param gcs_path: optional starting path on GCS side.
        :type gcs_path: str
        """

        if not gcs_path:
            gcs_path = os.path.basename(local_path)

        if os.path.isdir(local_path):
            for local_file in os.listdir(local_path):

                self.upload_directory_to_gcs_bucket(
                    local_path=os.path.join(local_path, local_file),
                    bucket=bucket,
                    gcs_path=f"{gcs_path}/{local_file}",
                )
        else:
            self.upload_data_to_gcs_from_file(bucket=bucket, blob_name=gcs_path, filename=local_path)

    @staticmethod
    def upload_data_to_gcs_from_file(
        bucket: Bucket,
        blob_name: str,
        filename: str,
        content_type: Optional[str] = None,
    ) -> None:
        """
        Allows to upload files to the Google Cloud Storage.

        :param bucket: The GCS bucket to upload to.
        :type bucket: Bucket
        :param blob_name: The object name to set when uploading the file.
        :type blob_name: str
        :param filename: The local file path to the file to be uploaded.
        :type filename: str
        :param content_type: Optional type of content being uploaded.
        :type content_type: str or None (if None, then content_type will be set to "text/csv")
        """

        try:
            blob = bucket.blob(blob_name=blob_name)
            blob.upload_from_filename(filename=filename, content_type=content_type)
        except Exception as exception:
            log.error(
                "An error occured while uploading blob %s to GCS bucket %s.",
                blob_name,
                bucket.name,
            )
            raise exception
