import logging

import google
from google.cloud.bigquery import job, Client, LoadJobConfig, SourceFormat

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

credentials, project_id = google.auth.default()
BQ = Client(credentials=credentials, project=project_id)
DATASET_NAME = "characteristics_dataset"


# pylint: disable=unused-argument
def streaming(data, context) -> None:
    """
    Cloud Function for uploading data from CSV to Big Query

    :param data: A dictionary containing the data for the event.
    :type data: Dict
    :param context: The Cloud Functions event metadata.
    :type context: (google.cloud.functions.Context)
    """

    bucket_name = data["bucket"]
    file_name = data["name"]
    table_name = data["name"]
    log.info("Uploading %s CSV to Big Query", file_name)
    _upload_csv_to_bigquery(bucket_name, file_name, table_name)


def _upload_csv_to_bigquery(bucket_name: str, file_name: str, table_name: str) -> None:
    """
    Uploads csv file from Google Cloud Storage into Big Query table,
    overriding it if it already exists.

    :param bucket_name: The bucket where the source file is located.
    :type bucket_name: str
    :param file_name: The name of the uploaded file.
    :type file_name: str
    :param table_name: Name of the table to which the data will be sent.
    :type table_name: str
    """

    job_config = LoadJobConfig()
    job_config.autodetect = True
    job_config.source_format = SourceFormat.CSV
    job_config.write_disposition = job.WriteDisposition.WRITE_TRUNCATE

    BQ.load_table_from_uri(
        source_uris=f"gs://{bucket_name}/{file_name}",
        destination=f"{project_id}.{DATASET_NAME}.{table_name}",
        job_config=job_config,
    )
