"""
Class used for communication with BigQuery.
"""

import logging
import tempfile
from typing import List, Optional

import google.auth
from google.api_core.exceptions import NotFound
from google.cloud.bigquery import job, Client, LoadJobConfig, SchemaField, SourceFormat
from pandas import DataFrame

CREATE_DATASET_TIMEOUT = 30

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


class BigQueryClient:
    """
    Class dedicated for uploading test results to BigQuery service.
    """

    def __init__(self, project_id: Optional[str] = None) -> None:
        """
        Creates an instance of BigQueryClient.

        :param project_id: Google Cloud project.
            If not provided, then it will be taken from default credentials.
        :type project_id: str

        :raises: ValueError if both project ids (provided via argument and ADC) are None.
        """

        credentials, adc_project_id = google.auth.default()

        if project_id is None:
            project_id = adc_project_id
            if adc_project_id is None:
                raise ValueError(
                    "No project id was provided and the one returned from ADC is also None. "
                    "This makes uploading results to BigQuery dataset impossible."
                )

        self.project_id = project_id
        self.client = Client(credentials=credentials, project=project_id)

    def upload_results(
        self, results_df: DataFrame, dataset_name: str, table_name: str,
        schema: List[SchemaField] = None, allow_field_addition: bool = False,
    ) -> None:
        """
        Saves the dataframe with the test results in BigQuery, creating specified dataset if it
        does not exist.

        :param results_df: pandas Dataframe containing information about tested environment
            configuration and performance metrics.
        :type results_df: DataFrame
        :param dataset_name: Name of the BigQuery dataset where the table should be stored.
        :type dataset_name: str
        :param table_name: Name of the BigQuery table under which the dataframe should be saved.
        :type table_name: str
        :param schema: schema of table. If not provided `autodetect` mode will be used.
        :type schema: List[SchemaField]
        :param allow_field_addition: True if adding new columns to table is allowed in case of
            appending data.
        :type allow_field_addition: bool
        """

        try:
            with tempfile.NamedTemporaryFile() as temp_file:
                results_df.to_csv(temp_file.name, index=False, header=True if schema is None else False)
                self.check_and_create_dataset(dataset_name)
                # TODO: using this way of uploading results causes some questionable conversions
                #  of columns into BOOL type (examples: T/F, Yes/No, Y/N)
                self._upload_csv_to_bigquery(
                    dataset_name=dataset_name,
                    table_name=table_name,
                    file_path=temp_file.name,
                    schema=schema,
                    allow_field_addition=allow_field_addition,
                )
        except Exception as exception:
            log.error(
                "An error occurred while loading results table %s "
                "to BigQuery dataset %s.",
                table_name,
                dataset_name,
            )
            raise exception

    def check_and_create_dataset(self, dataset_name: str) -> None:
        """
        Checks if dataset with given name exists and creates it, if it does not.

        :param dataset_name: name of the BigQuery dataset to check.
        :type dataset_name: str
        """
        if not self.check_if_dataset_exists(dataset_name):
            log.info(
                "Provided dataset %s does not exist. Creating the dataset.",
                dataset_name,
            )
            self._create_dataset(dataset_name)
        log.info("Provided dataset %s already exists.", dataset_name)

    def check_if_dataset_exists(self, dataset_name: str) -> bool:
        """
        Checks if dataset with given name exists under specified project id.

        :param dataset_name: name of the BigQuery dataset to check.
        :type dataset_name: str

        :return: True if dataset exists and False otherwise.
        :rtype: bool
        """

        try:
            self.client.get_dataset(dataset_name)
        except NotFound:
            return False
        return True

    def _create_dataset(self, dataset_name: str) -> None:
        """
        Creates dataset with provided name. Method is unsafe - it does not check if the dataset
        already exists.

        :param dataset_name: Name of the BigQuery dataset to create.
        :type dataset_name: str
        """

        self.client.create_dataset(dataset_name, timeout=CREATE_DATASET_TIMEOUT)
        log.info("Created dataset %s", dataset_name)

    def _upload_csv_to_bigquery(
        self, dataset_name: str, table_name: str, file_path: str, override: bool = False,
        schema: List[SchemaField] = None, allow_field_addition: bool = False,
    ) -> None:
        """
        Uploads csv file into BigQuery table.

        :param dataset_name: Name of the BigQuery dataset where the table should be stored.
        :type dataset_name: str
        :param table_name: Name of the table under which the file should be saved.
        :type table_name: str
        :param file_path: Path to the file which should be uploaded.
        :type file_path: str
        :param override: True if table should be overridden in case it already exists
            and False otherwise.
        :type override: bool
        :param schema: schema of table. If not provided `autodetect` mode will be used.
        :type schema: List[SchemaField]
        :param allow_field_addition: True if adding new columns to table is allowed in case of
            appending data.
        :type allow_field_addition: bool
        """

        job_config = LoadJobConfig()
        if schema is not None:
            job_config.schema = schema
        else:
            job_config.autodetect = True
        job_config.source_format = SourceFormat.CSV
        if override:
            job_config.write_disposition = job.WriteDisposition.WRITE_TRUNCATE
        if allow_field_addition:
            job_config.schema_update_options = [job.SchemaUpdateOption.ALLOW_FIELD_ADDITION]

        destination = f"{self.project_id}.{dataset_name}.{table_name}"

        log.info("Uploading file to %s BigQuery table.", destination)
        with open(file_path, "rb") as source_file:
            bq_job = self.client.load_table_from_file(
                file_obj=source_file, destination=destination, job_config=job_config
            )

        log.info("Waiting for the BigQuery job to complete.")
        bq_job.result()
