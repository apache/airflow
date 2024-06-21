from unittest import TestCase, mock

from google.api_core.exceptions import NotFound
from google.cloud.bigquery import job, SourceFormat

from performance_scripts.utils.google_cloud.big_query_client import (
    BigQueryClient,
    CREATE_DATASET_TIMEOUT,
)

MODULE_NAME = "performance_scripts.utils.google_cloud.big_query_client"


PROJECT_ID = "project_id"
DEFAULT_PROJECT_ID = "test_default_project_id"
DATASET_NAME = "test_dataset_name"
TABLE_NAME = "test_table_name"
FILE_PATH = "path/to/the/file"


# pylint: disable=no-member
# pylint: disable=protected-access
class TestBigQueryClient(TestCase):
    def setUp(self):
        with mock.patch(
            "google.auth.default", return_value=(mock.Mock(), DEFAULT_PROJECT_ID)
        ), mock.patch(MODULE_NAME + ".Client"):
            self.bq_client = BigQueryClient(project_id=PROJECT_ID)

    @mock.patch("google.auth.default", return_value=(mock.Mock(), DEFAULT_PROJECT_ID))
    @mock.patch(MODULE_NAME + ".Client")
    def test_init(self, mock_client, mock_google_auth):
        bq_client = BigQueryClient(PROJECT_ID)
        mock_google_auth.assert_called_once_with()
        mock_client.assert_called_once_with(
            credentials=mock_google_auth.return_value[0], project=PROJECT_ID
        )
        self.assertEqual(bq_client.client, mock_client.return_value)

    @mock.patch("google.auth.default", return_value=(mock.Mock(), DEFAULT_PROJECT_ID))
    @mock.patch(MODULE_NAME + ".Client")
    def test_init_no_project_id(self, mock_client, mock_google_auth):
        bq_client = BigQueryClient()
        mock_google_auth.assert_called_once_with()
        mock_client.assert_called_once_with(
            credentials=mock_google_auth.return_value[0], project=DEFAULT_PROJECT_ID
        )
        self.assertEqual(bq_client.client, mock_client.return_value)

    @mock.patch("google.auth.default", return_value=(mock.Mock(), None))
    @mock.patch(MODULE_NAME + ".Client")
    def test_init_no_project_id_no_default_project_id(
        self, mock_client, mock_google_auth
    ):
        with self.assertRaises(ValueError):
            _ = BigQueryClient()

        mock_google_auth.assert_called_once_with()
        mock_client.assert_not_called()

    @mock.patch("tempfile.NamedTemporaryFile")
    @mock.patch(MODULE_NAME + ".BigQueryClient.check_and_create_dataset")
    @mock.patch(MODULE_NAME + ".BigQueryClient._upload_csv_to_bigquery")
    def test_upload_results(
        self, mock_upload_csv_to_bigquery, mock_check_and_create_dataset, mock_temp_file
    ):

        mock_temp_file.return_value.__enter__.return_value = mock.Mock()

        results_df = mock.Mock()

        self.bq_client.upload_results(
            results_df=results_df, dataset_name=DATASET_NAME, table_name=TABLE_NAME
        )

        results_df.to_csv.assert_called_once_with(
            mock_temp_file.return_value.__enter__.return_value.name, index=False, header=True
        )
        mock_check_and_create_dataset.assert_called_once_with(DATASET_NAME)
        mock_upload_csv_to_bigquery.assert_called_once_with(
            dataset_name=DATASET_NAME,
            table_name=TABLE_NAME,
            file_path=mock_temp_file.return_value.__enter__.return_value.name,
            schema=None,
            allow_field_addition=False,
        )

    @mock.patch(
        MODULE_NAME + ".BigQueryClient.check_if_dataset_exists", return_value=True
    )
    @mock.patch(MODULE_NAME + ".BigQueryClient._create_dataset")
    def test_check_and_create_dataset_exists(
        self, mock_create_dataset, mock_check_if_dataset_exists
    ):

        self.bq_client.check_and_create_dataset(dataset_name=DATASET_NAME)
        mock_check_if_dataset_exists.assert_called_once_with(DATASET_NAME)
        mock_create_dataset.assert_not_called()

    @mock.patch(
        MODULE_NAME + ".BigQueryClient.check_if_dataset_exists", return_value=False
    )
    @mock.patch(MODULE_NAME + ".BigQueryClient._create_dataset")
    def test_check_and_create_dataset_not_exists(
        self, mock_create_dataset, mock_check_if_dataset_exists
    ):

        self.bq_client.check_and_create_dataset(dataset_name=DATASET_NAME)
        mock_check_if_dataset_exists.assert_called_once_with(DATASET_NAME)
        mock_create_dataset.assert_called_once_with(DATASET_NAME)

    def test_check_if_dataset_exists(self):

        result = self.bq_client.check_if_dataset_exists(DATASET_NAME)

        self.bq_client.client.get_dataset.assert_called_once_with(DATASET_NAME)

        self.assertEqual(result, True)

    def test_check_if_dataset_exists_exception(self):
        self.bq_client.client.get_dataset.side_effect = NotFound("message")

        result = self.bq_client.check_if_dataset_exists(DATASET_NAME)

        self.bq_client.client.get_dataset.assert_called_once_with(DATASET_NAME)

        self.assertEqual(result, False)

    def test_create_dataset(self):

        self.bq_client._create_dataset(DATASET_NAME)

        self.bq_client.client.create_dataset.assert_called_once_with(
            DATASET_NAME, timeout=CREATE_DATASET_TIMEOUT
        )

    @mock.patch(MODULE_NAME + ".LoadJobConfig")
    @mock.patch(MODULE_NAME + ".open")
    def test_upload_csv_to_bigquery(self, mock_open, mock_load_job_config):

        self.bq_client._upload_csv_to_bigquery(
            dataset_name=DATASET_NAME, table_name=TABLE_NAME, file_path=FILE_PATH
        )

        mock_load_job_config.assert_called_once_with()
        self.assertEqual(mock_load_job_config.return_value.autodetect, True)
        self.assertEqual(
            mock_load_job_config.return_value.source_format, SourceFormat.CSV
        )

        expected_destination = f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}"

        mock_open.assert_called_once_with(FILE_PATH, "rb")

        self.bq_client.client.load_table_from_file.assert_called_once_with(
            file_obj=mock_open.return_value.__enter__.return_value,
            destination=expected_destination,
            job_config=mock_load_job_config.return_value,
        )

        self.bq_client.client.load_table_from_file.return_value.result.assert_called_once_with()

    @mock.patch(MODULE_NAME + ".LoadJobConfig")
    @mock.patch(MODULE_NAME + ".open")
    def test_upload_csv_to_bigquery_override(self, mock_open, mock_load_job_config):

        self.bq_client._upload_csv_to_bigquery(
            dataset_name=DATASET_NAME,
            table_name=TABLE_NAME,
            file_path=FILE_PATH,
            override=True,
        )

        mock_load_job_config.assert_called_once_with()
        self.assertEqual(mock_load_job_config.return_value.autodetect, True)
        self.assertEqual(
            mock_load_job_config.return_value.source_format, SourceFormat.CSV
        )
        self.assertEqual(
            mock_load_job_config.return_value.write_disposition,
            job.WriteDisposition.WRITE_TRUNCATE,
        )

        expected_destination = f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}"

        mock_open.assert_called_once_with(FILE_PATH, "rb")

        self.bq_client.client.load_table_from_file.assert_called_once_with(
            file_obj=mock_open.return_value.__enter__.return_value,
            destination=expected_destination,
            job_config=mock_load_job_config.return_value,
        )

        self.bq_client.client.load_table_from_file.return_value.result.assert_called_once_with()
