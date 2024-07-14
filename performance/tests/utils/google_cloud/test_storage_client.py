from unittest import TestCase, mock

from google.api_core.exceptions import NotFound

from utils.google_cloud.storage_client import StorageClient

MODULE_NAME = "performance_scripts.utils.google_cloud.storage_client"

BUCKET_NAME = "bucket_name"
BLOB_NAME = "blob_name"
DAG_BLOB_NAME = "dags/file"
DAGS_FOLDER = "dags"
FILE = "path/to/the/file"
PROJECT_ID = "project_id"
DEFAULT_PROJECT_ID = "test_default_project_id"
CONTENT_TYPE = "text/csv"


# pylint: disable=no-member
class TestStorageClient(TestCase):
    def setUp(self):
        with mock.patch("google.auth.default", return_value=(mock.Mock(), DEFAULT_PROJECT_ID)), mock.patch(
            MODULE_NAME + ".storage.Client"
        ):
            self.storage = StorageClient(project_id=PROJECT_ID)

    @mock.patch("google.auth.default", return_value=(mock.Mock(), DEFAULT_PROJECT_ID))
    @mock.patch(MODULE_NAME + ".storage.Client")
    def test_init(self, mock_client, mock_google_auth):
        storage_client = StorageClient(PROJECT_ID)
        mock_google_auth.assert_called_once_with()
        mock_client.assert_called_once_with(credentials=mock_google_auth.return_value[0], project=PROJECT_ID)
        self.assertEqual(storage_client.client, mock_client.return_value)

    @mock.patch("google.auth.default", return_value=(mock.Mock(), DEFAULT_PROJECT_ID))
    @mock.patch(MODULE_NAME + ".storage.Client")
    def test_init_no_project_id(self, mock_client, mock_google_auth):
        storage_client = StorageClient()
        mock_google_auth.assert_called_once_with()
        mock_client.assert_called_once_with(
            credentials=mock_google_auth.return_value[0], project=DEFAULT_PROJECT_ID
        )
        self.assertEqual(storage_client.client, mock_client.return_value)

    @mock.patch("google.auth.default", return_value=(mock.Mock(), None))
    @mock.patch(MODULE_NAME + ".log")
    @mock.patch(MODULE_NAME + ".storage.Client")
    def test_init_no_project_id_no_default_project_id(self, mock_client, mock_log, mock_google_auth):
        storage_client = StorageClient()
        mock_google_auth.assert_called_once_with()
        mock_log.warning.assert_called_once()
        mock_client.assert_called_once_with(credentials=mock_google_auth.return_value[0], project=None)
        self.assertEqual(storage_client.client, mock_client.return_value)

    def test_should_be_connect_to_gcs(self):
        # given
        mock_conn = self.storage.client.return_value
        # when
        mock_conn()
        # then
        mock_conn.assert_called_once()

    def test_get_bucket_should_exist(self):
        # Given
        self.storage.client.get_bucket.return_value = BUCKET_NAME
        # When
        result = self.storage.get_bucket(bucket_name=BUCKET_NAME)
        self.storage.client.get_bucket.assert_called_once_with(BUCKET_NAME)
        self.storage.client.create_bucket.assert_not_called()
        # Then
        self.assertEqual(BUCKET_NAME, result)

    def test_get_bucket_should_create_bucket_if_it_not_exists(self):

        # Given
        self.storage.client.get_bucket.side_effect = NotFound("message")
        self.storage.client.create_bucket.return_value = BUCKET_NAME

        # When
        result = self.storage.get_bucket(bucket_name=BUCKET_NAME)
        # Then
        self.storage.client.get_bucket.assert_called_once_with(BUCKET_NAME)
        self.storage.client.create_bucket.assert_called_once_with(BUCKET_NAME)
        self.assertEqual(BUCKET_NAME, result)

    def test_get_bucket_not_exists_no_project_id(self):

        self.storage.client.get_bucket.side_effect = NotFound("message")
        self.storage.client.project = None

        with self.assertRaises(ValueError):
            _ = self.storage.get_bucket(bucket_name=BUCKET_NAME)
        self.storage.client.get_bucket.assert_called_once_with(BUCKET_NAME)
        self.storage.client.create_bucket.assert_not_called()

    def test_file_should_be_uploaded_to_gcs(self):
        # Given
        mock_blob = mock.MagicMock()

        mock_bucket = mock.MagicMock(**{"blob.return_value": mock_blob, "name.return_value": BUCKET_NAME})
        # When
        self.storage.upload_data_to_gcs_from_file(
            bucket=mock_bucket,
            blob_name=BLOB_NAME,
            filename=FILE,
            content_type=CONTENT_TYPE,
        )
        # Then
        mock_bucket.blob.assert_called_once_with(blob_name=BLOB_NAME)
        mock_blob.upload_from_filename.assert_called_once_with(content_type=CONTENT_TYPE, filename=FILE)

    @mock.patch("tempfile.NamedTemporaryFile")
    @mock.patch(MODULE_NAME + ".StorageClient.upload_data_to_gcs_from_file")
    @mock.patch(MODULE_NAME + ".StorageClient.get_bucket")
    def test_upload_results(self, mock_get_bucket, mock_upload_data, mock_temp_file):
        mock_temp_file.return_value.__enter__.return_value = mock.Mock()

        results_df = mock.Mock()

        self.storage.upload_results(results_df=results_df, bucket_name=BUCKET_NAME, blob_name=BLOB_NAME)

        results_df.to_csv.assert_called_once_with(
            mock_temp_file.return_value.__enter__.return_value.name, index=False
        )
        mock_get_bucket.assert_called_once_with(BUCKET_NAME)
        mock_upload_data.assert_called_once_with(
            bucket=mock_get_bucket.return_value,
            blob_name=BLOB_NAME,
            filename=mock_temp_file.return_value.__enter__.return_value.name,
            content_type=CONTENT_TYPE,
        )

    @mock.patch(MODULE_NAME + ".StorageClient.upload_data_to_gcs_from_file")
    @mock.patch(MODULE_NAME + ".StorageClient.get_bucket")
    def test_upload_single_dag_file(self, mock_get_bucket, mock_upload_data):

        self.storage.upload_dag_files(dag_file_paths=FILE, bucket_name=BUCKET_NAME, dags_folder=DAGS_FOLDER)

        mock_get_bucket.assert_called_once_with(BUCKET_NAME)
        mock_upload_data.assert_called_once_with(
            bucket=mock_get_bucket.return_value,
            blob_name=DAG_BLOB_NAME,
            filename=FILE,
            content_type=None,
        )

    @mock.patch(MODULE_NAME + ".StorageClient.upload_data_to_gcs_from_file")
    @mock.patch(MODULE_NAME + ".StorageClient.get_bucket")
    def test_upload_multiple_dag_file(self, mock_get_bucket, mock_upload_data):

        self.storage.upload_dag_files(
            dag_file_paths=[FILE] * 3, bucket_name=BUCKET_NAME, dags_folder=DAGS_FOLDER
        )

        mock_get_bucket.assert_called_once_with(BUCKET_NAME)
        self.assertEqual(mock_upload_data.call_count, 3)
