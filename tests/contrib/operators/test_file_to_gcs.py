import datetime
import unittest

from airflow import DAG, configuration
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None


class TestFileToGcsOperator(unittest.TestCase):

    _config = {
        'src': '/tmp/fake.csv',
        'dst': 'fake.csv',
        'bucket': 'dummy',
        'mime_type': 'application/octet-stream',
        'gzip': False
    }

    def setUp(self):
        configuration.load_test_config()
        args = {
            'owner': 'airflow',
            'start_date': datetime.datetime(2017, 1, 1)
        }
        self.dag = DAG('test_dag_id', default_args=args)

    def test_init(self):
        operator = FileToGoogleCloudStorageOperator(
            task_id='file_to_gcs_operator',
            dag=self.dag,
            **self._config
        )
        self.assertEqual(operator.src, self._config['src'])
        self.assertEqual(operator.dst, self._config['dst'])
        self.assertEqual(operator.bucket, self._config['bucket'])
        self.assertEqual(operator.mime_type, self._config['mime_type'])
        self.assertEqual(operator.gzip, self._config['gzip'])

    @mock.patch('airflow.contrib.operators.file_to_gcs.GoogleCloudStorageHook',
                autospec=True)
    def test_execute(self, mock_hook):
        mock_instance = mock_hook.return_value
        operator = FileToGoogleCloudStorageOperator(
            task_id='gcs_to_file_sensor',
            dag=self.dag,
            **self._config
        )
        operator.execute(None)
        mock_instance.upload.assert_called_once_with(
            bucket=self._config['bucket'],
            filename=self._config['src'],
            gzip=self._config['gzip'],
            mime_type=self._config['mime_type'],
            object=self._config['dst']
        )


if __name__ == '__main__':
    unittest.main()
