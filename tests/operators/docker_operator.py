import unittest

try:
    from airflow.operators.docker_operator import DockerOperator
    from docker.client import Client
except ImportError:
    pass

from airflow.exceptions import AirflowException

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None


class DockerOperatorTestCase(unittest.TestCase):
    @unittest.skipIf(mock is None, 'mock package not present')
    @mock.patch('airflow.utils.file.mkdtemp')
    @mock.patch('airflow.operators.docker_operator.Client')
    def test_execute(self, client_class_mock, mkdtemp_mock):
        host_config = mock.Mock()
        mkdtemp_mock.return_value = '/mkdtemp'

        client_mock = mock.Mock(spec=Client)
        client_mock.create_container.return_value = {'Id': 'some_id'}
        client_mock.create_host_config.return_value = host_config
        client_mock.images.return_value = []
        client_mock.logs.return_value = ['container log']
        client_mock.pull.return_value = ['{"status":"pull log"}']
        client_mock.wait.return_value = 0

        client_class_mock.return_value = client_mock

        operator = DockerOperator(api_version='1.19', command='env', environment={'UNIT': 'TEST'},
                                  image='ubuntu:latest', network_mode='bridge', owner='unittest',
                                  task_id='unittest', volumes=['/host/path:/container/path'])
        operator.execute(None)

        client_class_mock.assert_called_with(base_url='unix://var/run/docker.sock', tls=None,
                                             version='1.19')

        client_mock.create_container.assert_called_with(command='env', cpu_shares=1024,
                                                        environment={
                                                            'AIRFLOW_TMP_DIR': '/tmp/airflow',
                                                            'UNIT': 'TEST'
                                                        },
                                                        host_config=host_config,
                                                        image='ubuntu:latest',
                                                        mem_limit=None, user=None)
        client_mock.create_host_config.assert_called_with(binds=['/host/path:/container/path',
                                                                 '/mkdtemp:/tmp/airflow'],
                                                          network_mode='bridge')
        client_mock.images.assert_called_with(name='ubuntu:latest')
        client_mock.logs.assert_called_with(container='some_id', stream=True)
        client_mock.pull.assert_called_with('ubuntu:latest', stream=True)
        client_mock.wait.assert_called_with('some_id')

    @unittest.skipIf(mock is None, 'mock package not present')
    @mock.patch('airflow.operators.docker_operator.tls.TLSConfig')
    @mock.patch('airflow.operators.docker_operator.Client')
    def test_execute_tls(self, client_class_mock, tls_class_mock):
        client_mock = mock.Mock(spec=Client)
        client_mock.create_container.return_value = {'Id': 'some_id'}
        client_mock.create_host_config.return_value = mock.Mock()
        client_mock.images.return_value = []
        client_mock.logs.return_value = []
        client_mock.pull.return_value = []
        client_mock.wait.return_value = 0

        client_class_mock.return_value = client_mock
        tls_mock = mock.Mock()
        tls_class_mock.return_value = tls_mock

        operator = DockerOperator(docker_url='tcp://127.0.0.1:2376', image='ubuntu',
                                  owner='unittest', task_id='unittest', tls_client_cert='cert.pem',
                                  tls_ca_cert='ca.pem', tls_client_key='key.pem')
        operator.execute(None)

        tls_class_mock.assert_called_with(assert_hostname=None, ca_cert='ca.pem',
                                          client_cert=('cert.pem', 'key.pem'), ssl_version=None,
                                          verify=True)

        client_class_mock.assert_called_with(base_url='https://127.0.0.1:2376', tls=tls_mock,
                                             version=None)

    @unittest.skipIf(mock is None, 'mock package not present')
    @mock.patch('airflow.operators.docker_operator.Client')
    def test_execute_container_fails(self, client_class_mock):
        client_mock = mock.Mock(spec=Client)
        client_mock.create_container.return_value = {'Id': 'some_id'}
        client_mock.create_host_config.return_value = mock.Mock()
        client_mock.images.return_value = []
        client_mock.logs.return_value = []
        client_mock.pull.return_value = []
        client_mock.wait.return_value = 1

        client_class_mock.return_value = client_mock

        operator = DockerOperator(image='ubuntu', owner='unittest', task_id='unittest')

        with self.assertRaises(AirflowException):
            operator.execute(None)

    @unittest.skipIf(mock is None, 'mock package not present')
    def test_on_kill(self):
        client_mock = mock.Mock(spec=Client)

        operator = DockerOperator(image='ubuntu', owner='unittest', task_id='unittest')
        operator.cli = client_mock
        operator.container = {'Id': 'some_id'}

        operator.on_kill()

        client_mock.stop.assert_called_with('some_id')


if __name__ == "__main__":
    unittest.main()
