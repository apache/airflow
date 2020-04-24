#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import json
import unittest
from io import StringIO

import mock
import paramiko
import pytest

from airflow import AirflowException
from airflow.models import Connection
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.utils import db
from airflow.utils.session import create_session

HELLO_SERVER_CMD = """
import socket, sys
listener = socket.socket()
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(('localhost', 2134))
listener.listen(1)
sys.stdout.write('ready')
sys.stdout.flush()
conn = listener.accept()[0]
conn.sendall(b'hello')
"""


def generate_key_string(pkey: paramiko.PKey):
    key_fh = StringIO()
    pkey.write_private_key(key_fh)
    key_fh.seek(0)
    key_str = key_fh.read()
    return key_str


def generate_host_key_string(pkey: paramiko.PKey):
    key_fh = StringIO()
    pkey.write_private_key(key_fh)
    key_fh.seek(0)
    key_obj = paramiko.RSAKey(file_obj=key_fh)
    return key_obj.get_base64()


TEST_PKEY = paramiko.RSAKey.generate(4096)
TEST_PRIVATE_KEY = generate_key_string(pkey=TEST_PKEY)
TEST_HOST_KEY = generate_host_key_string(pkey=TEST_PKEY)


class TestSSHHook(unittest.TestCase):
    CONN_SSH_WITH_PRIVATE_KEY_EXTRA = None
    CONN_SSH_WITH_EXTRA = None
    CONN_SSH_WITH_HOST_KEY_EXTRA = None
    CONN_SSH_WITH_HOST_KEY_AND_NO_HOST_KEY_CHECK_FALSE = None
    CONN_SSH_WITH_HOST_KEY_AND_NO_HOST_KEY_CHECK_TRUE = None
    CONN_SSH_WITH_NO_HOST_KEY_AND_NO_HOST_KEY_CHECK_FALSE = None

    @classmethod
    def tearDownClass(cls) -> None:
        with create_session() as session:
            conns_to_reset = [
                cls.CONN_SSH_WITH_PRIVATE_KEY_EXTRA,
                cls.CONN_SSH_WITH_EXTRA,
                cls.CONN_SSH_WITH_HOST_KEY_EXTRA,
                cls.CONN_SSH_WITH_HOST_KEY_AND_NO_HOST_KEY_CHECK_FALSE,
                cls.CONN_SSH_WITH_HOST_KEY_AND_NO_HOST_KEY_CHECK_TRUE,
                cls.CONN_SSH_WITH_NO_HOST_KEY_AND_NO_HOST_KEY_CHECK_FALSE
            ]
            connections = session.query(Connection).filter(Connection.conn_id.in_(conns_to_reset))
            connections.delete(synchronize_session=False)
            session.commit()

    @classmethod
    def setUpClass(cls) -> None:
        cls.CONN_SSH_WITH_PRIVATE_KEY_EXTRA = 'ssh_with_private_key_extra'
        cls.CONN_SSH_WITH_EXTRA = 'ssh_with_extra'
        cls.CONN_SSH_WITH_HOST_KEY_EXTRA = 'ssh_with_host_key_extra'
        cls.CONN_SSH_WITH_HOST_KEY_AND_NO_HOST_KEY_CHECK_FALSE = \
            'ssh_with_host_key_extra_and_no_host_key_check_false'
        cls.CONN_SSH_WITH_HOST_KEY_AND_NO_HOST_KEY_CHECK_TRUE = \
            'ssh_with_host_key_extra_and_no_host_key_check_true'
        cls.CONN_SSH_WITH_NO_HOST_KEY_AND_NO_HOST_KEY_CHECK_FALSE = \
            'ssh_with_no_host_key_extra_and_no_host_key_check_false'
        db.merge_conn(
            Connection(
                conn_id=cls.CONN_SSH_WITH_EXTRA,
                host='localhost',
                conn_type='ssh',
                extra='{"compress" : true, "no_host_key_check" : "true", '
                      '"allow_host_key_change": false}'
            )
        )
        db.merge_conn(
            Connection(
                conn_id=cls.CONN_SSH_WITH_PRIVATE_KEY_EXTRA,
                host='localhost',
                conn_type='ssh',
                extra=json.dumps({"private_key": TEST_PRIVATE_KEY})
            )
        )
        db.merge_conn(
            Connection(
                conn_id=cls.CONN_SSH_WITH_HOST_KEY_EXTRA,
                host='localhost',
                conn_type='ssh',
                extra=json.dumps({
                    "private_key": TEST_PRIVATE_KEY,
                    "host_key": TEST_HOST_KEY
                })
            )
        )
        db.merge_conn(
            Connection(
                conn_id=cls.CONN_SSH_WITH_HOST_KEY_AND_NO_HOST_KEY_CHECK_FALSE,
                host='localhost',
                conn_type='ssh',
                extra=json.dumps({
                    "private_key": TEST_PRIVATE_KEY,
                    "host_key": TEST_HOST_KEY,
                    "no_host_key_check": False
                })
            )
        )
        db.merge_conn(
            Connection(
                conn_id=cls.CONN_SSH_WITH_HOST_KEY_AND_NO_HOST_KEY_CHECK_TRUE,
                host='localhost',
                conn_type='ssh',
                extra=json.dumps({
                    "private_key": TEST_PRIVATE_KEY,
                    "host_key": TEST_HOST_KEY,
                    "no_host_key_check": True
                })
            )
        )
        db.merge_conn(
            Connection(
                conn_id=cls.CONN_SSH_WITH_NO_HOST_KEY_AND_NO_HOST_KEY_CHECK_FALSE,
                host='localhost',
                conn_type='ssh',
                extra=json.dumps({
                    "private_key": TEST_PRIVATE_KEY,
                    "no_host_key_check": False
                })
            )
        )

    @mock.patch('airflow.providers.ssh.hooks.ssh.paramiko.SSHClient')
    def test_ssh_connection_with_password(self, ssh_mock):
        hook = SSHHook(remote_host='remote_host',
                       port='port',
                       username='username',
                       password='password',
                       timeout=10,
                       key_file='fake.file')

        with hook.get_conn():
            ssh_mock.return_value.connect.assert_called_once_with(
                hostname='remote_host',
                username='username',
                password='password',
                key_filename='fake.file',
                timeout=10,
                compress=True,
                port='port',
                sock=None
            )

    @mock.patch('airflow.providers.ssh.hooks.ssh.paramiko.SSHClient')
    def test_ssh_connection_without_password(self, ssh_mock):
        hook = SSHHook(remote_host='remote_host',
                       port='port',
                       username='username',
                       timeout=10,
                       key_file='fake.file')

        with hook.get_conn():
            ssh_mock.return_value.connect.assert_called_once_with(
                hostname='remote_host',
                username='username',
                key_filename='fake.file',
                timeout=10,
                compress=True,
                port='port',
                sock=None
            )

    @mock.patch('airflow.providers.ssh.hooks.ssh.SSHTunnelForwarder')
    def test_tunnel_with_password(self, ssh_mock):
        hook = SSHHook(remote_host='remote_host',
                       port='port',
                       username='username',
                       password='password',
                       timeout=10,
                       key_file='fake.file')

        with hook.get_tunnel(1234):
            ssh_mock.assert_called_once_with('remote_host',
                                             ssh_port='port',
                                             ssh_username='username',
                                             ssh_password='password',
                                             ssh_pkey='fake.file',
                                             ssh_proxy=None,
                                             local_bind_address=('localhost', ),
                                             remote_bind_address=('localhost', 1234),
                                             logger=hook.log)

    @mock.patch('airflow.providers.ssh.hooks.ssh.SSHTunnelForwarder')
    def test_tunnel_without_password(self, ssh_mock):
        hook = SSHHook(remote_host='remote_host',
                       port='port',
                       username='username',
                       timeout=10,
                       key_file='fake.file')

        with hook.get_tunnel(1234):
            ssh_mock.assert_called_once_with('remote_host',
                                             ssh_port='port',
                                             ssh_username='username',
                                             ssh_pkey='fake.file',
                                             ssh_proxy=None,
                                             local_bind_address=('localhost', ),
                                             remote_bind_address=('localhost', 1234),
                                             host_pkey_directories=[],
                                             logger=hook.log)

    def test_conn_with_extra_parameters(self):
        ssh_hook = SSHHook(ssh_conn_id=self.CONN_SSH_WITH_EXTRA)
        self.assertEqual(ssh_hook.compress, True)
        self.assertEqual(ssh_hook.no_host_key_check, True)
        self.assertEqual(ssh_hook.allow_host_key_change, False)

    @mock.patch('airflow.providers.ssh.hooks.ssh.SSHTunnelForwarder')
    def test_tunnel_with_private_key(self, ssh_mock):
        hook = SSHHook(
            ssh_conn_id=self.CONN_SSH_WITH_PRIVATE_KEY_EXTRA,
            remote_host='remote_host',
            port='port',
            username='username',
            timeout=10,
        )

        with hook.get_tunnel(1234):
            ssh_mock.assert_called_once_with('remote_host',
                                             ssh_port='port',
                                             ssh_username='username',
                                             ssh_pkey=TEST_PKEY,
                                             ssh_proxy=None,
                                             local_bind_address=('localhost',),
                                             remote_bind_address=('localhost', 1234),
                                             host_pkey_directories=[],
                                             logger=hook.log)

    def test_ssh_connection(self):
        hook = SSHHook(ssh_conn_id='ssh_default')
        with hook.get_conn() as client:
            # Note - Pylint will fail with no-member here due to https://github.com/PyCQA/pylint/issues/1437
            (_, stdout, _) = client.exec_command('ls')  # pylint: disable=no-member
            self.assertIsNotNone(stdout.read())

    def test_ssh_connection_old_cm(self):
        with SSHHook(ssh_conn_id='ssh_default') as hook:
            client = hook.get_conn()
            (_, stdout, _) = client.exec_command('ls')
            self.assertIsNotNone(stdout.read())

    def test_tunnel(self):
        hook = SSHHook(ssh_conn_id='ssh_default')

        import subprocess
        import socket

        subprocess_kwargs = dict(
            args=["python", "-c", HELLO_SERVER_CMD],
            stdout=subprocess.PIPE,
        )
        with subprocess.Popen(**subprocess_kwargs) as server_handle, hook.create_tunnel(2135, 2134):
            server_output = server_handle.stdout.read(5)
            self.assertEqual(b"ready", server_output)
            socket = socket.socket()
            socket.connect(("localhost", 2135))
            response = socket.recv(5)
            self.assertEqual(response, b"hello")
            socket.close()
            server_handle.communicate()
            self.assertEqual(server_handle.returncode, 0)

    @mock.patch('airflow.providers.ssh.hooks.ssh.paramiko.SSHClient')
    def test_ssh_connection_with_private_key_extra(self, ssh_mock):
        hook = SSHHook(
            ssh_conn_id=self.CONN_SSH_WITH_PRIVATE_KEY_EXTRA,
            remote_host='remote_host',
            port='port',
            username='username',
            timeout=10,
        )

        with hook.get_conn():
            ssh_mock.return_value.connect.assert_called_once_with(
                hostname='remote_host',
                username='username',
                pkey=TEST_PKEY,
                timeout=10,
                compress=True,
                port='port',
                sock=None
            )

    @mock.patch('airflow.providers.ssh.hooks.ssh.paramiko.SSHClient')
    def test_ssh_connection_with_host_key_extra(self, ssh_client):
        hook = SSHHook(ssh_conn_id=self.CONN_SSH_WITH_HOST_KEY_EXTRA)
        assert hook.host_key is None  # Since default no_host_key_check = True unless explicit override
        with hook.get_conn():
            assert ssh_client.return_value.connect.called is True
            assert ssh_client.return_value.get_host_keys.return_value.add.called is False
            assert ssh_client.return_value.get_host_keys.return_value.check.called is False

    @mock.patch('airflow.providers.ssh.hooks.ssh.paramiko.SSHClient')
    def test_ssh_connection_with_host_key_where_no_host_key_check_is_true(self, ssh_client):
        hook = SSHHook(ssh_conn_id=self.CONN_SSH_WITH_HOST_KEY_AND_NO_HOST_KEY_CHECK_TRUE)
        assert hook.host_key is None
        with hook.get_conn():
            assert ssh_client.return_value.connect.called is True
            assert ssh_client.return_value.get_host_keys.return_value.add.called is False
            assert ssh_client.return_value.get_host_keys.return_value.check.called is False

    @mock.patch('airflow.providers.ssh.hooks.ssh.paramiko.SSHClient')
    def test_ssh_connection_with_host_key_where_no_host_key_check_is_false(self, ssh_client):
        hook = SSHHook(ssh_conn_id=self.CONN_SSH_WITH_HOST_KEY_AND_NO_HOST_KEY_CHECK_FALSE)
        assert hook.host_key == TEST_HOST_KEY
        with hook.get_conn():
            assert ssh_client.return_value.connect.called is True
            assert ssh_client.return_value.get_host_keys.return_value.add.called is True
            assert ssh_client.return_value.get_host_keys.return_value.check.called is True

    @mock.patch('airflow.providers.ssh.hooks.ssh.paramiko.SSHClient')
    def test_ssh_connection_with_no_host_key_where_no_host_key_check_is_false(self, ssh_client):
        hook = SSHHook(ssh_conn_id=self.CONN_SSH_WITH_NO_HOST_KEY_AND_NO_HOST_KEY_CHECK_FALSE)
        assert hook.host_key is None
        with pytest.raises(AirflowException):
            with hook.get_conn():
                ssh_client


if __name__ == '__main__':
    unittest.main()
