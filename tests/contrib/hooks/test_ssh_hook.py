# -*- coding: utf-8 -*-
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

import unittest

from airflow.models import Connection
from airflow.utils import db
from airflow.contrib.hooks.ssh_hook import SSHHook

from tests.compat import mock

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


class SSHHookTest(unittest.TestCase):
    @mock.patch('airflow.contrib.hooks.ssh_hook.paramiko.SSHClient')
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

    @mock.patch('airflow.contrib.hooks.ssh_hook.paramiko.SSHClient')
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

    @mock.patch('airflow.contrib.hooks.ssh_hook.SSHTunnelForwarder')
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

    @mock.patch('airflow.contrib.hooks.ssh_hook.SSHTunnelForwarder')
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
        db.merge_conn(
            Connection(
                conn_id='ssh_with_extra',
                host='localhost',
                conn_type='ssh',
                extra='{"compress" : true, "no_host_key_check" : "true", '
                      '"allow_host_key_change": false}'
            )
        )
        ssh_hook = SSHHook(ssh_conn_id='ssh_with_extra')
        self.assertEqual(ssh_hook.compress, True)
        self.assertEqual(ssh_hook.no_host_key_check, True)
        self.assertEqual(ssh_hook.allow_host_key_change, False)

    def test_ssh_connection(self):
        hook = SSHHook(ssh_conn_id='ssh_default')
        with hook.get_conn() as client:
            (_, stdout, _) = client.exec_command('ls')
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

        server_handle = subprocess.Popen(["python", "-c", HELLO_SERVER_CMD],
                                         stdout=subprocess.PIPE)
        with hook.create_tunnel(2135, 2134):
            server_output = server_handle.stdout.read(5)
            self.assertEqual(server_output, b"ready")
            socket = socket.socket()
            socket.connect(("localhost", 2135))
            response = socket.recv(5)
            self.assertEqual(response, b"hello")
            socket.close()
            server_handle.communicate()
            self.assertEqual(server_handle.returncode, 0)


if __name__ == '__main__':
    unittest.main()
