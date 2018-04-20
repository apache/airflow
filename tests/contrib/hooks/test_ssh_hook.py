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
from airflow import configuration
from airflow.utils import db
from airflow import models

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
    def setUp(self):
        configuration.load_test_config()
        from airflow.contrib.hooks.ssh_hook import SSHHook
        self.hook = SSHHook(ssh_conn_id='ssh_default', keepalive_interval=10)
        self.hook.no_host_key_check = True

    def test_ssh_connection(self):
        ssh_hook = self.hook.get_conn()
        self.assertIsNotNone(ssh_hook)

    def test_tunnel(self):
        print("Setting up remote listener")
        import subprocess
        import socket

        self.server_handle = subprocess.Popen(["python", "-c", HELLO_SERVER_CMD],
                                              stdout=subprocess.PIPE)
        print("Setting up tunnel")
        with self.hook.create_tunnel(2135, 2134):
            print("Tunnel up")
            server_output = self.server_handle.stdout.read(5)
            self.assertEqual(server_output, b"ready")
            print("Connecting to server via tunnel")
            s = socket.socket()
            s.connect(("localhost", 2135))
            print("Receiving...",)
            response = s.recv(5)
            self.assertEqual(response, b"hello")
            print("Closing connection")
            s.close()
            print("Waiting for listener...")
            output, _ = self.server_handle.communicate()
            self.assertEqual(self.server_handle.returncode, 0)
            print("Closing tunnel")

    def test_conn_with_extra_parameters(self):
        from airflow.contrib.hooks.ssh_hook import SSHHook
        db.merge_conn(
            models.Connection(conn_id='ssh_with_extra',
                              host='localhost',
                              conn_type='ssh',
                              extra='{"compress" : true, "no_host_key_check" : "true"}'
                              )
        )
        ssh_hook = SSHHook(ssh_conn_id='ssh_with_extra', keepalive_interval=10)
        ssh_hook.get_conn()
        self.assertEqual(ssh_hook.compress, True)
        self.assertEqual(ssh_hook.no_host_key_check, True)


if __name__ == '__main__':
    unittest.main()
