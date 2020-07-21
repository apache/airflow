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
import os
import unittest
from multiprocessing import Process
from os.path import basename
from tempfile import NamedTemporaryFile
from time import sleep

import requests

from airflow.configuration import conf
from airflow.utils.serve_logs import serve_logs

LOG_DATA = "Airflow log data" * 20


class TestServeLogs(unittest.TestCase):
    def test_should_serve_file(self):
        log_dir = os.path.expanduser(conf.get('logging', 'BASE_LOG_FOLDER'))
        log_port = conf.get('celery', 'WORKER_LOG_SERVER_PORT')
        with NamedTemporaryFile(dir=log_dir) as f:
            f.write(LOG_DATA.encode())
            f.flush()
            sub_proc = Process(target=serve_logs)
            sub_proc.start()
            sleep(1)
            log_url = f"http://localhost:{log_port}/log/{basename(f.name)}"
            self.assertEqual(LOG_DATA, requests.get(log_url).content.decode())
            sub_proc.terminate()
