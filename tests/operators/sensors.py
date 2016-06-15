# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import unittest

from airflow.operators.sensors import HttpSensor
from airflow.exceptions import AirflowException


class HttpSensorTests(unittest.TestCase):

    def test_poke_exception(self):
        """
        Exception occurs in poke function should not be ignored.
        """
        def resp_check(resp):
            raise AirflowException('AirflowException raised here!')

        task = HttpSensor(
            task_id='http_sensor_poke_exception',
            http_conn_id='http_default',
            endpoint='',
            params={},
            response_check=resp_check,
            poke_interval=5)
        with self.assertRaisesRegexp(AirflowException, 'AirflowException raised here!'):
            task.execute(None)
