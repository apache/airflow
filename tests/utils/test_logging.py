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

import unittest

from airflow.exceptions import AirflowException
from airflow.utils import logging as logging_utils
from datetime import datetime, timedelta

class Logging(unittest.TestCase):

    def test_get_log_filename(self):
        dag_id = 'dag_id'
        task_id = 'task_id'
        execution_date = datetime(2017, 1, 1, 0, 0, 0)
        try_number = 0
        filename = logging_utils.get_log_filename(dag_id, task_id, execution_date, try_number)
        self.assertEqual(filename, 'dag_id/task_id/2017-01-01T00:00:00/1.log')
