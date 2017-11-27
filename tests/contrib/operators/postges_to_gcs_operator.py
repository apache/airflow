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
#

import unittest
import datetime
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGCSOperator

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

TASK_ID = 'test-postgres-to-gcs'
SQL = 'select 1'
BUCKET = 'gs://test'
FILENAME = 'test.json'
DEFAULT_DATE = datetime.datetime(2015, 1, 1)


class PostgresToGCSOperatorTests(unittest.TestCase):
    def setUp(self):
        self.postgres_to_gcs = PostgresToGCSOperator(
            task_id=TASK_ID, sql=SQL, bucket=BUCKET, filename=FILENAME)

    def test_init(self):
        """Test PostgresToGCSOperator instance is properly initialized."""
        self.assertEqual(self.postgres_to_gcs.task_id, TASK_ID)
        self.assertEqual(self.postgres_to_gcs.sql, SQL)
        self.assertEqual(self.postgres_to_gcs.bucket, BUCKET)
        self.assertEqual(self.postgres_to_gcs.filename, FILENAME)

    def test_run(self):
        self.postgres_to_gcs.run(
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE,
            ignore_ti_state=True)
