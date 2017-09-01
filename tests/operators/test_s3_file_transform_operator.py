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

import logging
import unittest

import boto
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.operators.s3_file_transform_operator import S3FileTransformOperator
from airflow.utils import db
try:
    from moto import mock_s3_deprecated
except ImportError:
    mock_s3_deprecated = None


DEFAULT_CONN_ID = "s3_default"


class S3FileTransformTest(unittest.TestCase):
    """
    Tests for the S3 file transform operator.
    """

    @db.provide_session
    def setUp(self, session=None):
        self.mock_s3 = mock_s3_deprecated()
        self.mock_s3.start()
        self.s3_connection = session.query(Connection).filter(
            Connection.conn_id == DEFAULT_CONN_ID
        ).first()
        if self.s3_connection is None:
            self.s3_connection = Connection(conn_id=DEFAULT_CONN_ID, conn_type="s3")
            session.add(self.s3_connection)
            session.commit()

    def tearDown(self):
        self.mock_s3.stop()

    @unittest.skipIf(mock_s3_deprecated is None, 'mock package not present')
    def test_execute(self):
        source_key = "/source/key"
        source_bucket_name = "source-bucket"
        dest_key = "/dest/key"
        dest_bucket_name = "dest-bucket"
        key_data = u"foobar"
        # set up mock data
        s3_client = boto.connect_s3()
        source_bucket = s3_client.create_bucket(source_bucket_name)
        dest_bucket = s3_client.create_bucket(dest_bucket_name)
        source_obj = boto.s3.key.Key(source_bucket)
        source_obj.key = source_key
        source_obj.set_contents_from_string(key_data)
        # Invoke .execute
        s3_xform_task = S3FileTransformOperator(
            task_id="s3_file_xform",
            source_s3_key="s3://{}{}".format(source_bucket_name, source_key),
            dest_s3_key="s3://{}{}".format(dest_bucket_name, dest_key),
            transform_script="cp")
        s3_xform_task.execute(None)
        # ensure the data is correct
        result = dest_bucket.get_key(dest_key)
        stored = result.get_contents_as_string()
        if six.PY3:
            stored = stored.decode()
        self.assertEqual(stored, key_data)


if __name__ == "__main__":
    unittest.main()
