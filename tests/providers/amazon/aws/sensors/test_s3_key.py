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
from typing import List
from unittest import mock

import pytest
from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.models import DAG, DagRun, TaskInstance
from airflow.models.variable import Variable
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils import timezone


class TestS3KeySensor(unittest.TestCase):
    def test_bucket_name_none_and_bucket_key_as_relative_path(self):
        """
        Test if exception is raised when bucket_name is None
        and bucket_key is provided as relative path rather than s3:// url.
        :return:
        """
        op = S3KeySensor(task_id='s3_key_sensor', bucket_key="file_in_bucket")
        with pytest.raises(AirflowException):
            op.poke(None)

    @mock.patch('airflow.providers.amazon.aws.sensors.s3.S3Hook.head_object')
    def test_bucket_name_none_and_bucket_key_is_list_and_contain_relative_path(self, mock_head_object):
        """
        Test if exception is raised when bucket_name is None
        and bucket_key is provided with one of the two keys as relative path rather than s3:// url.
        :return:
        """
        mock_head_object.return_value = {'ContentLength': 0}
        op = S3KeySensor(task_id='s3_key_sensor', bucket_key=["s3://test_bucket/file", "file_in_bucket"])
        with pytest.raises(AirflowException):
            op.poke(None)

    def test_bucket_name_provided_and_bucket_key_is_s3_url(self):
        """
        Test if exception is raised when bucket_name is provided
        while bucket_key is provided as a full s3:// url.
        :return:
        """
        op = S3KeySensor(
            task_id='s3_key_sensor', bucket_key="s3://test_bucket/file", bucket_name='test_bucket'
        )
        with pytest.raises(TypeError):
            op.poke(None)

    @mock.patch('airflow.providers.amazon.aws.sensors.s3.S3Hook.head_object')
    def test_bucket_name_provided_and_bucket_key_is_list_and_contains_s3_url(self, mock_head_object):
        """
        Test if exception is raised when bucket_name is provided
        while bucket_key contains a full s3:// url.
        :return:
        """
        mock_head_object.return_value = {'ContentLength': 0}
        op = S3KeySensor(
            task_id='s3_key_sensor',
            bucket_key=["test_bucket", "s3://test_bucket/file"],
            bucket_name='test_bucket',
        )
        with pytest.raises(TypeError):
            op.poke(None)

    @parameterized.expand(
        [
            ['s3://bucket/key', None, 'key', 'bucket'],
            ['key', 'bucket', 'key', 'bucket'],
        ]
    )
    @mock.patch('airflow.providers.amazon.aws.sensors.s3.S3Hook.head_object')
    def test_parse_bucket_key(self, key, bucket, parsed_key, parsed_bucket, mock_head_object):
        mock_head_object.return_value = None

        op = S3KeySensor(
            task_id='s3_key_sensor',
            bucket_key=key,
            bucket_name=bucket,
        )

        op.poke(None)

        mock_head_object.assert_called_once_with(parsed_key, parsed_bucket)

    @mock.patch('airflow.providers.amazon.aws.sensors.s3.S3Hook.head_object')
    def test_parse_bucket_key_from_jinja(self, mock_head_object):
        mock_head_object.return_value = None

        Variable.set("test_bucket_key", "s3://bucket/key")

        execution_date = timezone.datetime(2020, 1, 1)

        dag = DAG("test_s3_key", start_date=execution_date)
        op = S3KeySensor(
            task_id='s3_key_sensor',
            bucket_key='{{ var.value.test_bucket_key }}',
            bucket_name=None,
            dag=dag,
        )

        dag_run = DagRun(dag_id=dag.dag_id, execution_date=execution_date, run_id="test")
        ti = TaskInstance(task=op)
        ti.dag_run = dag_run
        context = ti.get_template_context()
        ti.render_templates(context)
        op.poke(None)

        mock_head_object.assert_called_once_with("key", "bucket")

    @mock.patch('airflow.providers.amazon.aws.sensors.s3.S3Hook.head_object')
    def test_poke(self, mock_head_object):
        op = S3KeySensor(task_id='s3_key_sensor', bucket_key='s3://test_bucket/file')

        mock_head_object.return_value = None
        assert op.poke(None) is False
        mock_head_object.assert_called_once_with("file", "test_bucket")

        mock_head_object.return_value = {'ContentLength': 0}
        assert op.poke(None) is True

    @mock.patch('airflow.providers.amazon.aws.sensors.s3.S3Hook.head_object')
    def test_poke_multiple_files(self, mock_head_object):
        op = S3KeySensor(
            task_id='s3_key_sensor', bucket_key=['s3://test_bucket/file1', 's3://test_bucket/file2']
        )

        mock_head_object.side_effect = [{'ContentLength': 0}, None]
        assert op.poke(None) is False

        mock_head_object.side_effect = [{'ContentLength': 0}, {'ContentLength': 0}]
        assert op.poke(None) is True

        mock_head_object.assert_any_call("file1", "test_bucket")
        mock_head_object.assert_any_call("file2", "test_bucket")

    @mock.patch('airflow.providers.amazon.aws.sensors.s3.S3Hook.get_file_metadata')
    def test_poke_wildcard(self, mock_get_file_metadata):
        op = S3KeySensor(task_id='s3_key_sensor', bucket_key='s3://test_bucket/file*', wildcard_match=True)

        mock_get_file_metadata.return_value = []
        assert op.poke(None) is False
        mock_get_file_metadata.assert_called_once_with("file", "test_bucket")

        mock_get_file_metadata.return_value = [{'Key': 'dummyFile', 'Size': 0}]
        assert op.poke(None) is False

        mock_get_file_metadata.return_value = [{'Key': 'file1', 'Size': 0}]
        assert op.poke(None) is True

    @mock.patch('airflow.providers.amazon.aws.sensors.s3.S3Hook.get_file_metadata')
    def test_poke_wildcard_multiple_files(self, mock_get_file_metadata):
        op = S3KeySensor(
            task_id='s3_key_sensor',
            bucket_key=['s3://test_bucket/file*', 's3://test_bucket/*.zip'],
            wildcard_match=True,
        )

        mock_get_file_metadata.side_effect = [[{'Key': 'file1', 'Size': 0}], []]
        assert op.poke(None) is False

        mock_get_file_metadata.side_effect = [[{'Key': 'file1', 'Size': 0}], [{'Key': 'file2', 'Size': 0}]]
        assert op.poke(None) is False

        mock_get_file_metadata.side_effect = [[{'Key': 'file1', 'Size': 0}], [{'Key': 'test.zip', 'Size': 0}]]
        assert op.poke(None) is True

        mock_get_file_metadata.assert_any_call("file", "test_bucket")
        mock_get_file_metadata.assert_any_call("", "test_bucket")

    @mock.patch('airflow.providers.amazon.aws.sensors.s3.S3Hook.head_object')
    def test_poke_with_check_function(self, mock_head_object):
        def check_fn(files: List) -> bool:
            return all(f.get('Size', 0) > 0 for f in files)

        op = S3KeySensor(task_id='s3_key_sensor', bucket_key='s3://test_bucket/file', check_fn=check_fn)

        mock_head_object.return_value = {'ContentLength': 0}
        assert op.poke(None) is False

        mock_head_object.return_value = {'ContentLength': 1}
        assert op.poke(None) is True
