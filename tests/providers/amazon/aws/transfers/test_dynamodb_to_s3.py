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
from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from airflow import DAG
from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models import DagRun, TaskInstance
from airflow.providers.amazon.aws.transfers.base import _DEPRECATION_MSG
from airflow.providers.amazon.aws.transfers.dynamodb_to_s3 import (
    DynamoDBToS3Operator,
    JSONEncoder,
)
from airflow.utils import timezone
from airflow.utils.types import DagRunType


class TestJSONEncoder:
    @pytest.mark.parametrize("value", ["102938.3043847474", 1.010001, 10, "100", "1E-128", 1e-128])
    def test_jsonencoder_with_decimal(self, value):
        """Test JSONEncoder correctly encodes and decodes decimal values."""

        org = Decimal(value)
        encoded = json.dumps(org, cls=JSONEncoder)
        decoded = json.loads(encoded, parse_float=Decimal)
        assert org == pytest.approx(decoded)


class TestDynamodbToS3:
    def setup_method(self):
        self.output_queue = []

    def mock_upload_file(self, Filename, Bucket, Key):
        with open(Filename) as f:
            lines = f.readlines()
            for line in lines:
                self.output_queue.append(json.loads(line))

    @patch("airflow.providers.amazon.aws.transfers.dynamodb_to_s3.S3Hook")
    @patch("airflow.providers.amazon.aws.transfers.dynamodb_to_s3.DynamoDBHook")
    def test_dynamodb_to_s3_success(self, mock_aws_dynamodb_hook, mock_s3_hook):
        responses = [
            {
                "Items": [{"a": 1}, {"b": 2}],
                "LastEvaluatedKey": "123",
            },
            {
                "Items": [{"c": 3}],
            },
        ]
        table = MagicMock()
        table.return_value.scan.side_effect = responses
        mock_aws_dynamodb_hook.return_value.conn.Table = table

        s3_client = MagicMock()
        s3_client.return_value.upload_file = self.mock_upload_file
        mock_s3_hook.return_value.get_conn = s3_client

        dynamodb_to_s3_operator = DynamoDBToS3Operator(
            task_id="dynamodb_to_s3",
            dynamodb_table_name="airflow_rocks",
            s3_bucket_name="airflow-bucket",
            file_size=4000,
        )

        dynamodb_to_s3_operator.execute(context={})

        assert [{"a": 1}, {"b": 2}, {"c": 3}] == self.output_queue

    @patch("airflow.providers.amazon.aws.transfers.dynamodb_to_s3.S3Hook")
    @patch("airflow.providers.amazon.aws.transfers.dynamodb_to_s3.DynamoDBHook")
    def test_dynamodb_to_s3_success_with_decimal(self, mock_aws_dynamodb_hook, mock_s3_hook):
        a = Decimal(10.028)
        b = Decimal("10.048")
        responses = [
            {
                "Items": [{"a": a}, {"b": b}],
            }
        ]
        table = MagicMock()
        table.return_value.scan.side_effect = responses
        mock_aws_dynamodb_hook.return_value.conn.Table = table

        s3_client = MagicMock()
        s3_client.return_value.upload_file = self.mock_upload_file
        mock_s3_hook.return_value.get_conn = s3_client

        dynamodb_to_s3_operator = DynamoDBToS3Operator(
            task_id="dynamodb_to_s3",
            dynamodb_table_name="airflow_rocks",
            s3_bucket_name="airflow-bucket",
            file_size=4000,
        )

        dynamodb_to_s3_operator.execute(context={})

        assert [{"a": float(a)}, {"b": float(b)}] == self.output_queue

    @patch("airflow.providers.amazon.aws.transfers.dynamodb_to_s3.S3Hook")
    @patch("airflow.providers.amazon.aws.transfers.dynamodb_to_s3.DynamoDBHook")
    def test_dynamodb_to_s3_default_connection(self, mock_aws_dynamodb_hook, mock_s3_hook):
        responses = [
            {
                "Items": [{"a": 1}, {"b": 2}],
                "LastEvaluatedKey": "123",
            },
            {
                "Items": [{"c": 3}],
            },
        ]
        table = MagicMock()
        table.return_value.scan.side_effect = responses
        mock_aws_dynamodb_hook.return_value.get_conn.return_value.Table = table

        s3_client = MagicMock()
        s3_client.return_value.upload_file = self.mock_upload_file
        mock_s3_hook.return_value.get_conn = s3_client

        dynamodb_to_s3_operator = DynamoDBToS3Operator(
            task_id="dynamodb_to_s3",
            dynamodb_table_name="airflow_rocks",
            s3_bucket_name="airflow-bucket",
            file_size=4000,
        )

        dynamodb_to_s3_operator.execute(context={})
        aws_conn_id = "aws_default"

        mock_s3_hook.assert_called_with(aws_conn_id=aws_conn_id)
        mock_aws_dynamodb_hook.assert_called_with(aws_conn_id=aws_conn_id)

    @patch("airflow.providers.amazon.aws.transfers.dynamodb_to_s3.S3Hook")
    @patch("airflow.providers.amazon.aws.transfers.dynamodb_to_s3.DynamoDBHook")
    def test_dynamodb_to_s3_with_aws_conn_id(self, mock_aws_dynamodb_hook, mock_s3_hook):
        responses = [
            {
                "Items": [{"a": 1}, {"b": 2}],
                "LastEvaluatedKey": "123",
            },
            {
                "Items": [{"c": 3}],
            },
        ]
        table = MagicMock()
        table.return_value.scan.side_effect = responses
        mock_aws_dynamodb_hook.return_value.get_conn.return_value.Table = table

        s3_client = MagicMock()
        s3_client.return_value.upload_file = self.mock_upload_file
        mock_s3_hook.return_value.get_conn = s3_client

        aws_conn_id = "test-conn-id"
        with pytest.warns(AirflowProviderDeprecationWarning, match=_DEPRECATION_MSG):
            dynamodb_to_s3_operator = DynamoDBToS3Operator(
                task_id="dynamodb_to_s3",
                dynamodb_table_name="airflow_rocks",
                s3_bucket_name="airflow-bucket",
                file_size=4000,
                aws_conn_id=aws_conn_id,
            )

        dynamodb_to_s3_operator.execute(context={})

        mock_s3_hook.assert_called_with(aws_conn_id=aws_conn_id)
        mock_aws_dynamodb_hook.assert_called_with(aws_conn_id=aws_conn_id)

    @patch("airflow.providers.amazon.aws.transfers.dynamodb_to_s3.S3Hook")
    @patch("airflow.providers.amazon.aws.transfers.dynamodb_to_s3.DynamoDBHook")
    def test_dynamodb_to_s3_with_different_aws_conn_id(self, mock_aws_dynamodb_hook, mock_s3_hook):
        responses = [
            {
                "Items": [{"a": 1}, {"b": 2}],
                "LastEvaluatedKey": "123",
            },
            {
                "Items": [{"c": 3}],
            },
        ]
        table = MagicMock()
        table.return_value.scan.side_effect = responses
        mock_aws_dynamodb_hook.return_value.conn.Table = table

        s3_client = MagicMock()
        s3_client.return_value.upload_file = self.mock_upload_file
        mock_s3_hook.return_value.get_conn = s3_client

        aws_conn_id = "test-conn-id"
        dynamodb_to_s3_operator = DynamoDBToS3Operator(
            task_id="dynamodb_to_s3",
            dynamodb_table_name="airflow_rocks",
            s3_bucket_name="airflow-bucket",
            file_size=4000,
            source_aws_conn_id=aws_conn_id,
        )

        dynamodb_to_s3_operator.execute(context={})

        assert [{"a": 1}, {"b": 2}, {"c": 3}] == self.output_queue

        mock_s3_hook.assert_called_with(aws_conn_id=aws_conn_id)
        mock_aws_dynamodb_hook.assert_called_with(aws_conn_id=aws_conn_id)

    @patch("airflow.providers.amazon.aws.transfers.dynamodb_to_s3.S3Hook")
    @patch("airflow.providers.amazon.aws.transfers.dynamodb_to_s3.DynamoDBHook")
    def test_dynamodb_to_s3_with_two_different_connections(self, mock_aws_dynamodb_hook, mock_s3_hook):
        responses = [
            {
                "Items": [{"a": 1}, {"b": 2}],
                "LastEvaluatedKey": "123",
            },
            {
                "Items": [{"c": 3}],
            },
        ]
        table = MagicMock()
        table.return_value.scan.side_effect = responses
        mock_aws_dynamodb_hook.return_value.conn.Table = table

        s3_client = MagicMock()
        s3_client.return_value.upload_file = self.mock_upload_file
        mock_s3_hook.return_value.get_conn = s3_client

        s3_aws_conn_id = "test-conn-id"
        dynamodb_conn_id = "test-dynamodb-conn-id"
        dynamodb_to_s3_operator = DynamoDBToS3Operator(
            task_id="dynamodb_to_s3",
            dynamodb_table_name="airflow_rocks",
            source_aws_conn_id=dynamodb_conn_id,
            s3_bucket_name="airflow-bucket",
            file_size=4000,
            dest_aws_conn_id=s3_aws_conn_id,
        )

        dynamodb_to_s3_operator.execute(context={})

        assert [{"a": 1}, {"b": 2}, {"c": 3}] == self.output_queue

        mock_s3_hook.assert_called_with(aws_conn_id=s3_aws_conn_id)
        mock_aws_dynamodb_hook.assert_called_with(aws_conn_id=dynamodb_conn_id)

    @patch("airflow.providers.amazon.aws.transfers.dynamodb_to_s3.S3Hook")
    @patch("airflow.providers.amazon.aws.transfers.dynamodb_to_s3.DynamoDBHook")
    def test_dynamodb_to_s3_with_just_dest_aws_conn_id(self, mock_aws_dynamodb_hook, mock_s3_hook):
        responses = [
            {
                "Items": [{"a": 1}, {"b": 2}],
                "LastEvaluatedKey": "123",
            },
            {
                "Items": [{"c": 3}],
            },
        ]
        table = MagicMock()
        table.return_value.scan.side_effect = responses
        mock_aws_dynamodb_hook.return_value.conn.Table = table

        s3_client = MagicMock()
        s3_client.return_value.upload_file = self.mock_upload_file
        mock_s3_hook.return_value.get_conn = s3_client

        s3_aws_conn_id = "test-conn-id"
        dynamodb_to_s3_operator = DynamoDBToS3Operator(
            task_id="dynamodb_to_s3",
            dynamodb_table_name="airflow_rocks",
            s3_bucket_name="airflow-bucket",
            file_size=4000,
            dest_aws_conn_id=s3_aws_conn_id,
        )

        dynamodb_to_s3_operator.execute(context={})

        assert [{"a": 1}, {"b": 2}, {"c": 3}] == self.output_queue

        mock_aws_dynamodb_hook.assert_called_with(aws_conn_id="aws_default")
        mock_s3_hook.assert_called_with(aws_conn_id=s3_aws_conn_id)

    @pytest.mark.db_test
    def test_render_template(self, session):
        dag = DAG("test_render_template_dag_id", schedule=None, start_date=datetime(2020, 1, 1))
        operator = DynamoDBToS3Operator(
            task_id="dynamodb_to_s3_test_render",
            dag=dag,
            dynamodb_table_name="{{ ds }}",
            s3_key_prefix="{{ ds }}",
            s3_bucket_name="{{ ds }}",
            file_size=4000,
            source_aws_conn_id="{{ ds }}",
            dest_aws_conn_id="{{ ds }}",
        )
        ti = TaskInstance(operator, run_id="something")
        ti.dag_run = DagRun(
            dag_id=dag.dag_id,
            run_id="something",
            execution_date=timezone.datetime(2020, 1, 1),
            run_type=DagRunType.MANUAL,
        )
        session.add(ti)
        session.commit()
        ti.render_templates()
        assert "2020-01-01" == getattr(operator, "source_aws_conn_id")
        assert "2020-01-01" == getattr(operator, "dest_aws_conn_id")
        assert "2020-01-01" == getattr(operator, "s3_bucket_name")
        assert "2020-01-01" == getattr(operator, "dynamodb_table_name")
        assert "2020-01-01" == getattr(operator, "s3_key_prefix")

    @patch("airflow.providers.amazon.aws.transfers.dynamodb_to_s3.DynamoDBToS3Operator._export_entire_data")
    def test_dynamodb_execute_calling_export_entire_data(self, _export_entire_data):
        """Test that DynamoDBToS3Operator when called without export_time will call _export_entire_data"""
        dynamodb_to_s3_operator = DynamoDBToS3Operator(
            task_id="dynamodb_to_s3",
            dynamodb_table_name="airflow_rocks",
            s3_bucket_name="airflow-bucket",
            file_size=4000,
        )
        dynamodb_to_s3_operator.execute(context={})
        _export_entire_data.assert_called()

    @patch(
        "airflow.providers.amazon.aws.transfers.dynamodb_to_s3.DynamoDBToS3Operator."
        "_export_table_to_point_in_time"
    )
    def test_dynamodb_execute_calling_export_table_to_point_in_time(self, _export_table_to_point_in_time):
        """Test that DynamoDBToS3Operator when called without export_time will call
        _export_table_to_point_in_time. Which implements point in time recovery logic"""
        dynamodb_to_s3_operator = DynamoDBToS3Operator(
            task_id="dynamodb_to_s3",
            dynamodb_table_name="airflow_rocks",
            s3_bucket_name="airflow-bucket",
            file_size=4000,
            point_in_time_export=True,
            export_time=datetime(year=1983, month=1, day=1),
        )
        dynamodb_to_s3_operator.execute(context={})
        _export_table_to_point_in_time.assert_called()

    def test_dynamodb_with_future_date(self):
        """Test that DynamoDBToS3Operator should raise a exception when future date is passed in
        export_time parameter"""
        with pytest.raises(ValueError, match="The export_time parameter cannot be a future time."):
            DynamoDBToS3Operator(
                task_id="dynamodb_to_s3",
                dynamodb_table_name="airflow_rocks",
                s3_bucket_name="airflow-bucket",
                file_size=4000,
                point_in_time_export=True,
                export_time=datetime(year=3000, month=1, day=1),
            ).execute(context={})
