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

from unittest import mock

from airflow.providers.amazon.aws.operators.redshift_cluster import (
    RedshiftPauseClusterOperator,
    RedshiftResumeClusterOperator,
)


class TestResumeClusterOperator:
    def test_init(self):
        redshift_operator = RedshiftResumeClusterOperator(
            task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
        )
        assert redshift_operator.task_id == "task_test"
        assert redshift_operator.cluster_identifier == "test_cluster"
        assert redshift_operator.aws_conn_id == "aws_conn_test"

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift.RedshiftHook.cluster_status")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift.RedshiftHook.get_conn")
    def test_resume_cluster_is_called_when_cluster_is_paused(self, mock_get_conn, mock_cluster_status):
        mock_cluster_status.return_value = 'paused'
        redshift_operator = RedshiftResumeClusterOperator(
            task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
        )
        redshift_operator.execute(None)
        mock_get_conn.return_value.resume_cluster.assert_called_once_with(ClusterIdentifier='test_cluster')

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift.RedshiftHook.cluster_status")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift.RedshiftHook.get_conn")
    def test_resume_cluster_not_called_when_cluster_is_not_paused(self, mock_get_conn, mock_cluster_status):
        mock_cluster_status.return_value = 'available'
        redshift_operator = RedshiftResumeClusterOperator(
            task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
        )
        redshift_operator.execute(None)
        mock_get_conn.return_value.resume_cluster.assert_not_called()


class TestPauseClusterOperator:
    def test_init(self):
        redshift_operator = RedshiftPauseClusterOperator(
            task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
        )
        assert redshift_operator.task_id == "task_test"
        assert redshift_operator.cluster_identifier == "test_cluster"
        assert redshift_operator.aws_conn_id == "aws_conn_test"

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift.RedshiftHook.cluster_status")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift.RedshiftHook.get_conn")
    def test_pause_cluster_is_called_when_cluster_is_available(self, mock_get_conn, mock_cluster_status):
        mock_cluster_status.return_value = 'available'
        redshift_operator = RedshiftPauseClusterOperator(
            task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
        )
        redshift_operator.execute(None)
        mock_get_conn.return_value.pause_cluster.assert_called_once_with(ClusterIdentifier='test_cluster')

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift.RedshiftHook.cluster_status")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift.RedshiftHook.get_conn")
    def test_pause_cluster_not_called_when_cluster_is_not_available(self, mock_get_conn, mock_cluster_status):
        mock_cluster_status.return_value = 'paused'
        redshift_operator = RedshiftPauseClusterOperator(
            task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
        )
        redshift_operator.execute(None)
        mock_get_conn.return_value.pause_cluster.assert_not_called()
