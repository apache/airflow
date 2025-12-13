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

import contextlib
import copy
import logging
import os
from unittest import mock

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from airflow.models import DAG, DagRun, TaskInstance
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.log.s3_task_handler import S3TaskHandler
from airflow.utils.state import State, TaskInstanceState

from tests_common.test_utils.compat import EmptyOperator
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.dag import sync_dag_to_db
from tests_common.test_utils.db import clear_db_dag_bundles, clear_db_dags, clear_db_runs
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

try:
    from airflow.sdk.timezone import datetime
except ImportError:
    from airflow.utils.timezone import datetime  # type: ignore[attr-defined,no-redef]


@pytest.fixture(autouse=True)
def s3mock():
    with mock_aws():
        yield


@pytest.mark.db_test
class TestS3RemoteLogIO:
    def clear_db(self):
        clear_db_dags()
        clear_db_runs()
        if AIRFLOW_V_3_0_PLUS:
            clear_db_dag_bundles()

    @pytest.fixture(autouse=True)
    def setup_tests(self, tmp_path_factory, session, testing_dag_bundle):
        with conf_vars({("logging", "remote_log_conn_id"): "aws_default"}):
            self.remote_log_base = "s3://bucket/remote/log/location"
            self.remote_log_location = "s3://bucket/remote/log/location/1.log"
            self.remote_log_key = "remote/log/location/1.log"
            self.local_log_location = str(tmp_path_factory.mktemp("local-s3-log-location"))
            self.s3_task_handler = S3TaskHandler(self.local_log_location, self.remote_log_base)
            # Verify the hook now with the config override
            self.subject = self.s3_task_handler.io
            assert self.subject.hook is not None

        date = datetime(2016, 1, 1)
        self.dag = DAG("dag_for_testing_s3_task_handler", schedule=None, start_date=date)
        task = EmptyOperator(task_id="task_for_testing_s3_log_handler", dag=self.dag)
        if AIRFLOW_V_3_0_PLUS:
            scheduler_dag = sync_dag_to_db(self.dag)
            dag_run = DagRun(
                dag_id=self.dag.dag_id,
                logical_date=date,
                run_id="test",
                run_type="manual",
            )
        else:
            scheduler_dag = self.dag
            dag_run = DagRun(
                dag_id=self.dag.dag_id,
                execution_date=date,
                run_id="test",
                run_type="manual",
            )
        session.add(dag_run)
        session.commit()
        session.refresh(dag_run)
        if AIRFLOW_V_3_0_PLUS:
            from airflow.models.dag_version import DagVersion

            dag_version = DagVersion.get_latest_version(self.dag.dag_id)
            self.ti = TaskInstance(task=task, dag_version_id=dag_version.id)
        else:
            self.ti = TaskInstance(task=task, run_id=dag_run.run_id)
        self.ti.dag_run = dag_run
        self.ti.try_number = 1
        self.ti.state = State.RUNNING
        session.add(self.ti)
        session.commit()

        self.conn = boto3.client("s3")
        self.conn.create_bucket(Bucket="bucket")
        yield

        scheduler_dag.clear()

        self.clear_db()
        if self.s3_task_handler.handler:
            with contextlib.suppress(Exception):
                os.remove(self.s3_task_handler.handler.baseFilename)

    def test_hook(self):
        assert isinstance(self.subject.hook, S3Hook)
        assert self.subject.hook.transfer_config.use_threads is False

    def test_log_exists(self):
        self.conn.put_object(Bucket="bucket", Key=self.remote_log_key, Body=b"")
        assert self.subject.s3_log_exists(self.remote_log_location)

    def test_log_exists_none(self):
        assert not self.subject.s3_log_exists(self.remote_log_location)

    def test_log_exists_raises(self):
        assert not self.subject.s3_log_exists("s3://nonexistentbucket/foo")

    def test_log_exists_no_hook(self):
        subject = S3TaskHandler(self.local_log_location, self.remote_log_base).io
        with mock.patch.object(S3Hook, "__init__", spec=S3Hook) as mock_hook:
            mock_hook.side_effect = ConnectionError("Fake: Failed to connect")
            with pytest.raises(ConnectionError, match="Fake: Failed to connect"):
                subject.s3_log_exists(self.remote_log_location)

    def test_s3_read_when_log_missing(self, caplog):
        url = "s3://bucket/foo"
        with caplog.at_level(logging.ERROR):
            result = self.subject.s3_read(url, return_error=True)
        msg = (
            f"Could not read logs from {url} with error: An error occurred (404) when calling the "
            f"HeadObject operation: Not Found"
        )
        assert result == msg
        assert len(caplog.records) == 1
        rec = caplog.records[0]
        assert rec.levelno == logging.ERROR
        assert rec.exc_info is not None

    def test_read_raises_return_error(self, caplog):
        url = "s3://nonexistentbucket/foo"
        with caplog.at_level(logging.ERROR):
            result = self.subject.s3_read(url, return_error=True)
            msg = (
                f"Could not read logs from {url} with error: An error occurred (NoSuchBucket) when "
                f"calling the HeadObject operation: The specified bucket does not exist"
            )
        assert result == msg
        assert len(caplog.records) == 1
        rec = caplog.records[0]
        assert rec.levelno == logging.ERROR
        assert rec.exc_info is not None

    def test_write(self):
        with mock.patch.object(self.subject.log, "error") as mock_error:
            self.subject.write("text", self.remote_log_location)
            # We shouldn't expect any error logs in the default working case.
            mock_error.assert_not_called()
        body = boto3.resource("s3").Object("bucket", self.remote_log_key).get()["Body"].read()

        assert body == b"text"

    def test_write_existing(self):
        self.conn.put_object(Bucket="bucket", Key=self.remote_log_key, Body=b"previous ")
        self.subject.write("text", self.remote_log_location)
        body = boto3.resource("s3").Object("bucket", self.remote_log_key).get()["Body"].read()

        assert body == b"previous \ntext"

    def test_write_raises(self, caplog):
        url = "s3://nonexistentbucket/foo"
        with caplog.at_level(logging.ERROR):
            self.subject.write("text", url)
        assert len(caplog.records) == 1
        rec = caplog.records[0]
        assert rec.levelno == logging.ERROR
        assert rec.message == f"Could not write logs to {url}"
        assert rec.exc_info is not None


@pytest.mark.db_test
class TestS3TaskHandler:
    def clear_db(self):
        clear_db_dags()
        clear_db_runs()
        if AIRFLOW_V_3_0_PLUS:
            clear_db_dag_bundles()

    @pytest.fixture(autouse=True)
    def setup_tests(self, tmp_path_factory, session, testing_dag_bundle):
        with conf_vars({("logging", "remote_log_conn_id"): "aws_default"}):
            self.remote_log_base = "s3://bucket/remote/log/location"
            self.remote_log_location = "s3://bucket/remote/log/location/1.log"
            self.remote_log_key = "remote/log/location/1.log"
            self.local_log_location = str(tmp_path_factory.mktemp("local-s3-log-location"))
            self.s3_task_handler = S3TaskHandler(self.local_log_location, self.remote_log_base)
            # Verify the hook now with the config override
            assert self.s3_task_handler.io.hook is not None

        date = datetime(2016, 1, 1)
        self.dag = DAG("dag_for_testing_s3_task_handler", schedule=None, start_date=date)
        task = EmptyOperator(task_id="task_for_testing_s3_log_handler", dag=self.dag)
        if AIRFLOW_V_3_0_PLUS:
            scheduler_dag = sync_dag_to_db(self.dag)
            dag_run = DagRun(
                dag_id=self.dag.dag_id,
                logical_date=date,
                run_id="test",
                run_type="manual",
            )
        else:
            scheduler_dag = self.dag
            dag_run = DagRun(
                dag_id=self.dag.dag_id,
                execution_date=date,
                run_id="test",
                run_type="manual",
            )
        session.add(dag_run)
        session.commit()
        session.refresh(dag_run)
        if AIRFLOW_V_3_0_PLUS:
            from airflow.models.dag_version import DagVersion

            dag_version = DagVersion.get_latest_version(self.dag.dag_id)
            self.ti = TaskInstance(task=task, run_id=dag_run.run_id, dag_version_id=dag_version.id)
        else:
            self.ti = TaskInstance(task=task, run_id=dag_run.run_id)
        self.ti.dag_run = dag_run
        self.ti.try_number = 1
        self.ti.state = State.RUNNING
        session.add(self.ti)
        session.commit()

        self.conn = boto3.client("s3")
        self.conn.create_bucket(Bucket="bucket")
        yield

        scheduler_dag.clear()

        self.clear_db()
        if self.s3_task_handler.handler:
            with contextlib.suppress(Exception):
                os.remove(self.s3_task_handler.handler.baseFilename)

    def test_set_context_raw(self):
        self.ti.raw = True
        mock_open = mock.mock_open()
        with mock.patch("airflow.providers.amazon.aws.log.s3_task_handler.open", mock_open):
            self.s3_task_handler.set_context(self.ti)

        assert not self.s3_task_handler.upload_on_close
        mock_open.assert_not_called()

    def test_set_context_not_raw(self):
        mock_open = mock.mock_open()
        with mock.patch("airflow.providers.amazon.aws.log.s3_task_handler.open", mock_open):
            self.s3_task_handler.set_context(self.ti)

        assert self.s3_task_handler.upload_on_close
        mock_open.assert_called_once_with(os.path.join(self.local_log_location, "1.log"), "w")
        mock_open().write.assert_not_called()

    def test_read(self):
        # Test what happens when we have two log files to read
        self.conn.put_object(Bucket="bucket", Key=self.remote_log_key, Body=b"Log line\nLine 2\n")
        self.conn.put_object(
            Bucket="bucket", Key=self.remote_log_key + ".trigger.log", Body=b"Log line 3\nLine 4\n"
        )
        ti = copy.copy(self.ti)
        ti.state = TaskInstanceState.SUCCESS
        log, metadata = self.s3_task_handler.read(ti)

        expected_s3_uri = f"s3://bucket/{self.remote_log_key}"

        if AIRFLOW_V_3_0_PLUS:
            log = list(log)
            assert log[0].event == "::group::Log message source details"
            assert expected_s3_uri in log[0].sources
            assert log[1].event == "::endgroup::"
            assert log[2].event == "Log line"
            assert log[3].event == "Line 2"
            assert log[4].event == "Log line 3"
            assert log[5].event == "Line 4"
            assert metadata == {"end_of_log": True, "log_pos": 4}
        else:
            actual = log[0][0][-1]
            assert f"*** Found logs in s3:\n***   * {expected_s3_uri}\n" in actual
            assert actual.endswith("Line 4")
            assert metadata == [{"end_of_log": True, "log_pos": 33}]

    def test_read_when_s3_log_missing(self):
        ti = copy.copy(self.ti)
        ti.state = TaskInstanceState.SUCCESS
        self.s3_task_handler._read_from_logs_server = mock.Mock(return_value=([], []))
        log, metadata = self.s3_task_handler.read(ti)
        if AIRFLOW_V_3_0_PLUS:
            log = list(log)
            assert len(log) == 2
            assert metadata == {"end_of_log": True, "log_pos": 0}
        else:
            assert len(log) == 1
            assert len(log) == len(metadata)
            actual = log[0][0][-1]
            expected = "*** No logs found on s3 for ti=<TaskInstance: dag_for_testing_s3_task_handler.task_for_testing_s3_log_handler test [success]>\n"
            assert expected in actual
            assert metadata[0] == {"end_of_log": True, "log_pos": 0}

    def test_close(self):
        self.s3_task_handler.set_context(self.ti)
        assert self.s3_task_handler.upload_on_close

        self.s3_task_handler.close()
        # Should not raise
        boto3.resource("s3").Object("bucket", self.remote_log_key).get()

    def test_close_no_upload(self):
        self.ti.raw = True
        self.s3_task_handler.set_context(self.ti)
        assert not self.s3_task_handler.upload_on_close
        self.s3_task_handler.close()

        with pytest.raises(ClientError):
            boto3.resource("s3").Object("bucket", self.remote_log_key).get()

    @pytest.mark.parametrize(
        ("delete_local_copy", "expected_existence_of_local_copy"),
        [(True, False), (False, True)],
    )
    def test_close_with_delete_local_logs_conf(self, delete_local_copy, expected_existence_of_local_copy):
        with conf_vars({("logging", "delete_local_logs"): str(delete_local_copy)}):
            handler = S3TaskHandler(self.local_log_location, self.remote_log_base)

        handler.log.info("test")
        handler.set_context(self.ti)
        assert handler.upload_on_close

        handler.close()
        assert os.path.exists(handler.handler.baseFilename) == expected_existence_of_local_copy

    def test_filename_template_for_backward_compatibility(self):
        # filename_template arg support for running the latest provider on airflow 2
        S3TaskHandler(self.local_log_location, self.remote_log_base, filename_template=None)
