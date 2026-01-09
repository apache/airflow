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

import os
from unittest import mock

import pytest

from airflow.models import DagRun, TaskInstance
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.common.compat.sdk import conf
from airflow.utils import timezone

from unit.apache.hive import DEFAULT_DATE, MockSubProcess, TestHiveEnvironment


class HiveOperatorConfigTest(TestHiveEnvironment):
    def test_hive_airflow_default_config_queue(self):
        op = HiveOperator(
            task_id="test_default_config_queue",
            hql=self.hql,
            mapred_queue_priority="HIGH",
            mapred_job_name="airflow.test_default_config_queue",
            dag=self.dag,
        )

        # just check that the correct default value in test_default.cfg is used
        test_config_hive_mapred_queue = conf.get("hive", "default_hive_mapred_queue")
        assert op.hook.mapred_queue == test_config_hive_mapred_queue

    def test_hive_airflow_default_config_queue_override(self):
        specific_mapred_queue = "default"
        op = HiveOperator(
            task_id="test_default_config_queue",
            hql=self.hql,
            mapred_queue=specific_mapred_queue,
            mapred_queue_priority="HIGH",
            mapred_job_name="airflow.test_default_config_queue",
            dag=self.dag,
        )

        assert op.hook.mapred_queue == specific_mapred_queue


class HiveOperatorTest(TestHiveEnvironment):
    def test_hiveconf_jinja_translate(self):
        hql = "SELECT ${num_col} FROM ${hiveconf:table};"
        op = HiveOperator(hiveconf_jinja_translate=True, task_id="dry_run_basic_hql", hql=hql, dag=self.dag)
        op.prepare_template()
        assert op.hql == "SELECT {{ num_col }} FROM {{ table }};"

    def test_hiveconf(self):
        hql = "SELECT * FROM ${hiveconf:table} PARTITION (${hiveconf:day});"
        op = HiveOperator(
            hiveconfs={"table": "static_babynames", "day": "{{ ds }}"},
            task_id="dry_run_basic_hql",
            hql=hql,
            dag=self.dag,
        )
        op.prepare_template()
        assert op.hql == "SELECT * FROM ${hiveconf:table} PARTITION (${hiveconf:day});"

    @mock.patch("airflow.providers.apache.hive.operators.hive.HiveOperator.hook", mock.MagicMock())
    def test_mapred_job_name(self, mock_hook):
        op = HiveOperator(task_id="test_mapred_job_name", hql=self.hql, dag=self.dag)

        fake_run_id = "test_mapred_job_name"
        fake_logical_date = timezone.datetime(2018, 6, 19)
        fake_ti = TaskInstance(task=op)
        fake_ti.dag_run = DagRun(run_id=fake_run_id, logical_date=fake_logical_date)
        fake_ti.hostname = "fake_hostname"
        fake_context = {"ti": fake_ti}

        op.execute(fake_context)
        assert (
            "Airflow HiveOperator task for "
            f"{fake_ti.hostname}.{self.dag.dag_id}.{op.task_id}.{fake_logical_date.isoformat()}"
            == mock_hook.mapred_job_name
        )


@pytest.mark.skipif(
    "AIRFLOW_RUNALL_TESTS" not in os.environ, reason="Skipped because AIRFLOW_RUNALL_TESTS is not set"
)
class TestHivePresto(TestHiveEnvironment):
    @mock.patch("tempfile.tempdir", "/tmp/")
    @mock.patch("tempfile._RandomNameSequence.__next__")
    @mock.patch("subprocess.Popen")
    def test_hive(self, mock_popen, mock_temp_dir):
        mock_subprocess = MockSubProcess()
        mock_popen.return_value = mock_subprocess
        mock_temp_dir.return_value = "tst"
        op = HiveOperator(task_id="basic_hql", hql=self.hql, dag=self.dag, mapred_job_name="test_job_name")

        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        hive_cmd = [
            "beeline",
            "-u",
            '"jdbc:hive2://localhost:10000/default"',
            "-hiveconf",
            "airflow.ctx.dag_id=test_dag_id",
            "-hiveconf",
            "airflow.ctx.task_id=basic_hql",
            "-hiveconf",
            "airflow.ctx.logical_date=2015-01-01T00:00:00+00:00",
            "-hiveconf",
            "airflow.ctx.dag_run_id=",
            "-hiveconf",
            "airflow.ctx.dag_owner=airflow",
            "-hiveconf",
            "airflow.ctx.dag_email=",
            "-hiveconf",
            "mapreduce.job.queuename=airflow",
            "-hiveconf",
            "mapred.job.queue.name=airflow",
            "-hiveconf",
            "tez.queue.name=airflow",
            "-hiveconf",
            "mapred.job.name=test_job_name",
            "-f",
            "/tmp/airflow_hiveop_tst/tmptst",
        ]

        mock_popen.assert_called_with(
            hive_cmd,
            stdout=mock_subprocess.PIPE,
            stderr=mock_subprocess.STDOUT,
            cwd="/tmp/airflow_hiveop_tst",
            close_fds=True,
        )

    @mock.patch("tempfile.tempdir", "/tmp/")
    @mock.patch("tempfile._RandomNameSequence.__next__")
    @mock.patch("subprocess.Popen")
    def test_hive_queues(self, mock_popen, mock_temp_dir):
        mock_subprocess = MockSubProcess()
        mock_popen.return_value = mock_subprocess
        mock_temp_dir.return_value = "tst"

        hive_cmd = [
            "beeline",
            "-u",
            '"jdbc:hive2://localhost:10000/default"',
            "-hiveconf",
            "airflow.ctx.dag_id=test_dag_id",
            "-hiveconf",
            "airflow.ctx.task_id=test_hive_queues",
            "-hiveconf",
            "airflow.ctx.logical_date=2015-01-01T00:00:00+00:00",
            "-hiveconf",
            "airflow.ctx.dag_run_id=",
            "-hiveconf",
            "airflow.ctx.dag_owner=airflow",
            "-hiveconf",
            "airflow.ctx.dag_email=",
            "-hiveconf",
            "mapreduce.job.queuename=default",
            "-hiveconf",
            "mapred.job.queue.name=default",
            "-hiveconf",
            "tez.queue.name=default",
            "-hiveconf",
            "mapreduce.job.priority=HIGH",
            "-hiveconf",
            "mapred.job.name=airflow.test_hive_queues",
            "-f",
            "/tmp/airflow_hiveop_tst/tmptst",
        ]
        op = HiveOperator(
            task_id="test_hive_queues",
            hql=self.hql,
            mapred_queue="default",
            mapred_queue_priority="HIGH",
            mapred_job_name="airflow.test_hive_queues",
            dag=self.dag,
        )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        mock_popen.assert_called_with(
            hive_cmd,
            stdout=mock_subprocess.PIPE,
            stderr=mock_subprocess.STDOUT,
            cwd="/tmp/airflow_hiveop_tst",
            close_fds=True,
        )

    @mock.patch("tempfile.tempdir", "/tmp/")
    @mock.patch("tempfile._RandomNameSequence.__next__")
    @mock.patch("subprocess.Popen")
    def test_hive_dryrun(self, mock_popen, mock_temp_dir):
        mock_subprocess = MockSubProcess()
        mock_popen.return_value = mock_subprocess
        mock_temp_dir.return_value = "tst"

        op = HiveOperator(task_id="dry_run_basic_hql", hql=self.hql, dag=self.dag)
        op.dry_run()

        hive_cmd = [
            "beeline",
            "-u",
            '"jdbc:hive2://localhost:10000/default"',
            "-hiveconf",
            "airflow.ctx.dag_id=",
            "-hiveconf",
            "airflow.ctx.task_id=",
            "-hiveconf",
            "airflow.ctx.logical_date=",
            "-hiveconf",
            "airflow.ctx.dag_run_id=",
            "-hiveconf",
            "airflow.ctx.dag_owner=",
            "-hiveconf",
            "airflow.ctx.dag_email=",
            "-hiveconf",
            "mapreduce.job.queuename=airflow",
            "-hiveconf",
            "mapred.job.queue.name=airflow",
            "-hiveconf",
            "tez.queue.name=airflow",
            "-f",
            "/tmp/airflow_hiveop_tst/tmptst",
        ]
        mock_popen.assert_called_with(
            hive_cmd,
            stdout=mock_subprocess.PIPE,
            stderr=mock_subprocess.STDOUT,
            cwd="/tmp/airflow_hiveop_tst",
            close_fds=True,
        )

    @mock.patch("tempfile.tempdir", "/tmp/")
    @mock.patch("tempfile._RandomNameSequence.__next__")
    @mock.patch("subprocess.Popen")
    def test_beeline(self, mock_popen, mock_temp_dir):
        mock_subprocess = MockSubProcess()
        mock_popen.return_value = mock_subprocess
        mock_temp_dir.return_value = "tst"

        hive_cmd = [
            "beeline",
            "-u",
            '"jdbc:hive2://localhost:10000/default"',
            "-hiveconf",
            "airflow.ctx.dag_id=test_dag_id",
            "-hiveconf",
            "airflow.ctx.task_id=beeline_hql",
            "-hiveconf",
            "airflow.ctx.logical_date=2015-01-01T00:00:00+00:00",
            "-hiveconf",
            "airflow.ctx.dag_run_id=",
            "-hiveconf",
            "airflow.ctx.dag_owner=airflow",
            "-hiveconf",
            "airflow.ctx.dag_email=",
            "-hiveconf",
            "mapreduce.job.queuename=airflow",
            "-hiveconf",
            "mapred.job.queue.name=airflow",
            "-hiveconf",
            "tez.queue.name=airflow",
            "-hiveconf",
            "mapred.job.name=test_job_name",
            "-f",
            "/tmp/airflow_hiveop_tst/tmptst",
        ]

        op = HiveOperator(
            task_id="beeline_hql",
            hive_cli_conn_id="hive_cli_default",
            hql=self.hql,
            dag=self.dag,
            mapred_job_name="test_job_name",
        )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        mock_popen.assert_called_with(
            hive_cmd,
            stdout=mock_subprocess.PIPE,
            stderr=mock_subprocess.STDOUT,
            cwd="/tmp/airflow_hiveop_tst",
            close_fds=True,
        )
