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
from unittest.mock import patch

import pytest

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import Connection
from airflow.providers.google.cloud.sensors.dataprocgdc import DataprocGDCKrmSensor
from airflow.utils import db, timezone


@patch("airflow.providers.cncf.kubernetes.hooks.kubernetes.KubernetesHook.get_conn")
class TestDataprocGDCSparkSubmitKrmSensor:
    TEST_COMPLETED_APPLICATION = {
        "apiVersion": "dataprocgdc.cloud.google.com/v1alpha1",
        "kind": "SparkApplication",
        "metadata": {
            "name": "spark-pi-2024-02-24-1",
            "namespace": "default",
            "resourceVersion": "455577",
            "uid": "9f825516-6e1a-4af1-8967-b05661e8fb08",
        },
        "spec": {
            "properties": {
                "spark.hadoop.fs.s3a.access.key": "***",
                "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
                "spark.hadoop.fs.s3a.endpoint": "***",
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3a.secret.key": "***",
            },
            "pySparkApplicationConfig": {},
            "sparkApplicationConfig": {
                "args": ["100000"],
                "jarFileUris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
                "mainClass": "org.apache.spark.examples.SparkPi",
            },
            "sparkRApplicationConfig": {},
            "sparkSqlApplicationConfig": {},
        },
        "status": {
            "imageUri": "us-central1-docker.pkg.dev/cloud-dataproc/dpgdce/spark:driver_log_fix",
            "state": "Succeeded",
        },
    }

    TEST_FAILED_APPLICATION = {
        "apiVersion": "dataprocgdc.cloud.google.com/v1alpha1",
        "kind": "SparkApplication",
        "metadata": {
            "name": "spark-pi-2024-02-24-1",
            "namespace": "default",
            "resourceVersion": "455577",
            "uid": "9f825516-6e1a-4af1-8967-b05661e8fb08",
        },
        "spec": {
            "properties": {
                "spark.hadoop.fs.s3a.access.key": "***",
                "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
                "spark.hadoop.fs.s3a.endpoint": "***",
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3a.secret.key": "***",
            },
            "pySparkApplicationConfig": {},
            "sparkApplicationConfig": {
                "args": ["100000"],
                "jarFileUris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
                "mainClass": "org.apache.spark.examples.SparkPi",
            },
            "sparkRApplicationConfig": {},
            "sparkSqlApplicationConfig": {},
        },
        "status": {
            "imageUri": "us-central1-docker.pkg.dev/cloud-dataproc/dpgdce/spark:driver_log_fix",
            "state": "Failed",
        },
    }

    TEST_RUNNING_APPLICATION = {
        "apiVersion": "dataprocgdc.cloud.google.com/v1alpha1",
        "kind": "SparkApplication",
        "metadata": {
            "name": "spark-pi-2024-02-24-1",
            "namespace": "default",
            "resourceVersion": "455577",
            "uid": "9f825516-6e1a-4af1-8967-b05661e8fb08",
        },
        "spec": {
            "properties": {
                "spark.hadoop.fs.s3a.access.key": "***",
                "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
                "spark.hadoop.fs.s3a.endpoint": "***",
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3a.secret.key": "***",
            },
            "pySparkApplicationConfig": {},
            "sparkApplicationConfig": {
                "args": ["100000"],
                "jarFileUris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
                "mainClass": "org.apache.spark.examples.SparkPi",
            },
            "sparkRApplicationConfig": {},
            "sparkSqlApplicationConfig": {},
        },
        "status": {
            "imageUri": "us-central1-docker.pkg.dev/cloud-dataproc/dpgdce/spark:driver_log_fix",
            "state": "Running",
        },
    }

    def setup_method(self):
        db.merge_conn(Connection(conn_id="kubernetes_default", conn_type="kubernetes", extra=json.dumps({})))

        args = {"owner": "airflow", "start_date": timezone.datetime(2024, 2, 1)}
        self.dag = DAG("test_dag_id", default_args=args)

    @pytest.mark.db_test
    def test_init(self, mock_kubernetes_hook):
        sensor = DataprocGDCKrmSensor(task_id="task", application_name="application")

        assert sensor.kubernetes_conn_id == "kubernetes_default"
        assert sensor.api_group == "dataprocgdc.cloud.google.com"
        assert sensor.api_version == "v1alpha1"

        assert "hook" not in sensor.__dict__  # Cached property has not been accessed as part of construction.

    @pytest.mark.db_test
    @patch(
        "kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object",
        return_value=TEST_COMPLETED_APPLICATION,
    )
    def test_completed_application(self, mock_get_namespaced_crd, mock_kubernetes_hook):
        sensor = DataprocGDCKrmSensor(application_name="spark_pi", dag=self.dag, task_id="test_task_id")
        assert sensor.poke({})
        mock_kubernetes_hook.assert_called_once_with()
        mock_get_namespaced_crd.assert_called_once_with(
            group="dataprocgdc.cloud.google.com",
            name="spark_pi",
            namespace="default",
            plural="sparkapplications",
            version="v1alpha1",
        )

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        "soft_fail, expected_exception", ((False, AirflowException), (True, AirflowSkipException))
    )
    @patch(
        "kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object",
        return_value=TEST_FAILED_APPLICATION,
    )
    def test_failed_application(
        self,
        mock_get_namespaced_crd,
        mock_kubernetes_hook,
        soft_fail: bool,
        expected_exception: type[AirflowException],
    ):
        sensor = DataprocGDCKrmSensor(
            application_name="spark_pi", dag=self.dag, task_id="test_task_id", soft_fail=soft_fail
        )
        with pytest.raises(expected_exception):
            sensor.poke({})
        mock_kubernetes_hook.assert_called_once_with()
        mock_get_namespaced_crd.assert_called_once_with(
            group="dataprocgdc.cloud.google.com",
            name="spark_pi",
            namespace="default",
            plural="sparkapplications",
            version="v1alpha1",
        )

    @pytest.mark.db_test
    @patch(
        "kubernetes.client.api.custom_objects_api.CustomObjectsApi.get_namespaced_custom_object",
        return_value=TEST_RUNNING_APPLICATION,
    )
    def test_running_application(self, mock_get_namespaced_crd, mock_kubernetes_hook):
        sensor = DataprocGDCKrmSensor(application_name="spark_pi", dag=self.dag, task_id="test_task_id")
        assert not sensor.poke({})
        mock_kubernetes_hook.assert_called_once_with()
        mock_get_namespaced_crd.assert_called_once_with(
            group="dataprocgdc.cloud.google.com",
            name="spark_pi",
            namespace="default",
            plural="sparkapplications",
            version="v1alpha1",
        )
