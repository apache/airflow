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

from unittest import mock

from airflow import AirflowException
from airflow.providers.amazon.aws.sensors.glacier import GlacierJobOperationSensor, JobStatus

SUCCEEDED = "Succeeded"
IN_PROGRESS = "InProgress"


class TestAmazonGlacierSensor(unittest.TestCase):
    def setUp(self):
        self.op = GlacierJobOperationSensor(
            task_id='test_athena_sensor',
            aws_conn_id='aws_default',
            vault_name="airflow",
            job_id="1a2b3c4d",
            poke_interval=60 * 20,
        )

    @mock.patch(
        "airflow.providers.amazon.aws.sensors.glacier.GlacierHook.describe_job",
        side_effect=[{"Action": "", "StatusCode": JobStatus.SUCCEEDED.value}],
    )
    def test_poke_succeeded(self, _):
        self.assertTrue(self.op.poke(None))

    @mock.patch(
        "airflow.providers.amazon.aws.sensors.glacier.GlacierHook.describe_job",
        side_effect=[{"Action": "", "StatusCode": JobStatus.IN_PROGRESS.value}],
    )
    def test_poke_in_progress(self, _):
        self.assertFalse(self.op.poke(None))

    @mock.patch(
        "airflow.providers.amazon.aws.sensors.glacier.GlacierHook.describe_job",
        side_effect=[{"Action": "", "StatusCode": ""}],
    )
    def test_poke_fail(self, _):
        with self.assertRaises(AirflowException) as context:
            self.op.poke(None)
        self.assertIn('Sensor failed', str(context.exception))


class TestSensorJobDescription(unittest.TestCase):
    def test_job_status_success(self):
        self.assertEqual(JobStatus.SUCCEEDED.value, SUCCEEDED)

    def test_job_status_in_progress(self):
        self.assertEqual(JobStatus.IN_PROGRESS.value, IN_PROGRESS)
