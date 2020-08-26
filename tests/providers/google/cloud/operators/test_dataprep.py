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
from unittest import TestCase, mock

from airflow.providers.google.cloud.operators.dataprep import DataprepGetJobsForJobGroupOperator

JOB_ID = 143
TASK_ID = "dataprep_job"


class TestDataprepGetJobsForJobGroupOperator(TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dataprep.GoogleDataprepHook")
    def test_execute(self, hook_mock):
        op = DataprepGetJobsForJobGroupOperator(job_id=JOB_ID, task_id=TASK_ID)
        op.execute(context={})
        hook_mock.assert_called_once_with(dataprep_conn_id='dataprep_conn_id')
        hook_mock.return_value.get_jobs_for_job_group.assert_called_once_with(job_id=JOB_ID)
