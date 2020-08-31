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

from airflow.providers.google.cloud.operators.dataprep import (
    DataprepGetJobGroupOperator,
    DataprepGetJobsForJobGroupOperator,
    DataprepRunJobGroupOperator,
)

DATAPREP_CONN_ID = "dataprep_default"
JOB_ID = 143
TASK_ID = "dataprep_job"
INCLUDE_DELETED = False
EMBED = ""
DATAPREP_JOB_RECIPE_ID = 1234567
PATH_TO_OUTOUT_FILE = "path_to_output_file"
DATA = {
    "wrangledDataset": {"id": DATAPREP_JOB_RECIPE_ID},
    "overrides": {
        "execution": "dataflow",
        "profiler": False,
        "writesettings": [
            {
                "path": PATH_TO_OUTOUT_FILE,
                "action": "create",
                "format": "csv",
                "compression": "none",
                "header": False,
                "asSingleFile": False,
            }
        ],
    },
}


class TestDataprepGetJobsForJobGroupOperator(TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dataprep.GoogleDataprepHook")
    def test_execute(self, hook_mock):
        op = DataprepGetJobsForJobGroupOperator(
            dataprep_conn_id=DATAPREP_CONN_ID, job_id=JOB_ID, task_id=TASK_ID
        )
        op.execute(context={})
        hook_mock.assert_called_once_with(dataprep_conn_id="dataprep_default")
        hook_mock.return_value.get_jobs_for_job_group.assert_called_once_with(job_id=JOB_ID)


class TestDataprepGetJobGroupOperator(TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dataprep.GoogleDataprepHook")
    def test_execute(self, hook_mock):
        op = DataprepGetJobGroupOperator(
            dataprep_conn_id=DATAPREP_CONN_ID,
            job_group_id=JOB_ID,
            embed=EMBED,
            include_deleted=INCLUDE_DELETED,
            task_id=TASK_ID,
        )
        op.execute(context={})
        hook_mock.assert_called_once_with(dataprep_conn_id="dataprep_default")
        hook_mock.return_value.get_job_group.assert_called_once_with(
            job_group_id=JOB_ID, embed=EMBED, include_deleted=INCLUDE_DELETED
        )


class TestDataprepRunJobGroupOperator(TestCase):
    @mock.patch("airflow.providers.google.cloud.operators.dataprep.GoogleDataprepHook")
    def test_execute(self, hook_mock):
        op = DataprepRunJobGroupOperator(
            dataprep_conn_id=DATAPREP_CONN_ID, body_request=DATA, task_id=TASK_ID
        )
        op.execute(context=None)
        hook_mock.assert_called_once_with(dataprep_conn_id="dataprep_default")
        hook_mock.return_value.run_job_group.assert_called_once_with(body_request=DATA)
