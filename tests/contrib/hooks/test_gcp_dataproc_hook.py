# -*- coding: utf-8 -*-
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
#

import unittest
from airflow.contrib.hooks.gcp_dataproc_hook import DataProcHook, _DataProcOperation

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

JOB = 'test-job'
PROJECT_ID = 'test-project-id'
REGION = 'global'
TASK_ID = 'test-task-id'

BASE_STRING = 'airflow.contrib.hooks.gcp_api_base_hook.{}'
DATAPROC_STRING = 'airflow.contrib.hooks.gcp_dataproc_hook.{}'

def mock_init(self, gcp_conn_id, delegate_to=None):
    pass

class DataProcHookTest(unittest.TestCase):
    def setUp(self):
        with mock.patch(BASE_STRING.format('GoogleCloudBaseHook.__init__'),
                        new=mock_init):
            self.dataproc_hook = DataProcHook()

    @mock.patch(DATAPROC_STRING.format('_DataProcJob'))
    def test_submit(self, job_mock):
        with mock.patch(DATAPROC_STRING.format('DataProcHook.get_conn',
                                               return_value=None)):
            self.dataproc_hook.submit(PROJECT_ID, JOB)
            job_mock.assert_called_once_with(mock.ANY, PROJECT_ID, JOB, REGION)

    def test_successful_operation_detector(self):
        operation_api_response = \
            {
                "done": True,
                "metadata": {
                    "@type": "type.googleapis.com/google.cloud.dataproc.v1beta2."
                             "WorkflowMetadata",
                    "clusterName": "fake-dataproc-cluster",
                    "createCluster": {
                        "done": True,
                        "operationId": "projects/my-project/regions/us-central1/"
                                       "operations/1111111-0000-aaaa-bbbb-ffffffffffff"
                    },
                    "deleteCluster": {
                        "done": True,
                        "operationId": "projects/my-project/regions/us-central1/"
                                       "operations/1111111-1111-aaaa-bbbb-ffffffffffff"
                    },
                    "graph": {
                        "nodes": [
                            {
                                "jobId": "my-job-abcdefghijklm",
                                "state": "COMPLETED",
                                "stepId": "my-job"
                            }
                        ]
                    },
                    "state": "DONE"
                },
                "name": "projects/my-project/regions/us-central1/operations/"
                        "dddddddd-dddd-dddd-dddd-dddddddddddd",
                "response": {
                    "@type": "type.googleapis.com/google.protobuf.Empty"
                }
            }

        operation = _DataProcOperation(None, operation_api_response)

        self.assertTrue(operation._check_done())

    def test_failed_operation_detector(self):
        failure_response = \
            {
                "done": True,
                "metadata": {
                    "@type": "type.googleapis.com/google.cloud.dataproc."
                             "v1beta2.WorkflowMetadata",
                    "clusterName": "fake-dataproc-cluster",
                    "createCluster": {
                        "done": True,
                        "operationId": "projects/my-project/regions/us-central1/"
                                       "operations/1111111-0000-aaaa-bbbb-ffffffffffff"
                    },
                    "deleteCluster": {
                        "done": True,
                        "operationId": "projects/my-project/regions/us-central1/"
                                       "operations/1111111-1111-aaaa-bbbb-ffffffffffff"
                    },
                    "graph": {
                        "nodes": [
                            {
                                "error": "Google Cloud Dataproc Agent reports job"
                                         "failure. If logs are available, they can"
                                         " be found in 'gs://dataproc-00000000-0000-"
                                         "0000-0000-000000000000-us-central1/"
                                         "google-cloud-dataproc-metainfo/cccccccc-cccc-"
                                         "cccc-cccc-cccccccccccc/jobs/"
                                         "my-job-abcdefghijklm/driveroutput'.",
                                "jobId": "my-job-abcdefghijklm",
                                "state": "FAILED",
                                "stepId": "my-job"
                            }
                        ]
                    },
                    "state": "DONE"
                },
                "name": "projects/my-project/regions/us-central1/operations/"
                        "dddddddd-dddd-dddd-dddd-dddddddddddd",
                "response": {
                    "@type": "type.googleapis.com/google.protobuf.Empty"
                }
            }

        operation = _DataProcOperation(None, failure_response)

        with self.assertRaises(Exception) as context:
            operation._check_done()

        self.assertTrue(context.exception.message.startswith(
            'Google Dataproc Operation'))
