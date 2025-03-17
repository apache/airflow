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

from unittest import mock

from airflow.providers.google.cloud.links.cloud_run import CloudRunJobLoggingLink

TEST_LOG_URI = (
    "https://console.cloud.google.com/run/jobs/logs?project=test-project&region=test-region&job=test-job"
)


class TestCloudRunJobLoggingLink:
    def test_class_attributes(self):
        assert CloudRunJobLoggingLink.key == "log_uri"
        assert CloudRunJobLoggingLink.name == "Cloud Run Job Logging"

    def test_persist(self):
        mock_context, mock_task_instance = mock.MagicMock(), mock.MagicMock()

        CloudRunJobLoggingLink.persist(
            context=mock_context,
            task_instance=mock_task_instance,
            log_uri=TEST_LOG_URI,
        )

        mock_task_instance.xcom_push.assert_called_once_with(
            mock_context,
            key=CloudRunJobLoggingLink.key,
            value=TEST_LOG_URI,
        )
