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

import mock

from airflow.kubernetes.pod_usage_metrics_logger import PodUsageMetricsLogger


class TestPodUsageMetricsLogger(unittest.TestCase):
    api_response_side_effect = [
        {"containers": [{"name": "base", "usage": {"cpu": "0", "memory": "10Ki"}}]},
        {"containers": [{"name": "base", "usage": {"cpu": "10m", "memory": "20Mi"}}]}
    ]

    invalid_api_response_side_effect = [
        {"containers": [{"name": "base", "usage": {"cpu": "0", "memory": "10KB"}}]},
        {"containers": [{"name": "base", "usage": {"cpu": "10n", "memory": "20Mi"}}]}
    ]

    def setUp(self):
        self.pod_launcher = mock.Mock()
        self.pod_launcher.call_api.side_effect = self.api_response_side_effect
        self.pod_launcher.pod_is_running.side_effect = [True] * 3 + [False]
        self.mock_pod = mock.Mock()
        self.mock_pod.metadata.name = 'Fake Pod Name'
        self.mock_pod.metadata.namespace = 'Fake Pod NameSpace'
        self.pod_usage_metrics_logger = PodUsageMetricsLogger(self.pod_launcher, self.mock_pod, 1, 1)
        self.pod_usage_metrics_logger._log = mock.MagicMock()
        # Mocking the logger (provided by LoggingMixin) to be able to inspect expected logging behavior.

    def test_log_pod_usage_metrics_with_negative_interval(self):
        self.pod_usage_metrics_logger.resource_usage_fetch_interval = -1
        self.pod_usage_metrics_logger.log_pod_usage_metrics()
        self.pod_usage_metrics_logger.log.error.assert_called_once()
        self.pod_usage_metrics_logger.log.info.assert_not_called()

    def test_log_pod_usage_metrics_call_counts(self):
        self.pod_usage_metrics_logger.log_pod_usage_metrics()
        self.pod_usage_metrics_logger.log.error.assert_not_called()
        self.assertEqual(self.pod_usage_metrics_logger.log.info.call_count, 6)
        # Three times pod_is_running returning true means two full cycle logging:
        # First call logs, second call is not due for logging and third call logs again.
        # There are three info logs for publishing max usage and one at the beginning to let user know
        # monitoring thread started. So there will be total of 6 info logs (two log cycle, three for max
        # usage and one at the beginning)
        self.assertEqual(self.pod_launcher.pod_is_running.call_count, 4)
        # There will be exactly 4 calls to pod_is_running till False is returned.
        self.assertEqual(self.pod_launcher.call_api.call_count, 2)
        # Two log cycles means two api calls to metric server

    def test_log_pod_usage_metrics_max_usages(self):
        self.pod_usage_metrics_logger.log_pod_usage_metrics()
        self.assertEqual(10, self.pod_usage_metrics_logger.max_cpu_usages['base'])
        self.assertEqual(20 * (2**10), self.pod_usage_metrics_logger.max_memory_usages['base'])

    def test_log_pod_usage_metrics_invalid_usage_units(self):
        self.pod_launcher.call_api.side_effect = self.invalid_api_response_side_effect
        self.pod_usage_metrics_logger.log_pod_usage_metrics()
        self.assertEqual(float('inf'), self.pod_usage_metrics_logger.max_cpu_usages['base'])
        self.assertEqual(float('inf'), self.pod_usage_metrics_logger.max_memory_usages['base'])
        # If invalid units are encountered infinite usage is shown as an indication.

    def test_convert_str_cpu_usage_to_int_millicpu(self):
        self.assertEqual(0, self.pod_usage_metrics_logger._convert_str_cpu_usage_to_int_millicpu('0'))
        self.assertEqual(10, self.pod_usage_metrics_logger._convert_str_cpu_usage_to_int_millicpu('10'))
        self.assertEqual(0, self.pod_usage_metrics_logger._convert_str_cpu_usage_to_int_millicpu('0m'))
        self.assertEqual(10, self.pod_usage_metrics_logger._convert_str_cpu_usage_to_int_millicpu('10m'))
        self.assertEqual(float('inf'),
                         self.pod_usage_metrics_logger._convert_str_cpu_usage_to_int_millicpu('0n'))
        self.assertEqual(float('inf'),
                         self.pod_usage_metrics_logger._convert_str_cpu_usage_to_int_millicpu('m'))

    def test_convert_str_memory_usage_to_int_kilobyte(self):
        self.assertEqual(0, self.pod_usage_metrics_logger._convert_str_memory_usage_to_int_kilobyte('0'))
        self.assertEqual(10, self.pod_usage_metrics_logger._convert_str_memory_usage_to_int_kilobyte('10'))
        self.assertEqual(0, self.pod_usage_metrics_logger._convert_str_memory_usage_to_int_kilobyte('0Ki'))
        self.assertEqual(0, self.pod_usage_metrics_logger._convert_str_memory_usage_to_int_kilobyte('0Mi'))
        self.assertEqual(0, self.pod_usage_metrics_logger._convert_str_memory_usage_to_int_kilobyte('0Gi'))
        self.assertEqual(0, self.pod_usage_metrics_logger._convert_str_memory_usage_to_int_kilobyte('0Ti'))
        self.assertEqual(0, self.pod_usage_metrics_logger._convert_str_memory_usage_to_int_kilobyte('0Pi'))
        self.assertEqual(0, self.pod_usage_metrics_logger._convert_str_memory_usage_to_int_kilobyte('0Ei'))
        self.assertEqual(10, self.pod_usage_metrics_logger._convert_str_memory_usage_to_int_kilobyte('10Ki'))
        self.assertEqual(10 * 2 ** 10,
                         self.pod_usage_metrics_logger._convert_str_memory_usage_to_int_kilobyte('10Mi'))
        self.assertEqual(10 * 2 ** 20,
                         self.pod_usage_metrics_logger._convert_str_memory_usage_to_int_kilobyte('10Gi'))
        self.assertEqual(10 * 2 ** 30,
                         self.pod_usage_metrics_logger._convert_str_memory_usage_to_int_kilobyte('10Ti'))
        self.assertEqual(10 * 2 ** 40,
                         self.pod_usage_metrics_logger._convert_str_memory_usage_to_int_kilobyte('10Pi'))
        self.assertEqual(10 * 2 ** 50,
                         self.pod_usage_metrics_logger._convert_str_memory_usage_to_int_kilobyte('10Ei'))
        self.assertEqual(float('inf'),
                         self.pod_usage_metrics_logger._convert_str_memory_usage_to_int_kilobyte('10GB'))
