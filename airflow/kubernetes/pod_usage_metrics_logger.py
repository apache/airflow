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
"""Logs resource usage of a pod"""
import re
import time
from collections import defaultdict
from datetime import datetime as dt, timedelta

from kubernetes.client.models.v1_pod import V1Pod

from airflow import LoggingMixin


class PodUsageMetricsLogger(LoggingMixin):
    """
    This class logs all the resource usage metrics of a pod during the pod life time.
    """
    memory_usage_units = {'Ki': 1,
                          'Mi': 2 ** 10,
                          'Gi': 2 ** 20,
                          'Ti': 2 ** 30,
                          'Pi': 2 ** 40,
                          'Ei': 2 ** 50,
                          '': 1}
    # Sometimes memory usage is not followed by unit if it is 0
    # In these cases regex might return '' or None for unit.
    valid_memory_usage_regex = re.compile(r'^([+]?[0-9]+?([.][0-9]+)?)(([KMGTPE][i])?)$')
    # This regex checks for a positive float number with optional Ki, Mi, Gi, Ti, Pi, or Ei as unit

    cpu_usage_units = {'m': 1,
                       '': 1}  # Sometimes cpu usage is not followed by the unit when usage is 0
    valid_cpu_usage_regex = re.compile(r'^([+]?[0-9]+?([.][0-9]+)?)([m]?)$')
    # This regex check for a positive float number with optional millicpu(m) unit

    def __init__(self,
                 pod_launcher: object,  # (PodLauncher type) importing it might causes circular imports
                 pod: V1Pod,
                 resource_usage_fetch_interval: int,
                 resource_usage_log_interval: int):
        """
        Creates the usage metrics logger.
        :param pod_launcher: reference to the launcher of the metrics logger
        :param pod: the pod we are logging the usage metrics
        :param resource_usage_log_interval: logging interval in seconds
        """
        super().__init__()
        self.pod_launcher = pod_launcher
        self.pod = pod
        self.resource_usage_fetch_interval = resource_usage_fetch_interval
        self.resource_usage_log_interval = resource_usage_log_interval
        self.max_cpu_usages = defaultdict(lambda: -1)  # type: dict
        self.max_memory_usages = defaultdict(lambda: -1)  # type: dict

    def log_pod_usage_metrics(self):
        """
        Periodically calls metrics api for the pod, logs current usage of the pod per container, and keeps
        track of max usage seen for all containers.
        :return:
        """
        if self.resource_usage_fetch_interval <= 0:
            self.log.error('Parameter resource_usage_fetch_interval must be positive. Cancelling pod usage '
                           'monitoring thread for pod: {0}.'.format(self.pod.metadata.name))
            return
        self.log.info('Pod usage monitoring thread started for pod: {0}'.format(self.pod.metadata.name))
        resource_usage_api_url = "/apis/metrics.k8s.io/v1beta1/namespaces/{0}/pods/{1}"\
                                 .format(self.pod.metadata.namespace,
                                         self.pod.metadata.name)
        cur_time = dt.now()
        last_fetch_time = cur_time - timedelta(seconds=self.resource_usage_fetch_interval)
        last_log_time = cur_time - timedelta(seconds=self.resource_usage_log_interval)
        while self.pod_launcher.pod_is_running(self.pod):
            if cur_time < last_fetch_time + timedelta(seconds=self.resource_usage_fetch_interval):
                time.sleep(1)
                cur_time = dt.now()
                continue
            try:
                self.log.debug('Calling metrics api: {0}'.format(resource_usage_api_url))
                last_fetch_time = dt.now()
                resp = self.pod_launcher.call_api(resource_usage_api_url,
                                                  'GET',
                                                  _preload_content=True,
                                                  response_type=object,
                                                  _return_http_data_only=True)
            except Exception as e:  # pylint: disable=broad-except
                self.log.error('Failed to fetch usage for pod: {0}, Exception: {1}'
                               .format(self.pod.metadata.name, e))
                continue
            containers_usages = self._get_cpu_memory_usage_from_api_response(resp)
            for container_name, cpu_usage, memory_usage in containers_usages:
                self._update_max_usages(container_name, cpu_usage, memory_usage)

            cur_time = dt.now()  # Refreshing current time since api call may take a bit sometimes
            if cur_time >= last_log_time + timedelta(seconds=self.resource_usage_log_interval):
                last_log_time = dt.now()
                for container_name, cpu_usage, memory_usage in containers_usages:
                    self.log.info('Pod: {0}, container: {1} -- cpu usage: {2}, memory usage: {3}'
                                  .format(self.pod.metadata.name,
                                          container_name,
                                          cpu_usage,
                                          memory_usage))
        self._report_max_usages()

    def _get_cpu_memory_usage_from_api_response(self, metric_api_response: dict):
        """
        This method gets the metric server response and extracts cpu and memory usage (as strings)
        for all containers.
        :param metric_api_response:
        :return: list of triplets (container_name: str, cpu_usage: str, memory_usage:str)
        """
        containers_usages = []
        try:
            containers = metric_api_response['containers']
            for container in containers:
                container_name = container['name']
                cpu_usage = container['usage']['cpu']
                mem_usage = container['usage']['memory']
                containers_usages.append((container_name, cpu_usage, mem_usage))
        except Exception as e:  # pylint: disable=broad-except
            self.log.error('Failed to get containers usages from metric api response: {0}, Exception: {1}'
                           .format(metric_api_response, e))
        return containers_usages

    def _update_max_usages(self, container_name: str, cpu_usage: str, memory_usage: str):
        """
        This method gets the metric server response and extracts cpu and memory usage (as strings)
        for all containers.
        :param container_name:
        :param cpu_usage:
        :param memory_usage:
        :param max_cpu_usages: map of container names to max cpu usages
        :param max_memory_usages: map of container names to max memory usages
        :return:
        """
        if not container_name:
            return
        cpu_usage_millicpu = self._convert_str_cpu_usage_to_int_millicpu(cpu_usage)
        self.max_cpu_usages[container_name] = max(self.max_cpu_usages[container_name], cpu_usage_millicpu)
        memory_usage_kilobyte = self._convert_str_memory_usage_to_int_kilobyte(memory_usage)
        self.max_memory_usages[container_name] = max(self.max_memory_usages[container_name],
                                                     memory_usage_kilobyte)

    def _convert_str_cpu_usage_to_int_millicpu(self, cpu_usage: str):
        """
        Given string representation of cpu usage returned by metrics api (in millicpu), this method
        validates and converts the string into integer value in millicpu.
        :param cpu_usage: string representation of cpu usage in millicpu like: (120m, 1m, 0m, or just 0)
        :return: integer value for cpu usage in millicpu
        """
        match = self.valid_cpu_usage_regex.match(cpu_usage)
        if not match:
            self.log.error('Cpu usage string {0} is not a valid cpu usage format and cannot be converted.'
                           .format(cpu_usage))
            return float('inf')
        num_str = match.group(1)
        unit = match.group(3)
        if unit not in self.cpu_usage_units:
            self.log.error('Detected invalid cpu usage unit: {0}'.format(unit))
            return float('inf')
        try:
            return int(num_str) * self.cpu_usage_units[unit]
        except Exception as e:  # pylint: disable=broad-except
            self.log.error('Failed to convert string to int for: {0}, Exception: {1}'.format(num_str, e))
        return float('inf')

    def _convert_str_memory_usage_to_int_kilobyte(self, memory_usage: str):
        """
        Given the string representation of memory usage by metrics server, this method converts the string
        into integer value for memory usage in kilo bytes.
        :param memory_usage: string representation of memory usge like: (120Ki, 22Mi, 1Gi, ...)
        :return: integer value for memory usage in kilo bytes.
        """
        match = self.valid_memory_usage_regex.match(memory_usage)
        if not match:
            self.log.error('Memory usage string {0} is not a valid usage format and cannot be converted.'
                           .format(memory_usage))
            return float('inf')
        num_str = match.group(1)
        unit = match.group(3)
        if unit not in self.memory_usage_units:
            self.log.error('Detected invalid memory usage unit: {0}'.format(unit))
            return float('inf')
        try:
            return int(num_str) * self.memory_usage_units[unit]
        except Exception as e:  # pylint: disable=broad-except
            self.log.error('Failed to convert string to int for: {0}, Exception: {1}'.format(num_str, e))
        return float('inf')

    def _report_max_usages(self):
        """
        logs the max usage of all containers seen during this pod runtime. Infinite usage means
        somewhere during parsing unit of usages unknown usage unit was encountered. This might happen
        if metric server unit format
        changes.
        :param max_cpu_usages: map of container names to max cpu usages
        :param max_memory_usages: map of container names to max memory usages
        :return:
        """
        self.log.info('Please note following usage summary is based on sampled usage fetches at regular '
                      'intervals specified in KubernetesPodOperator. An infinite max usage means there '
                      'were issues converting resource usage units. Please check individual resource '
                      'usage logs if needed. If a container is missing or nothing is published, it means '
                      'none of the api calls to metric server came back successfully (most likely due to '
                      'permissions and rbac in your cluster).')
        for container_name in self.max_memory_usages:
            self.log.info('max memory usage for container {0} was: {1}KB.'
                          .format(container_name, self.max_memory_usages[container_name]))
        for container_name in self.max_cpu_usages:
            self.log.info('max cpu usage for container {0} was: {1} millicpus.'
                          .format(container_name, self.max_cpu_usages[container_name]))
