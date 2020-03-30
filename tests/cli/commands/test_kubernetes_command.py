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
import os
import unittest
from datetime import datetime
from unittest.mock import call

import mock

from airflow.cli import cli_parser
from airflow.cli.commands import kubernetes_command
from airflow.models import DagBag
from airflow.utils import timezone
from tests.test_utils.config import conf_vars

dag_folder_path = '/'.join(os.path.realpath(__file__).split('/')[:-1])

DEFAULT_DATE = timezone.make_aware(datetime(2015, 1, 1))
TEST_DAG_FOLDER = os.path.join(
    os.path.dirname(dag_folder_path), 'dags')
TEST_DAG_ID = 'unit_tests'


EXAMPLE_DAGS_FOLDER = os.path.join(
    os.path.dirname(
        os.path.dirname(
            os.path.dirname(os.path.realpath(__file__))
        )
    ),
    "airflow/example_dags"
)


class TestCliDags(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dagbag = DagBag(include_examples=True)
        cls.parser = cli_parser.get_parser()

    @conf_vars({
        ('core', 'load_examples'): 'true'
    })
    @mock.patch("yaml.dump")
    def test_cli_kubernetes_preview(self, mock_dump):
        args = self.parser.parse_args(['kubernetes',
                                       'pod-preview',
                                       'example_kubernetes_pod_operator'])
        kubernetes_command.kubernetes_preview(args)
        expected_calls = [
            call(
                {'apiVersion': 'v1', 'kind': 'Pod',
                 'metadata': {'annotations': {},
                              'labels': {},
                              'name': 'pod-ex-minimum', 'namespace': 'beta'},
                 'spec':
                 {'affinity': {},
                  'containers':
                  [{'args': [],
                    'command': ['echo'],
                    'env': [],
                    'envFrom': [],
                    'image': 'gcr.io/gcp-runtimes/ubuntu_18_0_4', 'imagePullPolicy': 'IfNotPresent', 'name': 'base',
                    'ports': [],
                    'resources': [],
                    'volumeMounts': []}],
                  'hostNetwork': False, 'imagePullSecrets': [],
                  'initContainers': [],
                  'nodeSelector': {},
                  'securityContext': {},
                  'serviceAccountName': 'default', 'tolerations': [],
                  'volumes': []}}),
            call(
                {'apiVersion': 'v1', 'kind': 'Pod',
                 'metadata': {'annotations': {},
                              'labels': {},
                              'name': 'ex-kube-templates', 'namespace': 'prod'},
                 'spec':
                 {'affinity': {},
                  'containers':
                  [{'args': ['{{ ds }}'],
                    'command': ['echo'],
                    'env':
                    [{'name': 'MY_VALUE', 'value': '{{ var.value.my_value }}'},
                     {'name': 'SQL_CONN',
                      'valueFrom': {'secretKeyRef': {'key': 'sql_alchemy_conn', 'name': 'airflow-secrets'}}}],
                    'envFrom': [],
                    'image': 'bash', 'imagePullPolicy': 'IfNotPresent', 'name': 'base', 'ports': [],
                    'resources': [],
                    'volumeMounts': []}],
                  'hostNetwork': False, 'imagePullSecrets': [],
                  'initContainers': [],
                  'nodeSelector': {},
                  'securityContext': {},
                  'serviceAccountName': 'default', 'tolerations': [],
                  'volumes': []}}),
            call(
                {'apiVersion': 'v1', 'kind': 'Pod',
                 'metadata': {'annotations': {},
                              'labels': {},
                              'name': 'ex-pod-affinity', 'namespace': 'master'},
                 'spec':
                 {
                     'affinity':
                     {
                         'nodeAffinity':
                         {
                             'requiredDuringSchedulingIgnoredDuringExecution':
                             {
                                 'nodeSelectorTerms':
                                 [{
                                     'matchExpressions':
                                     [{'key': 'cloud.google.com/gke-nodepool', 'operator': 'In', 'values':
                                       ['pool-0', 'pool-1']}]}]}}},
                     'containers':
                     [{'args': ['-Mbignum=bpi', '-wle', 'print bpi(2000)'],
                       'command': ['perl'],
                       'env': [],
                       'envFrom': [],
                       'image': 'perl', 'imagePullPolicy': 'IfNotPresent', 'name': 'base', 'ports': [],
                       'resources': [],
                       'volumeMounts': []}],
                     'hostNetwork': False, 'imagePullSecrets': [],
                     'initContainers': [],
                     'nodeSelector': {},
                     'securityContext': {},
                     'serviceAccountName': 'default', 'tolerations': [],
                     'volumes': []}}),
            call(
                {'apiVersion': 'v1', 'kind': 'Pod',
                 'metadata':
                 {'annotations': {'key1': 'value1'},
                  'labels': {'pod-label': 'label-name'},
                  'name': 'pi', 'namespace': 'default'},
                 'spec':
                 {'affinity': {},
                  'containers':
                  [{'args': ['-Mbignum=bpi', '-wle', 'print bpi(2000)'],
                    'command': ['perl'],
                    'env': [{'name': 'EXAMPLE_VAR', 'value': '/example/value'}],
                    'envFrom': [],
                    'image': 'perl', 'imagePullPolicy': 'Always', 'name': 'base', 'ports': [],
                    'resources':
                    {'limits': {'cpu': 1, 'memory': 1, 'nvidia.com/gpu': None, 'ephemeral-storage': None},
                     'requests': {'cpu': None, 'memory': None, 'ephemeral-storage': None}},
                    'volumeMounts': []}],
                  'hostNetwork': False, 'imagePullSecrets': [],
                  'initContainers': [],
                  'nodeSelector': {},
                  'securityContext': {},
                  'serviceAccountName': 'default', 'tolerations': [],
                  'volumes': []}})]

        mock_dump.assert_has_calls(expected_calls, any_order=True)
