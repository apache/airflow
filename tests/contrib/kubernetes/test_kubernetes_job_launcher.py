# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and

import unittest
from airflow.contrib.kubernetes.kubernetes_job_builder import KubernetesJobBuilder
from airflow.contrib.kubernetes.kubernetes_request_factory import SimpleJobRequestFactory
from airflow import configuration
import json

secrets = {}
labels = {}
base_job = {'kind': 'Job',
            'spec': {
                'template': {
                    'spec': {
                        'restartPolicy': 'Never',
                        'volumes': [{'hostPath': {'path': '/tmp/dags'}, 'name': 'shared-data'}],
                        'containers': [
                            {'command': ['try', 'this', 'first'],
                             'image': 'foo.image', 'volumeMounts': [
                                {
                                    'mountPath': '/usr/local/airflow/dags',
                                    'name': 'shared-data'}
                            ],
                             'name': 'base',
                             'imagePullPolicy': 'Never'}
                        ]
                    },
                    'metadata': {'name': 'name'}
                }
            },
            'apiVersion': 'batch/v1', 'metadata': {'name': None}
            }


class KubernetesJobRequestTest(unittest.TestCase):
    job_to_load = None
    job_req_factory = SimpleJobRequestFactory()

    def setUp(self):
        configuration.load_test_config()
        self.job_to_load = KubernetesJobBuilder(
            image='foo.image',
            cmds=['try', 'this', 'first']
        )

    def test_job_creation_with_base_values(self):
        base_job_result = self.job_req_factory.create(self.job_to_load)
        self.assertEqual(base_job_result, base_job)
