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

import kubernetes.client.models as k8s

from airflow.kubernetes import pod_enricher


class TestPodEnricher(unittest.TestCase):

    @mock.patch('airflow.kubernetes.pod_enricher.uuid.uuid4', return_value="123456789")
    def test_should_apply_mutation(self, mock_uuid4):
        pod = k8s.V1Pod(
            api_version='v1',
            kind='Pod',
            metadata=k8s.V1ObjectMeta(labels={'run': 'example-yaml-2'}, name='example-yaml-2'),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name='example-yaml-2',
                        args=['sh', '-c', 'echo 123; sleep 10'],
                        image='busybox',
                    )
                ],
                restart_policy='Never'
            )
        )

        result = pod_enricher.refine_pod(pod)

        expected_result = k8s.V1Pod(
            api_version='v1',
            kind='Pod',
            metadata=k8s.V1ObjectMeta(
                labels={
                    'airflow-version': pod_enricher._AIRFLOW_VERSION,
                    'run': 'example-yaml-2'
                },
                name='example-yaml-2-12345678',
                namespace='default'
            ),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name='example-yaml-2',
                        args=['sh', '-c', 'echo 123; sleep 10'],
                        image='busybox',
                    )
                ],
                restart_policy='Never'
            )
        )
        self.assertEqual(expected_result, result)

    @mock.patch('airflow.kubernetes.pod_enricher.uuid.uuid4', return_value="123456789")
    def test_should_add_sidecar(self, mock_uuid4):
        pod = k8s.V1Pod(
            api_version='v1',
            kind='Pod',
            metadata=k8s.V1ObjectMeta(labels={'run': 'example-yaml-2'}, name='example-yaml-2'),
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name='example-yaml-2',
                        args=['sh', '-c', 'echo 123; sleep 10'],
                        image='busybox',
                    )
                ],
                restart_policy='Never'
            )
        )

        result_pod = pod_enricher.refine_pod(pod, extract_xcom=True)

        self.assertEqual(
            pod_enricher.XcomSidecarConfig.CONTAINER, result_pod.spec.containers[1]
        )
        self.assertEqual(
            pod_enricher.XcomSidecarConfig.VOLUME, result_pod.spec.volumes[0]
        )
        self.assertEqual(
            pod_enricher.XcomSidecarConfig.VOLUME_MOUNT, result_pod.spec.containers[0].volume_mounts[0]
        )
