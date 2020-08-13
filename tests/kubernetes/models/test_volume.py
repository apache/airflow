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

from kubernetes.client import models as k8s

from airflow.kubernetes.volume import Volume


class TestVolume(unittest.TestCase):
    def test_to_k8s_object(self):
        volume_config = {
            'persistentVolumeClaim':
                {
                    'claimName': 'test-volume'
                }
        }
        volume = Volume(name='test-volume', configs=volume_config)
        expected_volume = k8s.V1Volume(
            name="test-volume",
            persistent_volume_claim={
                "claimName": "test-volume"
            }
        )
        result = volume.to_k8s_client_obj()
        self.assertEqual(result, expected_volume)
