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

import unittest

from mock import patch

from airflow import settings
from airflow.models.kubernetes import KubeResourceVersion, KubeWorkerIdentifier


class TestKubeResourceVersion(unittest.TestCase):

    def test_checkpoint_resource_version(self):
        session = settings.Session()
        KubeResourceVersion.checkpoint_resource_version('7', session)
        self.assertEqual(KubeResourceVersion.get_current_resource_version(session), '7')

    def test_reset_resource_version(self):
        session = settings.Session()
        version = KubeResourceVersion.reset_resource_version(session)
        self.assertEqual(version, '0')
        self.assertEqual(KubeResourceVersion.get_current_resource_version(session), '0')


class TestKubeWorkerIdentifier(unittest.TestCase):

    @patch('airflow.models.kubernetes.uuid.uuid4')
    def test_get_or_create_not_exist(self, mock_uuid):
        session = settings.Session()
        session.query(KubeWorkerIdentifier).update({
            KubeWorkerIdentifier.worker_uuid: ''
        })
        mock_uuid.return_value = 'abcde'
        worker_uuid = KubeWorkerIdentifier.get_or_create_current_kube_worker_uuid(session)
        self.assertEqual(worker_uuid, 'abcde')

    def test_get_or_create_exist(self):
        session = settings.Session()
        KubeWorkerIdentifier.checkpoint_kube_worker_uuid('fghij', session)
        worker_uuid = KubeWorkerIdentifier.get_or_create_current_kube_worker_uuid(session)
        self.assertEqual(worker_uuid, 'fghij')
