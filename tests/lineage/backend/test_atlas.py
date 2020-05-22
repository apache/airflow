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
from configparser import DuplicateSectionError
from unittest import mock

from airflow import AirflowException
from airflow.configuration import AirflowConfigException, conf
from airflow.lineage.backend import atlas
from airflow.lineage.backend.atlas import AtlasBackend
from airflow.lineage.datasets import File
from airflow.models import DAG, TaskInstance as TI
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2016, 1, 1)


class TestAtlas(unittest.TestCase):
    def setUp(self):
        try:
            conf.add_section("atlas")
        except AirflowConfigException:
            pass
        except DuplicateSectionError:
            pass
        conf.set("atlas", "username", "none")
        conf.set("atlas", "password", "none")
        conf.set("atlas", "host", "none")
        conf.set("atlas", "port", "0")
        self.atlas_backend = AtlasBackend()
        self.dag = DAG(dag_id='test_prepare_lineage', start_date=DEFAULT_DATE)

        file1 = File("/tmp/does_not_exist_1")
        file2 = File("/tmp/does_not_exist_2")
        self.inlets_d = [file1, ]
        self.outlets_d = [file2, ]

        self.operator1 = DummyOperator(task_id='leave1',
                                       inlets={"datasets": self.inlets_d},
                                       outlets={"datasets": self.outlets_d},
                                       dag=self.dag)

        self.ctx = {"ti": TI(task=self.operator1, execution_date=DEFAULT_DATE)}

    @mock.patch("airflow.lineage.backend.atlas.Atlas")
    def test_lineage_send(self, atlas_mock):
        atlas._create_if_not_exists = False
        typedefs = mock.MagicMock()
        entity_post = mock.MagicMock()
        atlas_mock.return_value = mock.Mock(typedefs=typedefs, entity_post=entity_post)

        self.atlas_backend.send_lineage(operator=self.operator1, inlets=self.inlets_d, outlets=self.outlets_d,
                                        context=self.ctx)

        self.assertEqual(typedefs.create.call_count, 2)
        self.assertTrue(entity_post.create.called)
        self.assertEqual(entity_post.create.call_count, 1)

    @mock.patch("airflow.lineage.backend.atlas.Atlas")
    def test_create_inlets_outlets_entities(self, atlas_mock):
        atlas._create_if_not_exists = True
        typedefs = mock.MagicMock()
        entity_post = mock.MagicMock()
        atlas_mock.return_value = mock.Mock(typedefs=typedefs, entity_post=entity_post)

        self.atlas_backend.send_lineage(operator=self.operator1, inlets=self.inlets_d, outlets=self.outlets_d,
                                        context=self.ctx)
        self.assertEqual(typedefs.create.call_count, 2)
        self.assertTrue(entity_post.create.called)
        self.assertEqual(entity_post.create.call_count, 3)

    @mock.patch("airflow.lineage.backend.atlas.Atlas")
    def test_dag_fail_if_lineage_fail(self, atlas_mock):
        atlas._fail_if_lineage_error = True
        typedefs = mock.MagicMock()
        entity_post = mock.Mock(side_effect=AirflowException)
        atlas_mock.return_value = mock.Mock(typedefs=typedefs, entity_post=entity_post)

        with self.assertRaises(AirflowException):
            self.atlas_backend.send_lineage(operator=self.operator1, inlets=self.inlets_d,
                                            outlets=self.outlets_d,
                                            context=self.ctx)
