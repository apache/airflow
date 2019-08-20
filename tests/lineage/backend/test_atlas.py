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
from configparser import DuplicateSectionError

from airflow.configuration import conf, AirflowConfigException
from airflow.lineage.backend.atlas import AtlasBackend
from airflow.lineage.datasets import File
from airflow.models import DAG, TaskInstance as TI
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone
from tests.compat import mock

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

        self.atlas = AtlasBackend()

    @mock.patch("airflow.lineage.backend.atlas.Atlas")
    def test_lineage_send(self, atlas_mock):
        td = mock.MagicMock()
        en = mock.MagicMock()
        atlas_mock.return_value = mock.Mock(typedefs=td, entity_post=en)

        dag = DAG(
            dag_id='test_prepare_lineage',
            start_date=DEFAULT_DATE
        )

        f1 = File("/tmp/does_not_exist_1")
        f2 = File("/tmp/does_not_exist_2")

        inlets_d = [f1, ]
        outlets_d = [f2, ]

        with dag:
            op1 = DummyOperator(task_id='leave1',
                                inlets={"datasets": inlets_d},
                                outlets={"datasets": outlets_d})

        ctx = {"ti": TI(task=op1, execution_date=DEFAULT_DATE)}

        self.atlas.send_lineage(operator=op1, inlets=inlets_d,
                                outlets=outlets_d, context=ctx)

        self.assertEqual(td.create.call_count, 1)
        self.assertTrue(en.create.called)
        self.assertEqual(len(en.mock_calls), 3)

        # test can be broader
