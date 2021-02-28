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
# pylint: disable=no-value-for-parameter

import unittest
from unittest.mock import Mock

from airflow.models import TaskInstance
from airflow.ti_deps.deps.dag_ti_slots_available_dep import DagTISlotsAvailableDep


class TestDagTISlotsAvailableDep(unittest.TestCase):
    def test_concurrency_reached(self):
        """
        Test concurrency reached should fail dep
        """
        dag = Mock(concurrency=1, get_concurrency_reached=Mock(return_value=True))
        task = Mock(dag=dag, pool_slots=1)
        ti = TaskInstance(task, execution_date=None)

        assert not DagTISlotsAvailableDep().is_met(ti=ti)

    def test_all_conditions_met(self):
        """
        Test all conditions met should pass dep
        """
        dag = Mock(concurrency=1, get_concurrency_reached=Mock(return_value=False))
        task = Mock(dag=dag, pool_slots=1)
        ti = TaskInstance(task, execution_date=None)

        assert DagTISlotsAvailableDep().is_met(ti=ti)
