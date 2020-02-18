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

from mock import Mock

from airflow.jobs import BackfillJob
from airflow.models import DagRun
from airflow.ti_deps.deps.dagrun_id_dep import DagrunIdDep


class TestDagrunRunningDep(unittest.TestCase):
    def test_dagrun_id_is_backfill(self):
        """
        Task instances whose dagrun ID is a backfill dagrun ID should fail this dep.
        """
        dagrun = DagRun()
        dagrun.run_id = BackfillJob.ID_PREFIX + '_something'
        ti = Mock(get_dagrun=Mock(return_value=dagrun))
        self.assertFalse(DagrunIdDep().is_met(ti=ti))

    def test_dagrun_id_is_not_backfill(self):
        """
        Task instances whose dagrun ID is not a backfill dagrun ID should pass this dep.
        """
        dagrun = DagRun()
        dagrun.run_id = 'notbackfill_something'
        ti = Mock(get_dagrun=Mock(return_value=dagrun))
        self.assertTrue(DagrunIdDep().is_met(ti=ti))

        dagrun = DagRun()
        dagrun.run_id = None
        ti = Mock(get_dagrun=Mock(return_value=dagrun))
        self.assertTrue(DagrunIdDep().is_met(ti=ti))
