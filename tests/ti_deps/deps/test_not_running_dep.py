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
from datetime import datetime
from mock import Mock

from airflow.ti_deps.deps.not_running_dep import NotRunningDep
from airflow.utils.state import State


class NotRunningDepTest(unittest.TestCase):

    def test_ti_running(self):
        """
        Running task instances should fail this dep
        """
        ti = Mock(state=State.RUNNING, start_date=datetime(2016, 1, 1))
        self.assertFalse(NotRunningDep().is_met(ti=ti))

    def test_ti_not_running(self):
        """
        Non-running task instances should pass this dep
        """
        ti = Mock(state=State.NONE, start_date=datetime(2016, 1, 1))
        self.assertTrue(NotRunningDep().is_met(ti=ti))
