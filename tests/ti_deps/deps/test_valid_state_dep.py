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
from unittest.mock import Mock
from datetime import datetime

from airflow import AirflowException
from airflow.ti_deps.deps.valid_state_dep import ValidStateDep
from airflow.utils.state import State


class TestValidStateDep(unittest.TestCase):

    def test_valid_state(self):
        """
        Valid state should pass this dep
        """
        ti = Mock(state=State.QUEUED, end_date=datetime(2016, 1, 1))
        self.assertTrue(ValidStateDep({State.QUEUED}).is_met(ti=ti))

    def test_invalid_state(self):
        """
        Invalid state should fail this dep
        """
        ti = Mock(state=State.SUCCESS, end_date=datetime(2016, 1, 1))
        self.assertFalse(ValidStateDep({State.FAILED}).is_met(ti=ti))

    def test_no_valid_states(self):
        """
        If there are no valid states the dependency should throw
        """
        ti = Mock(state=State.SUCCESS, end_date=datetime(2016, 1, 1))
        with self.assertRaises(AirflowException):
            ValidStateDep({}).is_met(ti=ti)
