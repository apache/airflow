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

from airflow.ti_deps.deps.not_skipped_dep import NotSkippedDep
from airflow.utils.state import State


class NotSkippedDepTest(unittest.TestCase):

    def test_skipped(self):
        """
        Skipped task instances should fail this dep
        """
        ti = Mock(state=State.SKIPPED)
        self.assertFalse(NotSkippedDep().is_met(ti=ti))

    def test_not_skipped(self):
        """
        Non-skipped task instances should pass this dep
        """
        ti = Mock(state=State.RUNNING)
        self.assertTrue(NotSkippedDep().is_met(ti=ti))
