# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

from airflow.ti_deps.deps.not_queued_dep import NotQueuedDep
from airflow.utils.state import State
from fake_models import FakeTI


class NotQueuedDepTest(unittest.TestCase):

    def test_ti_queued(self):
        """
        Queued task instances should fail this dep
        """
        ti = FakeTI(state=State.QUEUED)

        self.assertFalse(NotQueuedDep().is_met(ti=ti, dep_context=None))

    def test_ti_not_queued_(self):
        """
        Non-queued task instances should pass this dep
        """
        ti = FakeTI(state=State.SUCCESS)

        self.assertTrue(NotQueuedDep().is_met(ti=ti, dep_context=None))
