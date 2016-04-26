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

from airflow.ti_deps.deps.pool_has_space_dep import PoolHasSpaceDep
from fake_models import FakeTI


class PoolHasSpaceDepTest(unittest.TestCase):

    def test_pool_full(self):
        """
        Full pools should fail this dep
        """
        ti = FakeTI(pool="fake_pool", pool_filled=True)

        self.assertFalse(PoolHasSpaceDep().is_met(ti=ti, dep_context=None))

    def test_not_skipped(self):
        """
        Pools with room should pass this dep
        """
        ti = FakeTI(pool="fake_pool", pool_filled=False)

        self.assertTrue(PoolHasSpaceDep().is_met(ti=ti, dep_context=None))
