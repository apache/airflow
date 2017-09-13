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
import warnings

from airflow.operators.bash_operator import BashOperator
from tests.test_utils.reset_warning_registry import reset_warning_registry


class TestLoggingMixin(unittest.TestCase):
    def setUp(self):
        warnings.filterwarnings(
            action='always'
        )

    def test_log(self):
        op = BashOperator(
            task_id='task-1',
            bash_command='exit 0'
        )
        with reset_warning_registry():
            with warnings.catch_warnings(record=True) as w:
                # Set to always, because the warning may have been thrown before
                # Trigger the warning
                op.logger.info('Some arbitrary line')

                self.assertEqual(len(w), 1)

                warning = w[0]
                self.assertTrue(issubclass(warning.category, DeprecationWarning))
                self.assertEqual(
                    'Initializing logger for airflow.operators.bash_operator.BashOperator'
                    ' using logger(), which will be replaced by .log in Airflow 2.0',
                    str(warning.message)
                )

    def tearDown(self):
        warnings.resetwarnings()
