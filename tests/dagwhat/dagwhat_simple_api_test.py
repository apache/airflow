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

import pytest

from airflow.dagwhat import *
from tests.dagwhat.dagwhat_test_example_dags_utils import basic_dag


class DagwhatSimpleApiTests(unittest.TestCase):

    def test_api_base_case(self):
        """Test only that the API works as expected."""
        thedag = basic_dag()

        assert_that(
            given(thedag) \
                .when(task('task_1'), succeeds()) \
                .then(task('task_2'), may_run()))

    def test_necessary_but_not_sufficient(self):
        """Test only that the API works as expected."""
        self.skipTest("This method / functionality is not yet implemented.")
        thedag = basic_dag()

        # TODO(pabloem): Refine the expected exception
        with self.assertRaises(Exception):
            assert_that(
                given(thedag) \
                    .when(task('task_1'), succeeds()) \
                    # task_1 succeeding is necessary but not sufficient to
                    # ensure that task_3 will run.
                    .then(task('task_3'), runs()))

    def test_api_with_multiple_conditions(self):
        self.skipTest("This method / functionality is not yet implemented.")
        thedag = basic_dag()

        assert_that(
            given(thedag)\
                .when(task('task_1'), succeeds())\
                .and_(task('task_2'), succeeds())\
                .then(task('task_3'), runs()))

    def test_checks_must_run(self):
        thedag = basic_dag()
        def raise_full_untested(the_dag):
            given(the_dag) \
                .when(task('task_1'), succeeds()) \
                .then(task('task_2'), runs())

        def raise_partial_untested(the_dag):
            given(the_dag) \
                .when(task('task_1'), succeeds())

        def raise_empty_check_untested(the_dag):
            given(the_dag)

        with self.assertRaises(AssertionError):
            raise_full_untested(thedag)

        with self.assertRaises(AssertionError):
            raise_partial_untested(thedag)

        with self.assertRaises(AssertionError):
            raise_empty_check_untested(thedag)
