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

from airflow import DAG
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import airflow.utils.dates


class BaseOperatorTest(unittest.TestCase):
    def setUp(self):
        self.dag = DAG(
            'dag',
            default_args={
                'email_on_failure': False,
                'email': 'example@localhost',
                'x': 'x',
            },
            start_date=airflow.utils.dates.days_ago(2),
        )

    class Op(BaseOperator):
        @apply_defaults
        def __init__(self, x=None, **kwargs):
            super().__init__(**kwargs)
            self._x = x

    def test_dag_setter_apply_defaults(self):
        """
        Test that apply_defaults operates correctly when using the `op.dag` setter.
        """

        op = self.Op(
            task_id='op',
            email_on_failure=True,  # happen to be the same as the default value in __init__ signature
        )

        op.dag = self.dag

        self.assertTrue(op.email_on_failure)
        self.assertEqual(op.email, 'example@localhost')
        # Test that we re-call __init__ with the new default args
        self.assertEqual(op._x, 'x')
        self.assertEqual(op.start_date, self.dag.start_date)

    def test_dag_bitshift_apply_defaults(self):
        """
        Test that apply_defaults operates correctly when using the `op.dag` setter.
        """

        op = self.Op(
            task_id='op',
            email_on_failure=True,  # happen to be the same as the default value in __init__ signature
        )

        self.dag >> op

        self.assertTrue(op.email_on_failure)
        self.assertEqual(op.email, 'example@localhost')
        # Test that we re-call __init__ with the new default args
        self.assertEqual(op._x, 'x')

    def test_dag_setter_no_apply_decorator(self):
        """
        Test that default_args not applied when the operator is not annotated with `@apply_defaults`.
        """
        class SubclassWithoutApplyDefaults(BaseOperator):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)

        op = SubclassWithoutApplyDefaults(
            task_id='op',
            email_on_failure=True,  # happen to be the same as the default value in __init__ signature
        )

        op.dag = self.dag

        self.assertEqual(op.start_date, self.dag.start_date)
        self.assertTrue(op.email_on_failure)
        # Since we don't' use the @apply_defaults decorator on this ctor default args aren't applied.
        self.assertIsNone(op.email)
