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

from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException


# Essentially similar to airflow.models.BaseOperator
class DummyClass:
    @apply_defaults
    def __init__(self, test_param, params=None, default_args=None):
        self.test_param = test_param


class DummySubClass(DummyClass):
    @apply_defaults
    def __init__(self, test_sub_param, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.test_sub_param = test_sub_param


class ApplyDefaultTest(unittest.TestCase):

    def test_apply(self):
        dc = DummyClass(test_param=True)
        self.assertTrue(dc.test_param)

        with self.assertRaisesRegex(AirflowException, 'Argument.*test_param.*required'):
            DummySubClass(test_sub_param=True)

    def test_default_args(self):
        default_args = {'test_param': True}
        dc = DummyClass(default_args=default_args)
        self.assertTrue(dc.test_param)

        default_args = {'test_param': True, 'test_sub_param': True}
        dsc = DummySubClass(default_args=default_args)
        self.assertTrue(dc.test_param)
        self.assertTrue(dsc.test_sub_param)

        default_args = {'test_param': True}
        dsc = DummySubClass(default_args=default_args, test_sub_param=True)
        self.assertTrue(dc.test_param)
        self.assertTrue(dsc.test_sub_param)

        with self.assertRaisesRegex(AirflowException,
                                    'Argument.*test_sub_param.*required'):
            DummySubClass(default_args=default_args)

    def test_incorrect_default_args(self):
        default_args = {'test_param': True, 'extra_param': True}
        dc = DummyClass(default_args=default_args)
        self.assertTrue(dc.test_param)

        default_args = {'random_params': True}
        with self.assertRaisesRegex(AirflowException, 'Argument.*test_param.*required'):
            DummyClass(default_args=default_args)
