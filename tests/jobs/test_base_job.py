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
#

import unittest

from airflow import configuration
from airflow.jobs import BaseJob
from airflow.utils.state import State

configuration.load_test_config()


class BaseJobTest(unittest.TestCase):
    class TestJob(BaseJob):
        __mapper_args__ = {
            'polymorphic_identity': 'TestJob'
        }

        def __init__(self, cb):
            self.cb = cb
            super().__init__()

        def _execute(self):
            return self.cb()

    def test_state_success(self):
        job = self.TestJob(lambda: True)
        job.run()

        self.assertEqual(job.state, State.SUCCESS)
        self.assertIsNotNone(job.end_date)

    def test_state_sysexit(self):
        import sys
        job = self.TestJob(lambda: sys.exit(0))
        job.run()

        self.assertEqual(job.state, State.SUCCESS)
        self.assertIsNotNone(job.end_date)

    def test_state_failed(self):
        def abort():
            raise RuntimeError("fail")

        job = self.TestJob(abort)
        with self.assertRaises(RuntimeError):
            job.run()

        self.assertEqual(job.state, State.FAILED)
        self.assertIsNotNone(job.end_date)
