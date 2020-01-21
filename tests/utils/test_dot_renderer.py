#!/usr/bin/env python
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
import datetime
import unittest

import mock

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils import dot_renderer

START_DATE = datetime.datetime.now()


class TestDotRenderer(unittest.TestCase):
    def test_should_render_dag(self):

        dag = DAG(dag_id="DAG_ID")
        task_1 = BashOperator(dag=dag, start_date=START_DATE, task_id="first", bash_command="echo 1")
        task_2 = BashOperator(dag=dag, start_date=START_DATE, task_id="second", bash_command="echo 1")
        task_3 = PythonOperator(
            dag=dag, start_date=START_DATE, task_id="third", python_callable=mock.MagicMock()
        )
        task_1 >> task_2
        task_1 >> task_3

        dot = dot_renderer.render_dag(dag)
        source = dot.source
        # Should render DAG title
        self.assertIn("label=DAG_ID", source)
        self.assertIn("first", source)
        self.assertIn("second", source)
        self.assertIn("third", source)
        self.assertIn("first -> second", source)
        self.assertIn("first -> third", source)
        self.assertIn('fillcolor="#f0ede4"', source)
        self.assertIn('fillcolor="#f0ede4"', source)
