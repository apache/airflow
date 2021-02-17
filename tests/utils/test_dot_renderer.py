#!/usr/bin/env python
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
from unittest import mock

from airflow.models import TaskInstance
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import dot_renderer
from airflow.utils.state import State
from airflow.utils.task_group import TaskGroup

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
        assert "label=DAG_ID" in source
        assert "first" in source
        assert "second" in source
        assert "third" in source
        assert "first -> second" in source
        assert "first -> third" in source
        assert 'fillcolor="#f0ede4"' in source
        assert 'fillcolor="#f0ede4"' in source

    def test_should_render_dag_with_task_instances(self):
        dag = DAG(dag_id="DAG_ID")
        task_1 = BashOperator(dag=dag, start_date=START_DATE, task_id="first", bash_command="echo 1")
        task_2 = BashOperator(dag=dag, start_date=START_DATE, task_id="second", bash_command="echo 1")
        task_3 = PythonOperator(
            dag=dag, start_date=START_DATE, task_id="third", python_callable=mock.MagicMock()
        )
        task_1 >> task_2
        task_1 >> task_3
        tis = [
            TaskInstance(task_1, execution_date=START_DATE, state=State.SCHEDULED),
            TaskInstance(task_2, execution_date=START_DATE, state=State.SUCCESS),
            TaskInstance(task_3, execution_date=START_DATE, state=State.RUNNING),
        ]
        dot = dot_renderer.render_dag(dag, tis=tis)
        source = dot.source
        # Should render DAG title
        assert "label=DAG_ID" in source
        assert (
            'first [color=black fillcolor=tan label=first shape=rectangle style="filled,rounded"]' in source
        )
        assert (
            'second [color=white fillcolor=green label=second shape=rectangle style="filled,rounded"]'
            in source
        )
        assert (
            'third [color=black fillcolor=lime label=third shape=rectangle style="filled,rounded"]' in source
        )

    def test_should_render_dag_orientation(self):
        orientation = "TB"
        dag = DAG(dag_id="DAG_ID", orientation=orientation)
        task_1 = BashOperator(dag=dag, start_date=START_DATE, task_id="first", bash_command="echo 1")
        task_2 = BashOperator(dag=dag, start_date=START_DATE, task_id="second", bash_command="echo 1")
        task_3 = PythonOperator(
            dag=dag, start_date=START_DATE, task_id="third", python_callable=mock.MagicMock()
        )
        task_1 >> task_2
        task_1 >> task_3
        tis = [
            TaskInstance(task_1, execution_date=START_DATE, state=State.SCHEDULED),
            TaskInstance(task_2, execution_date=START_DATE, state=State.SUCCESS),
            TaskInstance(task_3, execution_date=START_DATE, state=State.RUNNING),
        ]
        dot = dot_renderer.render_dag(dag, tis=tis)
        source = dot.source
        # Should render DAG title with orientation
        assert "label=DAG_ID" in source
        assert f'label=DAG_ID labelloc=t rankdir={orientation}' in source

        # Change orientation
        orientation = "LR"
        dag = DAG(dag_id="DAG_ID", orientation=orientation)
        dot = dot_renderer.render_dag(dag, tis=tis)
        source = dot.source
        # Should render DAG title with orientation
        assert "label=DAG_ID" in source
        assert f'label=DAG_ID labelloc=t rankdir={orientation}' in source

    def test_render_task_group(self):
        with DAG(dag_id="example_task_group", start_date=START_DATE) as dag:
            start = DummyOperator(task_id="start")

            with TaskGroup("section_1", tooltip="Tasks for section_1") as section_1:
                task_1 = DummyOperator(task_id="task_1")
                task_2 = BashOperator(task_id="task_2", bash_command='echo 1')
                task_3 = DummyOperator(task_id="task_3")

                task_1 >> [task_2, task_3]

            with TaskGroup("section_2", tooltip="Tasks for section_2") as section_2:
                task_1 = DummyOperator(task_id="task_1")

                with TaskGroup("inner_section_2", tooltip="Tasks for inner_section2"):
                    task_2 = BashOperator(task_id="task_2", bash_command='echo 1')
                    task_3 = DummyOperator(task_id="task_3")
                    task_4 = DummyOperator(task_id="task_4")

                    [task_2, task_3] >> task_4

            end = DummyOperator(task_id='end')

            start >> section_1 >> section_2 >> end

        dot = dot_renderer.render_dag(dag)
        source = dot.source

        assert "subgraph cluster_section_1" in source
        assert "section_1.upstream_join_id" in source
        assert """subgraph "cluster_section_2.inner_section_2""" in source
        assert '"section_2.upstream_join_id" -> "section_2.inner_section_2.task_2"' in source
        assert '"section_2.downstream_join_id" -> end' in source
