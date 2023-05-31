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
"""Example DAG demonstrating the usage of setup and teardown tasks."""
from __future__ import annotations

import pendulum
import pytest

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# with DAG(
#     dag_id="example_setup_teardown",
#     start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
#     catchup=False,
#     tags=["example"],
# ) as dag:
#     BashOperator.as_setup(task_id="root_setup", bash_command="echo 'Hello from root_setup'")
#     normal = BashOperator(task_id="normal", bash_command="echo 'I am just a normal task'")
#     BashOperator.as_teardown(task_id="root_teardown", bash_command="echo 'Goodbye from root_teardown'")
#
#     with TaskGroup("section_1") as section_1:
#         BashOperator.as_setup(task_id="taskgroup_setup", bash_command="echo 'Hello from taskgroup_setup'")
#         BashOperator(task_id="normal", bash_command="echo 'I am just a normal task'")
#         BashOperator.as_setup(
#             task_id="taskgroup_teardown", bash_command="echo 'Hello from taskgroup_teardown'"
#         )
#
#     normal >> section_1
#
#
# with DAG(dag_id="hello1", start_date=pendulum.now()):
#     s1 = BashOperator.as_setup(task_id="s1", bash_command="echo 'Hello from root_setup'")
#     t1 = BashOperator.as_teardown(task_id="t1", bash_command="echo 'Hello from root_setup'")
#     with s1 >> t1:
#         w2 = BashOperator(task_id="w2", bash_command="echo 'Hello from root_setup'")
#
# with pytest.raises(RuntimeError, match="May only set one level of deps in context mgr"):
#     with DAG(dag_id="hello2", start_date=pendulum.now()):
#         w1 = BashOperator(task_id="w1", bash_command="echo 'Hello from root_setup'")
#         s1 = BashOperator.as_setup(task_id="s1", bash_command="echo 'Hello from root_setup'")
#         t1 = BashOperator.as_teardown(task_id="t1", bash_command="echo 'Hello from root_setup'")
#         with w1 >> s1 >> t1:
#             w2 = BashOperator(task_id="w2", bash_command="echo 'Hello from root_setup'")
#
# with DAG(dag_id="hello3", start_date=pendulum.now()):
#     w1 = BashOperator(task_id="w1", bash_command="echo 'Hello from root_setup'")
#     s1 = BashOperator.as_setup(task_id="s1", bash_command="echo 'Hello from root_setup'")
#     s2 = BashOperator.as_setup(task_id="s2", bash_command="echo 'Hello from root_setup'")
#     t1 = BashOperator.as_teardown(task_id="t1", bash_command="echo 'Hello from root_setup'")
#     with [s1, s2] >> t1:
#         w2 = BashOperator(task_id="w2", bash_command="echo 'Hello from root_setup'")
#
# with pytest.raises(RuntimeError, match="May only set one level of deps in context mgr"):
#     with DAG(dag_id="hello4", start_date=pendulum.now()):
#         w1 = BashOperator(task_id="w1", bash_command="echo 'Hello from root_setup'")
#         s1 = BashOperator.as_setup(task_id="s1", bash_command="echo 'Hello from root_setup'")
#         s2 = BashOperator.as_setup(task_id="s2", bash_command="echo 'Hello from root_setup'")
#         t1 = BashOperator.as_teardown(task_id="t1", bash_command="echo 'Hello from root_setup'")
#         with [w1 >> s1, s2] >> t1:
#             w2 = BashOperator(task_id="w2", bash_command="echo 'Hello from root_setup'")
#
# with pytest.raises(RuntimeError, match="May only set one level of deps in context mgr"):
#     with DAG(dag_id="hello5", start_date=pendulum.now()):
#         w1 = BashOperator(task_id="w1", bash_command="echo 'Hello from root_setup'")
#         s1 = BashOperator.as_setup(task_id="s1", bash_command="echo 'Hello from root_setup'")
#         s2 = BashOperator.as_setup(task_id="s2", bash_command="echo 'Hello from root_setup'")
#         t1 = BashOperator.as_teardown(task_id="t1", bash_command="echo 'Hello from root_setup'")
#         with [s1, w1 >> s2] >> t1:
#             w2 = BashOperator(task_id="w2", bash_command="echo 'Hello from root_setup'")
#
# with pytest.raises(RuntimeError, match="May only set one level of deps in context mgr"):
#     with DAG(dag_id="hello6", start_date=pendulum.now()):
#         w1 = BashOperator(task_id="w1", bash_command="echo 'Hello from root_setup'")
#         s1 = BashOperator.as_setup(task_id="s1", bash_command="echo 'Hello from root_setup'")
#         s2 = BashOperator.as_setup(task_id="s2", bash_command="echo 'Hello from root_setup'")
#         t1 = BashOperator.as_teardown(task_id="t1", bash_command="echo 'Hello from root_setup'")
#         with [s1, w1 >> s2] >> t1:
#             w2 = BashOperator(task_id="w2", bash_command="echo 'Hello from root_setup'")
#
# with DAG(dag_id="hello7", start_date=pendulum.now()):
#     w1 = BashOperator(task_id="w1", bash_command="echo 'Hello from root_setup'")
#     s1 = BashOperator.as_setup(task_id="s1", bash_command="echo 'Hello from root_setup'")
#     s2 = BashOperator.as_setup(task_id="s2", bash_command="echo 'Hello from root_setup'")
#     t1 = BashOperator.as_teardown(task_id="t1", bash_command="echo 'Hello from root_setup'")
#     with [s1, s2] >> t1:
#         s3 = BashOperator.as_setup(task_id="s3", bash_command="echo 'Hello from root_setup'")
#         t3 = BashOperator.as_teardown(task_id="t3", bash_command="echo 'Hello from root_setup'")
#         with s3 >> t3:
#             w3 = BashOperator(task_id="w3", bash_command="echo 'Hello from root_setup'")
#
# with pytest.raises(RuntimeError, match="May only set one level of deps in context mgr"):
#     with DAG(dag_id="hello8", start_date=pendulum.now()):
#         w1 = BashOperator(task_id="w1", bash_command="echo 'Hello from root_setup'")
#         s1 = BashOperator.as_setup(task_id="s1", bash_command="echo 'Hello from root_setup'")
#         s2 = BashOperator.as_setup(task_id="s2", bash_command="echo 'Hello from root_setup'")
#         t1 = BashOperator.as_teardown(task_id="t1", bash_command="echo 'Hello from root_setup'")
#         with [s1, s2] >> t1:
#             s3 = BashOperator.as_setup(task_id="s3", bash_command="echo 'Hello from root_setup'")
#             t3 = BashOperator.as_teardown(task_id="t3", bash_command="echo 'Hello from root_setup'")
#             with w1 >> s3 >> t3:
#                 w3 = BashOperator(task_id="w3", bash_command="echo 'Hello from root_setup'")
#
# ideally should not fail, but not easy to prevent
with pytest.raises(RuntimeError, match="May only set one level of deps in context mgr"):
    with DAG(dag_id="hello9", start_date=pendulum.now()):
        w1 = BashOperator(task_id="w1", bash_command="echo 'Hello from root_setup'")

        s1 = BashOperator.as_setup(task_id="s1", bash_command="echo 'Hello from root_setup'")
        s2 = BashOperator.as_setup(task_id="s2", bash_command="echo 'Hello from root_setup'")
        t1 = BashOperator.as_teardown(task_id="t1", bash_command="echo 'Hello from root_setup'")

        w1 >> s1  # this increases the deps depth recorded on s1
        assert s1._deps_depth == 1

        with [s1, s2] >> t1:
            s3 = BashOperator.as_setup(task_id="s3", bash_command="echo 'Hello from root_setup'")
            t3 = BashOperator.as_teardown(task_id="t3", bash_command="echo 'Hello from root_setup'")
            with w1 >> s3 >> t3:
                w3 = BashOperator(task_id="w3", bash_command="echo 'Hello from root_setup'")

# remedy is to not set additional deps on setups / teardowns until after they are used in context mgr
with DAG(dag_id="hello9", start_date=pendulum.now()):
    w1 = BashOperator(task_id="w1", bash_command="echo 'Hello from root_setup'")

    s1 = BashOperator.as_setup(task_id="s1", bash_command="echo 'Hello from root_setup'")
    s2 = BashOperator.as_setup(task_id="s2", bash_command="echo 'Hello from root_setup'")
    t1 = BashOperator.as_teardown(task_id="t1", bash_command="echo 'Hello from root_setup'")

    with [s1, s2] >> t1:
        s3 = BashOperator.as_setup(task_id="s3", bash_command="echo 'Hello from root_setup'")
        t3 = BashOperator.as_teardown(task_id="t3", bash_command="echo 'Hello from root_setup'")
        with s3 >> t3:
            w3 = BashOperator(task_id="w3", bash_command="echo 'Hello from root_setup'")

    w1 >> s1  # this increases the deps depth recorded on s1
    assert s1._deps_depth == 4

