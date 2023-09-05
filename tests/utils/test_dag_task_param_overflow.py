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
from __future__ import annotations


class TestDagTaskParameterOverflow:
    # Pending to create tests
    """def test_cycle_empty(self):
        # test empty
        dag = DAG("dag", start_date=DEFAULT_DATE, default_args={"owner": "owner1"})

        assert not check_cycle(dag)

    def test_cycle_single_task(self):
        # test single task
        dag = DAG("dag", start_date=DEFAULT_DATE, default_args={"owner": "owner1"})

        with dag:
            EmptyOperator(task_id="A")

        assert not check_cycle(dag)

    def test_semi_complex(self):
        dag = DAG("dag", start_date=DEFAULT_DATE, default_args={"owner": "owner1"})

        # A -> B -> C
        #      B -> D
        # E -> F
        with dag:
            create_cluster = EmptyOperator(task_id="c")
            pod_task = EmptyOperator(task_id="p")
            pod_task_xcom = EmptyOperator(task_id="x")
            delete_cluster = EmptyOperator(task_id="d")
            pod_task_xcom_result = EmptyOperator(task_id="r")
            create_cluster >> pod_task >> delete_cluster
            create_cluster >> pod_task_xcom >> delete_cluster
            pod_task_xcom >> pod_task_xcom_result

    def test_cycle_no_cycle(self):
        # test no cycle
        dag = DAG("dag", start_date=DEFAULT_DATE, default_args={"owner": "owner1"})

        # A -> B -> C
        #      B -> D
        # E -> F
        with dag:
            op1 = EmptyOperator(task_id="A")
            op2 = EmptyOperator(task_id="B")
            op3 = EmptyOperator(task_id="C")
            op4 = EmptyOperator(task_id="D")
            op5 = EmptyOperator(task_id="E")
            op6 = EmptyOperator(task_id="F")
            op1.set_downstream(op2)
            op2.set_downstream(op3)
            op2.set_downstream(op4)
            op5.set_downstream(op6)

        assert not check_cycle(dag)

    def test_cycle_loop(self):
        # test self loop
        dag = DAG("dag", start_date=DEFAULT_DATE, default_args={"owner": "owner1"})

        # A -> A
        with dag:
            op1 = EmptyOperator(task_id="A")
            op1.set_downstream(op1)

        with pytest.raises(AirflowDagCycleException):
            assert not check_cycle(dag)
    """
