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
"""
Example DAG for demonstrating behavior of Datasets feature.

Notes on usage:

Turn on all the dags.

DAG example_dataset_dag1 should run because it's on a schedule.

After example_dataset_dag1 runs, example_dataset_dag3_req_dag1 should be triggered immediately
because its only dataset dependency is managed by example_dataset_dag1.

No other dags should be triggered.  Note that even though example_dataset_dag4_req_dag1_dag2 depends on
the dataset in example_dataset_dag1, it will not be triggered until example_dataset_dag2 runs
(and example_dataset_dag2 is left with no schedule so that we can trigger it manually).

Next, trigger example_dataset_dag2.  After example_dataset_dag2 finishes,
example_dataset_dag4_req_dag1_dag2 should run.

Dags example_dataset_dag5_req_dag1_D and example_dataset_dag6_req_DD should not run because they depend on
datasets that never get updated.

DAG example_dataset_dag7 should skip its only task and never trigger example_dataset_dag8_req_dag7

DAG example_dataset_dag9 should fail its only task and never trigger example_dataset_dag10_req_dag9

"""
from datetime import datetime

from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import DAG, Dataset
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# [START dataset_def]
dag1_dataset = Dataset('s3://dag1/output_1.txt', extra={'hi': 'bye'})
# [END dataset_def]
dag2_dataset = Dataset('s3://dag2/output_1.txt', extra={'hi': 'bye'})
dag7_dataset = Dataset('s3://dag7/output_1.txt', extra={'hi': 'bye'})
dag9_dataset = Dataset('s3://dag9/output_1.txt', extra={'hi': 'bye'})

dag1 = DAG(
    dag_id='example_dataset_dag1',
    catchup=False,
    start_date=datetime(2020, 1, 1),
    schedule_interval='@daily',
    tags=['upstream'],
)

# [START task_outlet]
BashOperator(outlets=[dag1_dataset], task_id='upstream_task_1', bash_command="sleep 5", dag=dag1)
# [END task_outlet]

with DAG(
    dag_id='example_dataset_dag2',
    catchup=False,
    start_date=datetime(2020, 1, 1),
    schedule_interval=None,
    tags=['upstream'],
) as dag2:
    BashOperator(
        outlets=[dag2_dataset],
        task_id='upstream_task_2',
        bash_command="sleep 5",
    )

# [START dag_dep]
dag3 = DAG(
    dag_id='example_dataset_dag3_req_dag1',
    catchup=False,
    start_date=datetime(2020, 1, 1),
    schedule_on=[dag1_dataset],
    tags=['downstream'],
)
# [END dag_dep]

BashOperator(
    outlets=[Dataset('s3://downstream_1_task/dataset_other.txt')],
    task_id='downstream_1',
    bash_command="sleep 5",
    dag=dag3,
)

with DAG(
    dag_id='example_dataset_dag4_req_dag1_dag2',
    catchup=False,
    start_date=datetime(2020, 1, 1),
    schedule_on=[dag1_dataset, dag2_dataset],
    tags=['downstream'],
) as dag4:
    BashOperator(
        outlets=[Dataset('s3://downstream_2_task/dataset_other_unknown.txt')],
        task_id='downstream_2',
        bash_command="sleep 5",
    )

with DAG(
    dag_id='example_dataset_dag5_req_dag1_D',
    catchup=False,
    start_date=datetime(2020, 1, 1),
    schedule_on=[
        dag1_dataset,
        Dataset('s3://this-dataset-doesnt-get-triggered'),
    ],
    tags=['downstream'],
) as dag5:
    BashOperator(
        outlets=[Dataset('s3://downstream_2_task/dataset_other_unknown.txt')],
        task_id='downstream_3',
        bash_command="sleep 5",
    )

with DAG(
    dag_id='example_dataset_dag6_req_DD',
    catchup=False,
    start_date=datetime(2020, 1, 1),
    schedule_on=[
        Dataset('s3://unrelated/dataset3.txt'),
        Dataset('s3://unrelated/dataset_other_unknown.txt'),
    ],
    tags=['unrelated'],
) as dag6:
    BashOperator(
        task_id='unrelated_task',
        outlets=[Dataset('s3://unrelated_task/dataset_other_unknown.txt')],
        bash_command="sleep 5",
    )


def raise_skip_exc():
    raise AirflowSkipException


dag7 = DAG(
    dag_id='example_dataset_dag7',
    catchup=False,
    start_date=datetime(2020, 1, 1),
    schedule_interval='@daily',
    tags=['upstream-skipping'],
)
PythonOperator(
    task_id='skip_task',
    outlets=[dag7_dataset],
    python_callable=raise_skip_exc,
    dag=dag7,
)

with DAG(
    dag_id='example_dataset_dag8_req_dag7',
    catchup=False,
    start_date=datetime(2020, 1, 1),
    schedule_on=[dag7_dataset],
    tags=['downstream-skipped'],
) as dag8:
    BashOperator(
        task_id='dag8_task',
        bash_command="sleep 5",
    )


def raise_assertionerror():
    raise AirflowFailException


dag9 = DAG(
    dag_id='example_dataset_dag9',
    catchup=False,
    start_date=datetime(2020, 1, 1),
    schedule_interval='@daily',
    tags=['upstream-skipping'],
)
PythonOperator(
    task_id='fail_task',
    outlets=[dag9_dataset],
    python_callable=raise_assertionerror,
    dag=dag9,
)

with DAG(
    dag_id='example_dataset_dag10_req_dag9',
    catchup=False,
    start_date=datetime(2020, 1, 1),
    schedule_on=[dag9_dataset],
    tags=['downstream-failed'],
) as dag10:
    BashOperator(
        task_id='dag10_task',
        bash_command="sleep 5",
    )
