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
import logging

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.timezone import datetime

logger = logging.getLogger(__name__)


def test_logging_fn(**kwargs):
    logger.info("Log from DAG Logger")
    kwargs["ti"].log.info("Log from TI Logger")
    print("Log from Print statement")


dag = DAG(
    dag_id='test_logging_dag',
    schedule_interval=None,
    start_date=datetime(2016, 1, 1)
)

PythonOperator(
    task_id='test_task',
    python_callable=test_logging_fn,
    provide_context=True,
    dag=dag,
)
