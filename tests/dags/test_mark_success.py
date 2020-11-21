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
from datetime import datetime
from time import sleep

from airflow.models import DAG
from airflow.operators.python import PythonOperator

DEFAULT_DATE = datetime(2016, 1, 1)

args = {
    'owner': 'airflow',
    'start_date': DEFAULT_DATE,
}


dag = DAG(dag_id='test_mark_success', default_args=args)
task = PythonOperator(
    task_id='task1', python_callable=lambda x: sleep(x), op_args=[600], dag=dag  # pylint: disable=W0108
)
