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

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.timezone import datetime

# The schedule_interval specified here is an INVALID
# Cron expression. This invalid DAG will be used to
# test whether dagbag.process_file() can identify
# invalid Cron expression.
dag1 = DAG(dag_id='test_invalid_cron', start_date=datetime(2015, 1, 1), schedule_interval="0 100 * * *")
dag1_task1 = DummyOperator(task_id='task1', dag=dag1, owner='airflow')
