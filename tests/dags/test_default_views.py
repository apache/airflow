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
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
    'retries': 3,
    'start_date': days_ago(2)
}

tree_dag = DAG(
    dag_id='test_tree_view', default_args=args,
    schedule_interval='0 0 * * *',
    default_view='tree',
)

graph_dag = DAG(
    dag_id='test_graph_view', default_args=args,
    schedule_interval='0 0 * * *',
    default_view='graph',
)
