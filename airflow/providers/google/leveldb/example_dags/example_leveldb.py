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
"""
Example use of LevelDB operators.
"""

from datetime import datetime

from airflow import models
from airflow.providers.google.leveldb.operators.leveldb import LevelDBOperator

with models.DAG(
    'example_leveldb',
    start_date=datetime(2021, 1, 1),
    schedule_interval='@once',
    catchup=False,
    tags=['example'],
) as dag:
    # [START howto_operator_leveldb_get_key]
    get_key_leveldb_task = LevelDBOperator(task_id='get_key_leveldb', command='get', key=b'key')
    # [END howto_operator_leveldb_get_key]
    # [START howto_operator_leveldb_put_key]
    put_key_leveldb_task = LevelDBOperator(
        task_id='put_key_leveldb',
        command='put',
        key=b'another_key',
        value=b'another_value',
    )
    # [END howto_operator_leveldb_put_key]
    get_key_leveldb_task >> put_key_leveldb_task
