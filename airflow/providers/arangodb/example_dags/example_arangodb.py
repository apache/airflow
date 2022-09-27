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

from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.arangodb.operators.arangodb import AQLOperator
from airflow.providers.arangodb.sensors.arangodb import AQLSensor

dag = DAG(
    'example_arangodb_operator',
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
)

# [START howto_aql_sensor_arangodb]

sensor = AQLSensor(
    task_id="aql_sensor",
    query="FOR doc IN students FILTER doc.name == 'judy' RETURN doc",
    timeout=60,
    poke_interval=10,
    dag=dag,
)

# [END howto_aql_sensor_arangodb]

# [START howto_aql_sensor_template_file_arangodb]

sensor2 = AQLSensor(
    task_id="aql_sensor_template_file",
    query="search_judy.sql",
    timeout=60,
    poke_interval=10,
    dag=dag,
)

# [END howto_aql_sensor_template_file_arangodb]


# [START howto_aql_operator_arangodb]

operator = AQLOperator(
    task_id='aql_operator',
    query="FOR doc IN students RETURN doc",
    dag=dag,
    result_processor=lambda cursor: print([document["name"] for document in cursor]),
)

# [END howto_aql_operator_arangodb]

# [START howto_aql_operator_template_file_arangodb]

operator2 = AQLOperator(
    task_id='aql_operator_template_file',
    dag=dag,
    result_processor=lambda cursor: print([document["name"] for document in cursor]),
    query="search_all.sql",
)

# [END howto_aql_operator_template_file_arangodb]
