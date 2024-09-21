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

import datetime

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.standard.sensors.date_time import DateTimeSensor
from airflow.utils import timezone

with DAG(
    dag_id="test_sensor", start_date=datetime.datetime(2022, 1, 1), catchup=False, schedule="@once"
) as dag:

    @task
    def get_date():
        return str(timezone.utcnow() + datetime.timedelta(seconds=3))

    DateTimeSensor(task_id="dts", target_time=str(get_date()), poke_interval=1, mode="reschedule")
