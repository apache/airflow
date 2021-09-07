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

"""Example DAG demostrating how to implement a custom timetable for a DAG."""

# [START howto_timetable]
from airflow import DAG
from airflow.example_dags.plugins.workday import AfterWorkdayTimetable
from airflow.operators.dummy import DummyOperator

with DAG(timetable=AfterWorkdayTimetable(), tags=["example", "timetable"]) as dag:
    DummyOperator(task_id="run_this")

if __name__ == "__main__":
    dag.cli()
# [END howto_timetable]
