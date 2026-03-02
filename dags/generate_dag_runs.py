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

from airflow import settings
from airflow.models import DagRun
from airflow.models.dagrun import DagRunType
from airflow.utils import timezone


def create_test_dag_runs():
    """生成測試 DAG runs."""
    session = settings.Session()

    dag_runs = [
        # 創建不同狀態的 runs
        DagRun(
            dag_id="test_dag_simple",
            run_id="test_simple_2024_01_01",
            logical_date=timezone.parse("2024-01-01"),
            start_date=timezone.parse("2024-01-01 10:00:00"),
            end_date=timezone.parse("2024-01-01 10:05:00"),
            state="success",
            run_type=DagRunType.SCHEDULED,
        ),
        DagRun(
            dag_id="test_dag_simple",
            run_id="test_simple_2024_01_02",
            logical_date=timezone.parse("2024-01-02"),
            start_date=timezone.parse("2024-01-02 12:00:00"),
            state="failed",
            run_type=DagRunType.MANUAL,
        ),
        DagRun(
            dag_id="test_dag_parametrized",
            run_id="test_param_2024_01_01",
            logical_date=timezone.parse("2024-01-01"),
            start_date=timezone.parse("2024-01-01 06:15:00"),
            end_date=timezone.parse("2024-01-01 06:45:00"),
            state="success",
            run_type=DagRunType.SCHEDULED,
        ),
    ]

    for run in dag_runs:
        session.add(run)

    session.commit()
    session.close()
    print("✅ DAG runs 已創建")


if __name__ == "__main__":
    create_test_dag_runs()
