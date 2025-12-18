#!/usr/bin/python3
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

import os
import sys
from datetime import datetime, timedelta

import atheris

os.environ.setdefault("AIRFLOW_HOME", "/tmp/airflow")

with atheris.instrument_imports(include=["airflow"], enable_loader_override=False):
    from airflow import DAG
    from airflow.exceptions import AirflowException

    try:
        from airflow.providers.standard.operators.empty import EmptyOperator as DummyOperator
        from airflow.providers.standard.operators.python import PythonOperator
    except ImportError:
        try:
            from airflow.operators.empty import EmptyOperator as DummyOperator
            from airflow.operators.python import PythonOperator
        except ImportError:
            from airflow.operators.dummy_operator import DummyOperator
            from airflow.operators.python_operator import PythonOperator


def _py_func():
    return None


def TestInput(input_bytes: bytes):
    fdp = atheris.FuzzedDataProvider(input_bytes)

    default_args = {
        "owner": fdp.ConsumeString(32),
        "depends_on_past": fdp.ConsumeBool(),
        "start_date": datetime.now() - timedelta(days=fdp.ConsumeIntInRange(1, 5)),
        "email": [fdp.ConsumeString(64)],
        "email_on_failure": fdp.ConsumeBool(),
        "email_on_retry": fdp.ConsumeBool(),
        "retries": fdp.ConsumeIntInRange(0, 5),
        "retry_delay": timedelta(minutes=fdp.ConsumeIntInRange(0, 5)),
    }

    schedule = fdp.PickValueInList(
        [
            None,
            "@once",
            "@hourly",
            "@daily",
            "@weekly",
            "@monthly",
            "@yearly",
            "0 0 * * *",
            "*/5 * * * *",
        ]
    )
    if fdp.ConsumeBool():
        schedule = fdp.ConsumeString(64)

    try:
        with DAG(
            fdp.ConsumeString(64),
            schedule=schedule,
            default_args=default_args,
            catchup=fdp.ConsumeBool(),
        ) as dag:
            dummy_task = DummyOperator(
                task_id=fdp.ConsumeString(64),
                retries=fdp.ConsumeIntInRange(0, 5),
            )
            python_task = PythonOperator(task_id=fdp.ConsumeString(64), python_callable=_py_func)

            dummy_task >> python_task
        _ = dag.dag_id
    except (AirflowException, ValueError, TypeError):
        return


def main():
    atheris.Setup(sys.argv, TestInput, enable_python_coverage=True)
    atheris.Fuzz()


if __name__ == "__main__":
    main()

