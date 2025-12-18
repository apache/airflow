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
from datetime import timedelta

import atheris
import pendulum

os.environ.setdefault("AIRFLOW_HOME", "/tmp/airflow")

with atheris.instrument_imports(include=["airflow"], enable_loader_override=False):
    from airflow import DAG
    from airflow.exceptions import AirflowException, DeserializationError
    from airflow.serialization.serialized_objects import SerializedDAG

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


def _collect_containers(obj, dicts: list[dict], lists: list[list], depth: int = 0) -> None:
    if depth > 32:
        return
    if isinstance(obj, dict):
        dicts.append(obj)
        for v in obj.values():
            _collect_containers(v, dicts, lists, depth + 1)
    elif isinstance(obj, list):
        lists.append(obj)
        for v in obj:
            _collect_containers(v, dicts, lists, depth + 1)


def _fuzz_value(fdp: atheris.FuzzedDataProvider, depth: int = 0):
    if depth > 2:
        return None
    choice = fdp.ConsumeIntInRange(0, 7)
    if choice == 0:
        return None
    if choice == 1:
        return fdp.ConsumeBool()
    if choice == 2:
        return fdp.ConsumeIntInRange(-1024, 1024)
    if choice == 3:
        return fdp.ConsumeString(256)
    if choice == 4:
        return fdp.ConsumeFloat()
    if choice == 5:
        return [_fuzz_value(fdp, depth + 1) for _ in range(fdp.ConsumeIntInRange(0, 8))]
    return {
        fdp.ConsumeString(16): _fuzz_value(fdp, depth + 1) for _ in range(fdp.ConsumeIntInRange(0, 8))
    }


def _mutate(serialized: dict, fdp: atheris.FuzzedDataProvider) -> None:
    dicts: list[dict] = []
    lists: list[list] = []
    _collect_containers(serialized, dicts, lists)

    if not dicts and not lists:
        return

    action = fdp.ConsumeIntInRange(0, 7)
    if action in (0, 1, 2, 3) and dicts:
        d = dicts[fdp.ConsumeIntInRange(0, len(dicts) - 1)]
        if action == 0 and d:
            k = list(d.keys())[fdp.ConsumeIntInRange(0, len(d) - 1)]
            d.pop(k, None)
        elif action == 1 and d:
            k = list(d.keys())[fdp.ConsumeIntInRange(0, len(d) - 1)]
            d[k] = _fuzz_value(fdp)
        elif action == 2:
            d[fdp.ConsumeString(16)] = _fuzz_value(fdp)
        else:
            # Rename key.
            if d:
                k = list(d.keys())[fdp.ConsumeIntInRange(0, len(d) - 1)]
                v = d.pop(k)
                d[fdp.ConsumeString(16) or k] = v
        return

    if lists:
        lst = lists[fdp.ConsumeIntInRange(0, len(lists) - 1)]
        if action == 4 and lst:
            idx = fdp.ConsumeIntInRange(0, len(lst) - 1)
            lst[idx] = _fuzz_value(fdp)
        elif action == 5 and len(lst) < 128:
            lst.append(_fuzz_value(fdp))
        elif action == 6 and lst:
            idx = fdp.ConsumeIntInRange(0, len(lst) - 1)
            del lst[idx]
        elif action == 7 and lst:
            del lst[fdp.ConsumeIntInRange(0, len(lst) - 1) :]


def TestInput(input_bytes: bytes):
    if len(input_bytes) > 8192:
        return

    fdp = atheris.FuzzedDataProvider(input_bytes)
    start_date = pendulum.datetime(2020, 1, 1, tz="UTC")

    schedule = fdp.PickValueInList([None, "@daily", "@hourly", "*/5 * * * *"])
    if fdp.ConsumeBool():
        schedule = fdp.ConsumeString(64) or schedule

    try:
        with DAG(
            fdp.ConsumeString(64) or "dag",
            schedule=schedule,
            start_date=start_date,
            catchup=fdp.ConsumeBool(),
        ) as dag:
            t1 = DummyOperator(task_id=fdp.ConsumeString(64) or "t1")
            t2 = PythonOperator(task_id=fdp.ConsumeString(64) or "t2", python_callable=_py_func)
            t1 >> t2
        dag.description = fdp.ConsumeString(128)
        dag.dagrun_timeout = timedelta(seconds=fdp.ConsumeIntInRange(0, 3600))
    except Exception:
        return

    try:
        serialized = SerializedDAG.to_dict(dag)
    except Exception:
        return

    for _ in range(fdp.ConsumeIntInRange(0, 32)):
        try:
            _mutate(serialized, fdp)
        except Exception:
            break

    try:
        deserialized = SerializedDAG.from_dict(serialized)
        _ = deserialized.dag_id
        if getattr(deserialized, "task_dict", None):
            _ = list(deserialized.task_dict.keys())[:3]
    except (AirflowException, DeserializationError, ValueError, TypeError, KeyError, RecursionError):
        return


def main():
    atheris.Setup(sys.argv, TestInput, enable_python_coverage=True)
    atheris.Fuzz()


if __name__ == "__main__":
    main()
