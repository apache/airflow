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
"""Helpers for the e2e tests of language SDK Dags generated from a ``dags.yaml`` in the Dag bundle.

Each entry of the YAML has a ``dag_id`` and a ``greeting``, and its Dag has one ``greet`` task that
returns the greeting.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import yaml

if TYPE_CHECKING:
    from pathlib import Path

    from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient


def write_yaml_atomically(path: Path, greetings: dict[str, str]) -> None:
    """Replace the YAML in one step, so the Dag processor never reads half of it."""
    entries = [{"dag_id": dag_id, "greeting": greeting} for dag_id, greeting in greetings.items()]
    tmp_path = path.with_name(f"{path.name}.tmp")
    tmp_path.write_text(yaml.safe_dump({"dags": entries}))
    os.replace(tmp_path, path)


def swap_yaml_and_check(
    client: AirflowClient,
    yaml_path: Path,
    greetings: dict[str, str],
    *,
    previous: set[str],
    tags: list[str],
    timeout: int,
    task_timeout: int,
) -> set[str]:
    """Swap in a YAML of *greetings*, then check the Dags, a run of each, and the Dags it dropped."""
    write_yaml_atomically(yaml_path, greetings)
    client.wait_for_dag_ids(set(greetings), tags=tags, timeout=timeout)

    run_ids = {
        dag_id: client.trigger_dag(dag_id, json={"logical_date": datetime.now(UTC).isoformat()})["dag_run_id"]
        for dag_id in greetings
    }
    for dag_id, run_id in run_ids.items():
        state = client.wait_for_dag_run(dag_id=dag_id, run_id=run_id, timeout=task_timeout)
        assert state == "success", f"{dag_id} run {run_id} ended {state!r}"
        xcom = client.get_xcom_value(dag_id=dag_id, task_id="greet", run_id=run_id, key="return_value")
        assert xcom["value"] == greetings[dag_id], f"{dag_id} returned {xcom['value']!r}"

    for dag_id in previous - set(greetings):
        assert client.get_dag(dag_id)["is_stale"], f"{dag_id} left the YAML but is not stale"
    return set(greetings)
