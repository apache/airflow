#!/usr/bin/env python
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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
from __future__ import annotations

import ast
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_prek_utils is imported
from common_prek_utils import AIRFLOW_CORE_SOURCES_PATH, console

TI_PATH = AIRFLOW_CORE_SOURCES_PATH / "airflow" / "models" / "taskinstance.py"
TIS_PATH = AIRFLOW_CORE_SOURCES_PATH / "airflow" / "models" / "taskinstancehistory.py"


def get_class_attributes(module_path, class_name):
    with open(module_path) as file:
        tree = ast.parse(file.read(), filename=module_path)

    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            attributes = [n.targets[0].id for n in node.body if isinstance(n, ast.Assign)]
            return attributes
    return []


def compare_attributes(path1, path2):
    diff = set(get_class_attributes(path1, "TaskInstance")) - set(
        get_class_attributes(path2, "TaskInstanceHistory")
    )
    diff = diff - {
        "run_after",
        "_logger_name",
        "_rendered_map_index",
        "_task_display_property_value",
        "task_instance_note",
        "dag_run",
        "trigger",
        "logical_date",
        "triggerer_job",
        "note",
        "rendered_task_instance_fields",
        "hitl_detail",
        # Storing last heartbeat for historic TIs is not interesting/useful
        "last_heartbeat_at",
        "id",
    }  # exclude attrs not necessary to be in TaskInstanceHistory
    if not diff:
        return
    console.print(f"Attributes in TaskInstance but not in TaskInstanceHistory: \n\n {diff}")
    console.print("\n Please make sure that the attributes are consistent across the classes.")
    sys.exit(1)


if __name__ == "__main__":
    compare_attributes(TI_PATH, TIS_PATH)
