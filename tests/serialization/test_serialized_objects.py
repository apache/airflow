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

import json
from datetime import datetime

import pytest

from airflow.exceptions import SerializationError
from airflow.models.taskinstance import TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.serialization.pydantic.taskinstance import TaskInstancePydantic
from airflow.settings import _ENABLE_AIP_44
from airflow.utils.state import State
from tests import REPO_ROOT


def test_recursive_serialize_calls_must_forward_kwargs():
    """Any time we recurse cls.serialize, we must forward all kwargs."""
    import ast

    valid_recursive_call_count = 0
    file = REPO_ROOT / "airflow/serialization/serialized_objects.py"
    content = file.read_text()
    tree = ast.parse(content)

    class_def = None
    for stmt in ast.walk(tree):
        if not isinstance(stmt, ast.ClassDef):
            continue
        if stmt.name == "BaseSerialization":
            class_def = stmt

    method_def = None
    for elem in ast.walk(class_def):
        if isinstance(elem, ast.FunctionDef):
            if elem.name == "serialize":
                method_def = elem
                break
    kwonly_args = [x.arg for x in method_def.args.kwonlyargs]

    for elem in ast.walk(method_def):
        if isinstance(elem, ast.Call):
            if getattr(elem.func, "attr", "") == "serialize":
                kwargs = {y.arg: y.value for y in elem.keywords}
                for name in kwonly_args:
                    if name not in kwargs or getattr(kwargs[name], "id", "") != name:
                        ref = f"{file}:{elem.lineno}"
                        message = (
                            f"Error at {ref}; recursive calls to `cls.serialize` "
                            f"must forward the `{name}` argument"
                        )
                        raise Exception(message)
                    valid_recursive_call_count += 1
    print(f"validated calls: {valid_recursive_call_count}")
    assert valid_recursive_call_count > 0


def test_strict_mode():
    """If strict=True, serialization should fail when object is not JSON serializable."""

    class Test:
        a = 1

    from airflow.serialization.serialized_objects import BaseSerialization

    obj = [[[Test()]]]  # nested to verify recursive behavior
    BaseSerialization.serialize(obj)  # does not raise
    with pytest.raises(SerializationError, match="Encountered unexpected type"):
        BaseSerialization.serialize(obj, strict=True)  # now raises


@pytest.mark.skipif(not _ENABLE_AIP_44, reason="AIP-44 is disabled")
def test_use_pydantic_models():
    """If use_pydantic_models=True the TaskInstance object should be serialized to TaskInstancePydantic."""

    from airflow.serialization.serialized_objects import BaseSerialization

    ti = TaskInstance(
        task=EmptyOperator(task_id="task"),
        run_id="run_id",
        state=State.RUNNING,
    )
    start_date = datetime.utcnow()
    ti.start_date = start_date
    obj = [[ti]]  # nested to verify recursive behavior

    serialized = BaseSerialization.serialize(obj, use_pydantic_models=True)  # does not raise
    deserialized = BaseSerialization.deserialize(serialized, use_pydantic_models=True)  # does not raise
    assert isinstance(deserialized[0][0], TaskInstancePydantic)

    serialized_json = json.dumps(serialized)  # does not raise
    deserialized_from_json = BaseSerialization.deserialize(
        json.loads(serialized_json), use_pydantic_models=True
    )  # does not raise
    assert isinstance(deserialized_from_json[0][0], TaskInstancePydantic)
    assert deserialized_from_json[0][0].start_date == start_date


def test_serialized_mapped_operator_unmap(dag_maker):
    from airflow.serialization.serialized_objects import SerializedDAG
    from tests.test_utils.mock_operators import MockOperator

    with dag_maker(dag_id="dag") as dag:
        MockOperator(task_id="task1", arg1="x")
        MockOperator.partial(task_id="task2").expand(arg1=["a", "b"])

    serialized_dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))
    assert serialized_dag.dag_id == "dag"

    serialized_task1 = serialized_dag.get_task("task1")
    assert serialized_task1.dag is serialized_dag

    serialized_task2 = serialized_dag.get_task("task2")
    assert serialized_task2.dag is serialized_dag

    serialized_unmapped_task = serialized_task2.unmap(None)
    assert serialized_unmapped_task.dag is serialized_dag
