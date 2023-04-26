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

    ti = TaskInstance(task=EmptyOperator(task_id="task"), run_id="run_id", state=State.RUNNING)
    obj = [[ti]]  # nested to verify recursive behavior

    serialized = BaseSerialization.serialize(obj, use_pydantic_models=True)  # does not raise
    deserialized = BaseSerialization.deserialize(serialized, use_pydantic_models=True)  # does not raise

    assert isinstance(deserialized[0][0], TaskInstancePydantic)
