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
"""
Guard the invariant that every supervisor-schema body's class
``__name__`` equals the value of its ``type`` ``Literal`` discriminator.

``CommsDecoder`` routes incoming wire frames against the ``type`` literal
on each member of a discriminated union, but downstream consumers
(registry lookups, snapshot codegen, debug output) identify the head
class by ``__name__``. If the two strings drift, a frame decodes against
one class then surfaces under a different name -- a silent contract
break.
"""

from __future__ import annotations

from typing import Annotated, get_args, get_origin

import pytest
from pydantic import BaseModel

from airflow.dag_processing.processor import ToDagProcessor, ToManager
from airflow.jobs.triggerer_job_runner import ToTriggerRunner, ToTriggerSupervisor
from airflow.sdk.execution_time.comms import ToSupervisor, ToTask


def _members_of_union(union: object) -> tuple[type[BaseModel], ...]:
    """Return the BaseModel classes composing an ``Annotated[A | B | ..., Field(...)]``."""
    if get_origin(union) is Annotated:
        union = get_args(union)[0]
    return tuple(m for m in get_args(union) if isinstance(m, type) and issubclass(m, BaseModel))


# All six supervisor discriminated unions. Triggerer's two unions are
# not part of the lang-SDK-facing registry, but the same name/type
# invariant is required for ``CommsDecoder`` to round-trip them.
SUPERVISOR_UNIONS = [
    pytest.param(ToTask, id="ToTask"),
    pytest.param(ToSupervisor, id="ToSupervisor"),
    pytest.param(ToManager, id="ToManager"),
    pytest.param(ToDagProcessor, id="ToDagProcessor"),
    pytest.param(ToTriggerRunner, id="ToTriggerRunner"),
    pytest.param(ToTriggerSupervisor, id="ToTriggerSupervisor"),
]


@pytest.mark.parametrize("union", SUPERVISOR_UNIONS)
def test_class_name_matches_type_literal(union):
    """For every member, ``cls.__name__`` must equal its ``type`` Literal value."""
    mismatches: list[str] = []
    for member in _members_of_union(union):
        field = member.model_fields.get("type")
        if field is None:
            mismatches.append(f"{member.__name__}: missing `type` field")
            continue
        args = get_args(field.annotation)
        if len(args) != 1:
            mismatches.append(f"{member.__name__}: `type` must be a single-value Literal, got {args!r}")
            continue
        literal = args[0]
        if literal != member.__name__:
            mismatches.append(f"{member.__name__}: type literal = {literal!r}, expected {member.__name__!r}")

    assert not mismatches, "Class __name__ must equal its `type` Literal value:\n  " + "\n  ".join(mismatches)
