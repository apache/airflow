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
from pydantic import TypeAdapter, ValidationError

from airflow.api_fastapi.core_api.datamodels.common import BulkBody, BulkCreateAction
from airflow.api_fastapi.core_api.datamodels.variables import VariableBody

# The bulk body is generic; ``VariableBody`` is the smallest entity to instantiate it with. The
# discriminator under test is shared by every bulk endpoint (variables, pools, connections,
# Dag runs, task instances), so one concrete instantiation covers all of them.
_bulk_adapter: TypeAdapter[BulkBody[VariableBody]] = TypeAdapter(BulkBody[VariableBody])


def test_bulk_action_discriminator_reads_the_tag_off_a_model_instance():
    """
    Validating an already-built action -- what ``model_validate`` on a model instance does -- hands
    the discriminator the instance rather than a mapping, so the tag has to be read as an attribute.
    """
    action = BulkCreateAction[VariableBody](action="create", entities=[VariableBody(key="k", value="v")])
    validated = _bulk_adapter.validate_python({"actions": [action]})
    assert isinstance(validated.actions[0], BulkCreateAction)


@pytest.mark.parametrize(
    "action",
    [
        pytest.param("x", id="not_a_mapping"),
        pytest.param(None, id="null"),
        pytest.param(5, id="number"),
        pytest.param({"entities": []}, id="missing_action_key"),
        pytest.param({"action": "bogus", "entities": []}, id="unknown_action_value"),
        pytest.param({"action": None, "entities": []}, id="null_action_value"),
        pytest.param({"action": ["create"], "entities": []}, id="unhashable_action_value"),
    ],
)
def test_bulk_action_discriminator_reports_invalid_actions_as_validation_errors(action):
    """
    A callable discriminator is handed the *raw, unvalidated* input and pydantic does not wrap what
    it raises, so any exception escaping it surfaces as a 500 instead of a 422. Every malformed
    ``action`` entry must instead come back as a ``ValidationError`` naming the accepted tags.
    """
    with pytest.raises(ValidationError) as exc_info:
        _bulk_adapter.validate_python({"actions": [action]})

    (error,) = exc_info.value.errors()
    assert error["loc"] == ("actions", 0)
    assert error["msg"] == "Each entry needs an 'action' of 'create', 'delete', 'update'"
