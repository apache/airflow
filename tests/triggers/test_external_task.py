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

from datetime import datetime

import pendulum

from airflow.triggers.external_task import ExternalTaskTrigger
from airflow.utils.state import State


def test_external_task_trigger_serialization():
    """
    Tests that the DateTimeTrigger correctly serializes its arguments
    and classpath.
    """
    default_values = {
        "external_task_ids": None,
        "allowed_states": [State.SUCCESS],
        "failed_states": [],
        "poke_interval": 60,
    }

    kwargs = {
        "dttm": pendulum.instance(datetime(2020, 4, 1, 13, 0), pendulum.UTC),
        "external_dag_id": "dag_id",
        "external_task_ids": ["task_id1", "task_id2"],
    }
    trigger = ExternalTaskTrigger(**kwargs)
    serialize_class, serialize_kwargs = trigger.serialize()
    assert serialize_class == "airflow.triggers.external_task.ExternalTaskTrigger"
    assert serialize_kwargs == {**default_values, **kwargs}

    kwargs = {
        "dttm": pendulum.instance(datetime(2020, 4, 1, 13, 0), pendulum.UTC),
        "external_dag_id": "dag_id",
        "external_task_ids": ["task_id1", "task_id2"],
        "allowed_states": [State.SUCCESS, State.RUNNING],
        "failed_states": [State.FAILED, State.REMOVED, State.RESTARTING],
    }
    trigger = ExternalTaskTrigger(**kwargs)
    serialize_class, serialize_kwargs = trigger.serialize()
    assert serialize_class == "airflow.triggers.external_task.ExternalTaskTrigger"
    assert serialize_kwargs == {**default_values, **kwargs}
