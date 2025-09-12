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
from __future__ import annotations

from airflow.sdk.bases.operator import BaseOperator
from airflow.triggers.base import BaseTrigger
from airflow.triggers.base import StartTriggerArgs

class DummyOperator(BaseOperator):
    template_fields = ("name",)


class DummyTrigger(BaseTrigger):
    def __init__(self, name: str, **kwargs):
        super().__init__(**kwargs)
        self.name = name

    def run(self):
        return None

    def serialize(self):
        return {"name": self.name}


def test_render_template_fields(create_task_instance):
    op = DummyOperator(task_id="dummy_task")
    ti = create_task_instance(
        task=op,
        start_from_trigger=True,
        start_trigger_args=StartTriggerArgs(
            trigger_cls=f"{DummyTrigger.__module__}.{DummyTrigger.__qualname__}",
            next_method="resume_method",
            trigger_kwargs={"name": "Hello {{ name }}"}
        )
    )

    trigger = DummyTrigger(name="Hello {{ name }}")
    trigger.task_instance = ti

    assert trigger.template_fields == ("name",)

    trigger.render_template_fields(context={"name": "world"})

    assert trigger.name == "Hello world"
