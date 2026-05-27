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

import pytest

from airflow.sdk.bases.operator import BaseOperator
from airflow.triggers.base import BaseEventTrigger, BaseTrigger, StartTriggerArgs, TriggerEvent


class DummyOperator(BaseOperator):
    template_fields = ("name",)


class OperatorWithExtraTemplateFields(BaseOperator):
    """Operator whose template_fields do NOT all exist on the trigger."""

    template_fields = ("bash_command", "env", "name")

    def __init__(self, bash_command="", env=None, name="", **kwargs):
        super().__init__(**kwargs)
        self.bash_command = bash_command
        self.env = env
        self.name = name


class DummyTrigger(BaseTrigger):
    def __init__(self, name: str, **kwargs):
        super().__init__(**kwargs)
        self.name = name

    def run(self):
        return None

    def serialize(self):
        return {"name": self.name}


@pytest.mark.db_test
def test_render_template_fields(create_task_instance):
    op = DummyOperator(task_id="dummy_task")
    ti = create_task_instance(
        task=op,
        start_from_trigger=True,
        start_trigger_args=StartTriggerArgs(
            trigger_cls=f"{DummyTrigger.__module__}.{DummyTrigger.__qualname__}",
            next_method="resume_method",
            trigger_kwargs={"name": "Hello {{ name }}"},
        ),
    )

    trigger = DummyTrigger(name="Hello {{ name }}")

    assert not trigger.task_instance
    assert not trigger.template_fields
    assert not trigger.template_ext

    trigger.task_instance = ti

    assert trigger.task_instance == ti
    assert "name" in trigger.template_fields
    assert not trigger.template_ext

    trigger.render_template_fields(context={"name": "world"})

    assert trigger.name == "Hello world"


@pytest.mark.db_test
def test_render_template_fields_filters_to_trigger_kwargs(create_task_instance):
    """Only fields present in both trigger_kwargs and on the trigger should be rendered.

    Operator template_fields like 'bash_command' and 'env' that don't exist on the
    trigger must be excluded to avoid AttributeError.
    """
    op = OperatorWithExtraTemplateFields(
        task_id="extra_fields_task",
        bash_command="echo hello",
        env={"KEY": "val"},
        name="static",
    )
    ti = create_task_instance(
        task=op,
        start_from_trigger=True,
        start_trigger_args=StartTriggerArgs(
            trigger_cls=f"{DummyTrigger.__module__}.{DummyTrigger.__qualname__}",
            next_method="resume_method",
            trigger_kwargs={"name": "Hello {{ name }}"},
        ),
    )

    trigger = DummyTrigger(name="Hello {{ name }}")
    trigger.task_instance = ti

    # Only 'name' should be in template_fields; 'bash_command' and 'env' are excluded
    # because they aren't keys in trigger_kwargs or don't exist on the trigger.
    assert trigger.template_fields == ("name",)

    # Rendering must not raise AttributeError for missing operator fields
    trigger.render_template_fields(context={"name": "world"})
    assert trigger.name == "Hello world"


@pytest.mark.db_test
def test_render_template_fields_empty_when_no_trigger_kwargs(create_task_instance):
    """When start_trigger_args has no trigger_kwargs, template_fields should be empty."""
    op = DummyOperator(task_id="no_kwargs_task")
    ti = create_task_instance(
        task=op,
        start_from_trigger=True,
        start_trigger_args=StartTriggerArgs(
            trigger_cls=f"{DummyTrigger.__module__}.{DummyTrigger.__qualname__}",
            next_method="resume_method",
            trigger_kwargs=None,
        ),
    )

    trigger = DummyTrigger(name="Hello {{ name }}")
    trigger.task_instance = ti

    assert trigger.template_fields == ()

    # Rendering with empty template_fields is a no-op
    trigger.render_template_fields(context={"name": "world"})
    assert trigger.name == "Hello {{ name }}"


class _PlainEventTrigger(BaseEventTrigger):
    """A BaseEventTrigger that does not opt into shared streams."""

    def __init__(self, name: str = "plain"):
        super().__init__()
        self.name = name

    def serialize(self):
        return (f"{type(self).__module__}.{type(self).__qualname__}", {"name": self.name})

    async def run(self):
        yield TriggerEvent({"name": self.name})


class _SharedQueueTrigger(BaseEventTrigger):
    """A BaseEventTrigger that opts into shared streams."""

    def __init__(self, queue_url: str, region: str | None = None):
        super().__init__()
        self.queue_url = queue_url
        self.region = region

    def serialize(self):
        return (
            f"{type(self).__module__}.{type(self).__qualname__}",
            {"queue_url": self.queue_url, "region": self.region},
        )

    def shared_stream_key(self):
        return ("shared-queue", self.queue_url)

    @classmethod
    async def open_shared_stream(cls, kwargs):
        for region in ("us", "eu", "us"):
            yield {"queue_url": kwargs["queue_url"], "region": region}

    async def filter_shared_stream(self, shared_stream):
        async for raw in shared_stream:
            if self.region is None or raw["region"] == self.region:
                yield TriggerEvent(raw)

    async def run(self):  # pragma: no cover - replaced by filter_shared_stream
        yield TriggerEvent({})


def test_base_event_trigger_defaults_no_sharing():
    trigger = _PlainEventTrigger()
    assert trigger.shared_stream_key() is None


async def _drain_async_iter(it):
    async for _ in it:
        pass


@pytest.mark.asyncio
async def test_base_event_trigger_default_open_shared_stream_raises():
    with pytest.raises(NotImplementedError, match="open_shared_stream"):
        await _drain_async_iter(_PlainEventTrigger.open_shared_stream({}))


@pytest.mark.asyncio
async def test_base_event_trigger_default_filter_shared_stream_raises():
    trigger = _PlainEventTrigger()

    async def empty_stream():
        if False:
            yield  # pragma: no cover

    with pytest.raises(NotImplementedError, match="filter_shared_stream"):
        await _drain_async_iter(trigger.filter_shared_stream(empty_stream()))


def test_subclass_can_declare_shared_stream_key():
    a = _SharedQueueTrigger(queue_url="https://q", region="us")
    b = _SharedQueueTrigger(queue_url="https://q", region="eu")
    c = _SharedQueueTrigger(queue_url="https://other", region="us")

    assert a.shared_stream_key() == b.shared_stream_key()
    assert a.shared_stream_key() != c.shared_stream_key()


@pytest.mark.asyncio
async def test_subclass_filter_shared_stream_applies_per_instance_match():
    us = _SharedQueueTrigger(queue_url="https://q", region="us")

    async def stream():
        for region in ("us", "eu", "us"):
            yield {"queue_url": "https://q", "region": region}

    payloads = [event.payload async for event in us.filter_shared_stream(stream())]
    assert [p["region"] for p in payloads] == ["us", "us"]
