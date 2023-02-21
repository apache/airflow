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

import jinja2
import pytest

from airflow.notifications.basenotifier import BaseNotifier
from airflow.operators.empty import EmptyOperator
from airflow.utils.context import Context


class MockNotifier(BaseNotifier):
    """MockNotifier class for testing"""

    template_fields = ("message",)
    template_ext = (".txt",)

    def __init__(self, message: str | None = "This is a test message"):
        super().__init__()
        self.message = message

    def notify(self, context: Context) -> None:
        pass


class TestBaseNotifier:
    def test_render_message_with_message(self, dag_maker):
        with dag_maker("test_render_message_with_message") as dag:
            EmptyOperator(task_id="test_id")

        notifier = MockNotifier(message="Hello {{ dag.dag_id }}")
        context: Context = {"dag": dag}
        notifier.render_template_fields(context)
        assert notifier.message == "Hello test_render_message_with_message"

    def test_render_message_with_template(self, dag_maker, caplog):
        with dag_maker("test_render_message_with_template") as dag:
            EmptyOperator(task_id="test_id")
        notifier = MockNotifier(message="test.txt")
        context: Context = {"dag": dag}
        with pytest.raises(jinja2.exceptions.TemplateNotFound):
            notifier.render_template_fields(context)

    def test_render_message_with_template_works(self, dag_maker, caplog):
        with dag_maker("test_render_message_with_template_works") as dag:
            EmptyOperator(task_id="test_id")
        notifier = MockNotifier(message="test_notifier.txt")
        context: Context = {"dag": dag}
        notifier.render_template_fields(context)
        assert notifier.message == "Hello test_render_message_with_template_works"
