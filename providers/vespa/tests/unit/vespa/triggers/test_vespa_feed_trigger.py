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

from unittest.mock import Mock, patch

import pytest

from airflow.providers.vespa.triggers.vespa_feed_trigger import VespaFeedTrigger


class TestVespaFeedTrigger:
    def test_init(self):
        docs = [{"id": "1", "title": "Test"}]
        conn_info = {"host": "https://vespa.test:8080", "namespace": "test", "schema": "doc"}

        trigger = VespaFeedTrigger(docs=docs, conn_info=conn_info, operation_type="feed")

        assert trigger.docs == docs
        assert trigger.conn_info == conn_info
        assert trigger.operation_type == "feed"
        assert trigger.feed_kwargs == {}

    def test_init_with_feed_kwargs(self):
        trigger = VespaFeedTrigger(
            docs=[{"id": "1"}],
            conn_info={"host": "h"},
            operation_type="update",
            feed_kwargs={"auto_assign": False, "create": True},
        )

        assert trigger.feed_kwargs == {"auto_assign": False, "create": True}

    def test_serialize(self):
        docs = [{"id": "1"}]
        conn_info = {"host": "test"}
        kw = {"auto_assign": False}

        trigger = VespaFeedTrigger(docs=docs, conn_info=conn_info, feed_kwargs=kw)
        class_path, data = trigger.serialize()

        assert class_path == "airflow.providers.vespa.triggers.vespa_feed_trigger.VespaFeedTrigger"
        assert data["docs"] == docs
        assert data["conn_info"] == conn_info
        assert data["operation_type"] == "feed"
        assert data["feed_kwargs"] == kw

    @pytest.mark.asyncio
    async def test_run_success(self):
        docs = [{"id": "1", "content": "test"}]
        conn_info = {
            "host": "vespa.test",
            "port": 19071,
            "namespace": "test",
            "schema": "doc",
            "extra": {"protocol": "https", "max_workers": 4},
        }
        trigger = VespaFeedTrigger(docs=docs, conn_info=conn_info)

        with patch("airflow.providers.vespa.hooks.vespa.VespaHook") as mock_hook_class:
            mock_hook = Mock(spec=["feed_iterable"])
            mock_hook_class.from_resolved_connection.return_value = mock_hook

            def feed_iterable(*args, **kwargs):
                mock_hook_class.from_resolved_connection.assert_called_once_with(
                    host="vespa.test",
                    port=19071,
                    schema="doc",
                    namespace="test",
                    extra={"protocol": "https", "max_workers": 4},
                )
                return {"sent": 1, "errors": 0, "error_details": []}

            mock_hook.feed_iterable.side_effect = feed_iterable

            events = []
            async for event in trigger.run():
                events.append(event)

            mock_hook.feed_iterable.assert_called_once_with(docs, operation_type="feed")
            assert len(events) == 1
            assert events[0].payload["success"] is True
            assert events[0].payload["sent"] == 1

    @pytest.mark.asyncio
    async def test_run_forwards_feed_kwargs(self):
        docs = [{"id": "1", "fields": {"x": {"assign": 1}}}]
        conn_info = {
            "host": "https://vespa.test:8080",
            "namespace": "test",
            "schema": "doc",
            "extra": {},
        }
        trigger = VespaFeedTrigger(
            docs=docs,
            conn_info=conn_info,
            operation_type="update",
            feed_kwargs={"auto_assign": False, "create": True},
        )

        with patch("airflow.providers.vespa.hooks.vespa.VespaHook") as mock_hook_class:
            mock_hook = Mock(spec=["feed_iterable"])
            mock_hook.feed_iterable.return_value = {"sent": 1, "errors": 0, "error_details": []}
            mock_hook_class.from_resolved_connection.return_value = mock_hook

            events = []
            async for event in trigger.run():
                events.append(event)

            mock_hook.feed_iterable.assert_called_once_with(
                docs, operation_type="update", auto_assign=False, create=True
            )

    @pytest.mark.asyncio
    async def test_run_hook_creation_failure(self):
        trigger = VespaFeedTrigger(
            docs=[{"id": "1"}],
            conn_info={"host": "invalid", "namespace": "test", "schema": "doc", "extra": {}},
        )

        with patch("airflow.providers.vespa.hooks.vespa.VespaHook") as mock_hook_class:
            mock_hook_class.from_resolved_connection.side_effect = ValueError("Invalid host URL")

            events = []
            async for event in trigger.run():
                events.append(event)

            assert len(events) == 1
            assert events[0].payload["success"] is False
            assert "ValueError: Invalid host URL" in events[0].payload["errors"][0]["error"]

    @pytest.mark.asyncio
    async def test_run_feed_operation_failure(self):
        trigger = VespaFeedTrigger(
            docs=[{"id": "1"}],
            conn_info={"host": "https://vespa.test:8080", "namespace": "test", "schema": "doc", "extra": {}},
        )

        with patch("airflow.providers.vespa.hooks.vespa.VespaHook") as mock_hook_class:
            mock_hook = Mock(spec=["feed_iterable"])
            mock_hook.feed_iterable.side_effect = ConnectionError("Unable to connect")
            mock_hook_class.from_resolved_connection.return_value = mock_hook

            events = []
            async for event in trigger.run():
                events.append(event)

            assert events[0].payload["success"] is False
            assert "ConnectionError" in events[0].payload["errors"][0]["error"]

    @pytest.mark.asyncio
    async def test_run_with_feed_errors(self):
        docs = [{"id": "1"}, {"id": "2"}, {"id": "3"}]
        conn_info = {"host": "https://vespa.test:8080", "namespace": "test", "schema": "doc", "extra": {}}
        trigger = VespaFeedTrigger(docs=docs, conn_info=conn_info)

        with patch("airflow.providers.vespa.hooks.vespa.VespaHook") as mock_hook_class:
            mock_hook = Mock(spec=["feed_iterable"])
            mock_hook.feed_iterable.return_value = {
                "sent": 3,
                "errors": 2,
                "error_details": [
                    {"id": "2", "status": 400, "reason": {"error": "Invalid"}},
                    {"id": "3", "status": 500, "reason": {"error": "Server error"}},
                ],
            }
            mock_hook_class.from_resolved_connection.return_value = mock_hook

            events = []
            async for event in trigger.run():
                events.append(event)

            assert events[0].payload["success"] is False
            assert events[0].payload["sent"] == 3
            assert len(events[0].payload["errors"]) == 2

    @pytest.mark.asyncio
    async def test_run_update_operation(self):
        docs = [{"id": "1", "fields": {"title": {"assign": "Updated"}}}]
        conn_info = {"host": "https://vespa.test:8080", "namespace": "test", "schema": "doc", "extra": {}}
        trigger = VespaFeedTrigger(docs=docs, conn_info=conn_info, operation_type="update")

        with patch("airflow.providers.vespa.hooks.vespa.VespaHook") as mock_hook_class:
            mock_hook = Mock(spec=["feed_iterable"])
            mock_hook.feed_iterable.return_value = {"sent": 1, "errors": 0, "error_details": []}
            mock_hook_class.from_resolved_connection.return_value = mock_hook

            events = []
            async for event in trigger.run():
                events.append(event)

            mock_hook.feed_iterable.assert_called_once_with(docs, operation_type="update")
            assert events[0].payload["success"] is True

    @pytest.mark.asyncio
    async def test_run_delete_operation(self):
        docs = [{"id": "1"}, {"id": "2"}]
        conn_info = {"host": "https://vespa.test:8080", "namespace": "test", "schema": "doc", "extra": {}}
        trigger = VespaFeedTrigger(docs=docs, conn_info=conn_info, operation_type="delete")

        with patch("airflow.providers.vespa.hooks.vespa.VespaHook") as mock_hook_class:
            mock_hook = Mock(spec=["feed_iterable"])
            mock_hook.feed_iterable.return_value = {"sent": 2, "errors": 0, "error_details": []}
            mock_hook_class.from_resolved_connection.return_value = mock_hook

            events = []
            async for event in trigger.run():
                events.append(event)

            mock_hook.feed_iterable.assert_called_once_with(docs, operation_type="delete")
            assert events[0].payload["success"] is True

    @pytest.mark.asyncio
    async def test_run_default_feed_operation(self):
        docs = [{"id": "1", "content": "test"}]
        conn_info = {"host": "https://vespa.test:8080", "namespace": "test", "schema": "doc", "extra": {}}
        trigger = VespaFeedTrigger(docs=docs, conn_info=conn_info)

        with patch("airflow.providers.vespa.hooks.vespa.VespaHook") as mock_hook_class:
            mock_hook = Mock(spec=["feed_iterable"])
            mock_hook.feed_iterable.return_value = {"sent": 1, "errors": 0, "error_details": []}
            mock_hook_class.from_resolved_connection.return_value = mock_hook

            events = []
            async for event in trigger.run():
                events.append(event)

            mock_hook.feed_iterable.assert_called_once_with(docs, operation_type="feed")
            assert events[0].payload["success"] is True
