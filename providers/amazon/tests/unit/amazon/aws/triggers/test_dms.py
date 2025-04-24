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

from unittest import mock
from unittest.mock import AsyncMock

import pytest

from airflow.providers.amazon.aws.hooks.dms import DmsHook
from airflow.providers.amazon.aws.triggers.dms import (
    DmsReplicationCompleteTrigger,
    DmsReplicationConfigDeletedTrigger,
    DmsReplicationDeprovisionedTrigger,
    DmsReplicationStoppedTrigger,
    DmsReplicationTerminalStatusTrigger,
)
from airflow.triggers.base import TriggerEvent

from unit.amazon.aws.utils.test_waiter import assert_expected_waiter_type

BASE_TRIGGER_CLASSPATH = "airflow.providers.amazon.aws.triggers.dms."


class TestBaseDmsTrigger:
    EXPECTED_WAITER_NAME: str | None = None

    def test_setup(self):
        if self.__class__.__name__ != "TestBaseDmsTrigger":
            assert isinstance(self.EXPECTED_WAITER_NAME, str)


class TestDmsReplicationCompleteTrigger(TestBaseDmsTrigger):
    EXPECTED_WAITER_NAME = "replication_complete"
    REPLICATION_CONFIG_ARN = "arn:aws:dms:region:account:config"

    def test_serialization(self):
        trigger = DmsReplicationCompleteTrigger(replication_config_arn=self.REPLICATION_CONFIG_ARN)

        classpath, kwargs = trigger.serialize()
        assert classpath == BASE_TRIGGER_CLASSPATH + "DmsReplicationCompleteTrigger"

        assert kwargs.get("replication_config_arn") == self.REPLICATION_CONFIG_ARN

    @pytest.mark.asyncio
    @mock.patch.object(DmsHook, "get_waiter")
    @mock.patch.object(DmsHook, "get_async_conn")
    async def test_complete(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.__aenter__.return_value = mock.MagicMock()
        mock_get_waiter().wait = AsyncMock()
        trigger = DmsReplicationCompleteTrigger(replication_config_arn=self.REPLICATION_CONFIG_ARN)
        generator = trigger.run()
        response = await generator.asend(None)
        assert response == TriggerEvent(
            {"status": "success", "replication_config_arn": self.REPLICATION_CONFIG_ARN}
        )
        mock_get_waiter().wait.assert_called_once()


class TestDmsReplicationTerminalStatusTrigger(TestBaseDmsTrigger):
    EXPECTED_WAITER_NAME = "replication_terminal_status"
    REPLICATION_CONFIG_ARN = "arn:aws:dms:region:account:config"

    def test_serialization(self):
        trigger = DmsReplicationTerminalStatusTrigger(replication_config_arn=self.REPLICATION_CONFIG_ARN)

        classpath, kwargs = trigger.serialize()
        assert classpath == BASE_TRIGGER_CLASSPATH + "DmsReplicationTerminalStatusTrigger"

        assert kwargs.get("replication_config_arn") == self.REPLICATION_CONFIG_ARN

    @pytest.mark.asyncio
    @mock.patch.object(DmsHook, "get_waiter")
    @mock.patch.object(DmsHook, "get_async_conn")
    async def test_complete(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.__aenter__.return_value = mock.MagicMock()
        mock_get_waiter().wait = AsyncMock()
        trigger = DmsReplicationTerminalStatusTrigger(replication_config_arn=self.REPLICATION_CONFIG_ARN)
        generator = trigger.run()
        response = await generator.asend(None)
        assert response == TriggerEvent(
            {"status": "success", "replication_config_arn": self.REPLICATION_CONFIG_ARN}
        )
        assert_expected_waiter_type(mock_get_waiter, self.EXPECTED_WAITER_NAME)

        mock_get_waiter().wait.assert_called_once()


class TestDmsReplicationConfigDeletedTrigger(TestBaseDmsTrigger):
    EXPECTED_WAITER_NAME = "replication_config_deleted"
    REPLICATION_CONFIG_ARN = "arn:aws:dms:region:account:config"

    def test_serialization(self):
        trigger = DmsReplicationConfigDeletedTrigger(replication_config_arn=self.REPLICATION_CONFIG_ARN)

        classpath, kwargs = trigger.serialize()
        assert classpath == BASE_TRIGGER_CLASSPATH + "DmsReplicationConfigDeletedTrigger"

        assert kwargs.get("replication_config_arn") == self.REPLICATION_CONFIG_ARN

    @pytest.mark.asyncio
    @mock.patch.object(DmsHook, "get_waiter")
    @mock.patch.object(DmsHook, "get_async_conn")
    async def test_complete(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.__aenter__.return_value = mock.MagicMock()
        mock_get_waiter().wait = AsyncMock()
        trigger = DmsReplicationConfigDeletedTrigger(replication_config_arn=self.REPLICATION_CONFIG_ARN)
        generator = trigger.run()
        response = await generator.asend(None)
        assert response == TriggerEvent(
            {"status": "success", "replication_config_arn": self.REPLICATION_CONFIG_ARN}
        )
        assert_expected_waiter_type(mock_get_waiter, self.EXPECTED_WAITER_NAME)

        mock_get_waiter().wait.assert_called_once()


class TestDmsReplicationStoppedTrigger(TestBaseDmsTrigger):
    EXPECTED_WAITER_NAME = "replication_stopped"
    REPLICATION_CONFIG_ARN = "arn:aws:dms:region:account:config"

    def test_serialization(self):
        trigger = DmsReplicationStoppedTrigger(replication_config_arn=self.REPLICATION_CONFIG_ARN)

        classpath, kwargs = trigger.serialize()
        assert classpath == BASE_TRIGGER_CLASSPATH + "DmsReplicationStoppedTrigger"

        """ assert kwargs.get("Filters") == [
            {"Name": "replication-config-arn", "Values": ["arn:aws:dms:region:account:config"]}
        ] """
        assert kwargs.get("replication_config_arn") == self.REPLICATION_CONFIG_ARN

    @pytest.mark.asyncio
    @mock.patch.object(DmsHook, "get_waiter")
    @mock.patch.object(DmsHook, "get_async_conn")
    async def test_complete(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.__aenter__.return_value = mock.MagicMock()
        mock_get_waiter().wait = AsyncMock()
        trigger = DmsReplicationStoppedTrigger(replication_config_arn=self.REPLICATION_CONFIG_ARN)
        generator = trigger.run()
        response = await generator.asend(None)
        assert response == TriggerEvent(
            {"status": "success", "replication_config_arn": self.REPLICATION_CONFIG_ARN}
        )
        assert_expected_waiter_type(mock_get_waiter, self.EXPECTED_WAITER_NAME)
        mock_get_waiter().wait.assert_called_once()


class TestDmsReplicationDeprovisionedTrigger(TestBaseDmsTrigger):
    EXPECTED_WAITER_NAME = "replication_deprovisioned"
    REPLICATION_CONFIG_ARN = "arn:aws:dms:region:account:config"

    def test_serialization(self):
        trigger = DmsReplicationDeprovisionedTrigger(replication_config_arn=self.REPLICATION_CONFIG_ARN)

        classpath, kwargs = trigger.serialize()
        assert classpath == BASE_TRIGGER_CLASSPATH + "DmsReplicationDeprovisionedTrigger"

        assert kwargs.get("replication_config_arn") == self.REPLICATION_CONFIG_ARN

    @pytest.mark.asyncio
    @mock.patch.object(DmsHook, "get_waiter")
    @mock.patch.object(DmsHook, "get_async_conn")
    async def test_complete(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.__aenter__.return_value = mock.MagicMock()
        mock_get_waiter().wait = AsyncMock()
        trigger = DmsReplicationDeprovisionedTrigger(replication_config_arn=self.REPLICATION_CONFIG_ARN)
        generator = trigger.run()
        response = await generator.asend(None)
        assert response == TriggerEvent(
            {"status": "success", "replication_config_arn": self.REPLICATION_CONFIG_ARN}
        )
        assert_expected_waiter_type(mock_get_waiter, self.EXPECTED_WAITER_NAME)
        mock_get_waiter().wait.assert_called_once()
