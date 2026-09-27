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

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.dms import DmsHook
from airflow.providers.amazon.aws.triggers.dms import (
    DmsReplicationCompleteTrigger,
    DmsReplicationConfigDeletedTrigger,
    DmsReplicationDeprovisionedTrigger,
    DmsReplicationStoppedTrigger,
    DmsReplicationTerminalStatusTrigger,
    DmsTableReloadCompleteTrigger,
    DmsTaskModifyCompleteTrigger,
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


class TestDmsTaskModifyCompleteTrigger:
    EXPECTED_WAITER_NAME = "replication_task_modified"
    TASK_ARN = "arn:aws:dms:us-east-1:123456789012:task:EXAMPLE"

    def test_serialization(self):
        trigger = DmsTaskModifyCompleteTrigger(replication_task_arn=self.TASK_ARN)
        classpath, kwargs = trigger.serialize()
        assert classpath == BASE_TRIGGER_CLASSPATH + "DmsTaskModifyCompleteTrigger"
        assert kwargs["replication_task_arn"] == self.TASK_ARN

    @pytest.mark.asyncio
    @mock.patch.object(DmsHook, "get_waiter")
    @mock.patch.object(DmsHook, "get_async_conn")
    async def test_run_success(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.__aenter__.return_value = mock.MagicMock()
        mock_get_waiter().wait = AsyncMock()
        trigger = DmsTaskModifyCompleteTrigger(replication_task_arn=self.TASK_ARN, waiter_delay=0)
        response = await trigger.run().__anext__()
        assert response == TriggerEvent({"status": "success", "replication_task_arn": self.TASK_ARN})
        assert_expected_waiter_type(mock_get_waiter, self.EXPECTED_WAITER_NAME)
        mock_get_waiter().wait.assert_called_once()

    @pytest.mark.asyncio
    @mock.patch.object(DmsHook, "get_waiter")
    @mock.patch.object(DmsHook, "get_async_conn")
    async def test_run_error(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.__aenter__.return_value = mock.MagicMock()
        mock_get_waiter().wait = AsyncMock(
            side_effect=AirflowException("Replication task modification failed to complete.")
        )
        trigger = DmsTaskModifyCompleteTrigger(replication_task_arn=self.TASK_ARN, waiter_delay=0)
        response = await trigger.run().__anext__()
        assert response == TriggerEvent(
            {
                "status": "error",
                "message": "Replication task modification failed to complete.",
                "replication_task_arn": self.TASK_ARN,
            }
        )


class TestDmsTableReloadCompleteTrigger:
    TASK_ARN = "arn:aws:dms:us-east-1:123456789012:task:EXAMPLE"
    TABLES = [
        {"SchemaName": "public", "TableName": "first_table"},
        {"SchemaName": "archive", "TableName": "second_table"},
    ]

    def build_trigger(self, **overrides):
        kwargs = {
            "replication_task_arn": self.TASK_ARN,
            "tables_to_reload": self.TABLES,
            "reload_option": "data-reload",
            "waiter_delay": 5,
            "waiter_max_attempts": 10,
            "aws_conn_id": "test_conn",
            "region_name": "us-east-2",
            "verify": False,
            "botocore_config": {"read_timeout": 42},
            **overrides,
        }
        return DmsTableReloadCompleteTrigger(**kwargs)

    def test_serialization(self):
        trigger = self.build_trigger()

        classpath, kwargs = trigger.serialize()

        assert classpath == BASE_TRIGGER_CLASSPATH + "DmsTableReloadCompleteTrigger"
        assert kwargs == {
            "replication_task_arn": self.TASK_ARN,
            "tables_to_reload": self.TABLES,
            "reload_option": "data-reload",
            "waiter_delay": 5,
            "waiter_max_attempts": 10,
            "aws_conn_id": "test_conn",
            "region_name": "us-east-2",
            "verify": False,
            "botocore_config": {"read_timeout": 42},
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("reload_option", "waiter_name", "operation_name", "status_query"),
        [
            pytest.param(
                "data-reload",
                "table_reload_complete",
                "reload",
                "TableStatistics[0].TableState",
                id="data-reload",
            ),
            pytest.param(
                "validate-only",
                "table_validation_complete",
                "validation",
                "TableStatistics[0].ValidationState",
                id="validate-only",
            ),
        ],
    )
    @mock.patch(f"{DmsTableReloadCompleteTrigger.__module__}.async_wait", autospec=True)
    @mock.patch.object(DmsHook, "get_waiter", autospec=True)
    @mock.patch.object(DmsHook, "get_async_conn", autospec=True)
    async def test_run_success(
        self,
        mock_get_async_conn,
        mock_get_waiter,
        mock_async_wait,
        reload_option,
        waiter_name,
        operation_name,
        status_query,
    ):
        mock_client = mock.MagicMock(spec=["describe_table_statistics"])
        mock_waiter = mock.MagicMock(spec=["wait"])
        mock_get_async_conn.return_value.__aenter__.return_value = mock_client
        mock_get_waiter.return_value = mock_waiter

        [response] = [event async for event in self.build_trigger(reload_option=reload_option).run()]

        assert response == TriggerEvent({"status": "success", "replication_task_arn": self.TASK_ARN})
        assert mock_get_waiter.call_args_list == [
            mock.call(
                mock.ANY,
                waiter_name,
                deferrable=True,
                client=mock_client,
            ),
            mock.call(
                mock.ANY,
                waiter_name,
                deferrable=True,
                client=mock_client,
            ),
        ]
        assert mock_async_wait.await_args_list == [
            mock.call(
                mock_waiter,
                5,
                10,
                {
                    "ReplicationTaskArn": self.TASK_ARN,
                    "Filters": [
                        {"Name": "schema-name", "Values": ["public"]},
                        {"Name": "table-name", "Values": ["first_table"]},
                    ],
                },
                f"DMS table {operation_name} failed for public.first_table.",
                f"Status of DMS table {operation_name} public.first_table is",
                [status_query],
            ),
            mock.call(
                mock_waiter,
                5,
                10,
                {
                    "ReplicationTaskArn": self.TASK_ARN,
                    "Filters": [
                        {"Name": "schema-name", "Values": ["archive"]},
                        {"Name": "table-name", "Values": ["second_table"]},
                    ],
                },
                f"DMS table {operation_name} failed for archive.second_table.",
                f"Status of DMS table {operation_name} archive.second_table is",
                [status_query],
            ),
        ]

    @pytest.mark.asyncio
    @mock.patch(
        f"{DmsTableReloadCompleteTrigger.__module__}.async_wait",
        autospec=True,
        side_effect=AirflowException("DMS table reload failed."),
    )
    @mock.patch.object(DmsHook, "get_waiter", autospec=True)
    @mock.patch.object(DmsHook, "get_async_conn", autospec=True)
    async def test_run_failure(self, mock_get_async_conn, mock_get_waiter, mock_async_wait):
        mock_get_async_conn.return_value.__aenter__.return_value = mock.MagicMock(
            spec=["describe_table_statistics"]
        )
        mock_get_waiter.return_value = mock.MagicMock(spec=["wait"])

        [response] = [event async for event in self.build_trigger().run()]

        assert response.payload == {
            "status": "error",
            "message": "DMS table reload failed.",
            "replication_task_arn": self.TASK_ARN,
        }
        mock_async_wait.assert_awaited_once()
