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

from airflow.providers.amazon.aws.hooks.comprehend import ComprehendHook
from airflow.providers.amazon.aws.triggers.comprehend import (
    ComprehendCreateDocumentClassifierCompletedTrigger,
    ComprehendPiiEntitiesDetectionJobCompletedTrigger,
)
from airflow.triggers.base import TriggerEvent

from providers.tests.amazon.aws.utils.test_waiter import assert_expected_waiter_type

BASE_TRIGGER_CLASSPATH = "airflow.providers.amazon.aws.triggers.comprehend."


class TestBaseComprehendTrigger:
    EXPECTED_WAITER_NAME: str | None = None
    JOB_ID: str | None = None

    def test_setup(self):
        # Ensure that all subclasses have an expected waiter name set.
        if self.__class__.__name__ != "TestBaseComprehendTrigger":
            assert isinstance(self.EXPECTED_WAITER_NAME, str)
            assert isinstance(self.JOB_ID, str)


class TestComprehendPiiEntitiesDetectionJobCompletedTrigger(TestBaseComprehendTrigger):
    EXPECTED_WAITER_NAME = "pii_entities_detection_job_complete"
    JOB_ID = "job_id"

    def test_serialization(self):
        """Assert that arguments and classpath are correctly serialized."""
        trigger = ComprehendPiiEntitiesDetectionJobCompletedTrigger(job_id=self.JOB_ID)
        classpath, kwargs = trigger.serialize()
        assert (
            classpath
            == BASE_TRIGGER_CLASSPATH
            + "ComprehendPiiEntitiesDetectionJobCompletedTrigger"
        )
        assert kwargs.get("job_id") == self.JOB_ID

    @pytest.mark.asyncio
    @mock.patch.object(ComprehendHook, "get_waiter")
    @mock.patch.object(ComprehendHook, "async_conn")
    async def test_run_success(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.__aenter__.return_value = mock.MagicMock()
        mock_get_waiter().wait = AsyncMock()
        trigger = ComprehendPiiEntitiesDetectionJobCompletedTrigger(job_id=self.JOB_ID)

        generator = trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent({"status": "success", "job_id": self.JOB_ID})
        assert_expected_waiter_type(mock_get_waiter, self.EXPECTED_WAITER_NAME)
        mock_get_waiter().wait.assert_called_once()


class TestComprehendCreateDocumentClassifierCompletedTrigger:
    EXPECTED_WAITER_NAME = "create_document_classifier_complete"
    DOCUMENT_CLASSIFIER_ARN = "arn:aws:comprehend:us-east-1:123456789012:document-classifier/insurance-classifier/version/v1"

    def test_serialization(self):
        """Assert that arguments and classpath are correctly serialized."""
        trigger = ComprehendCreateDocumentClassifierCompletedTrigger(
            document_classifier_arn=self.DOCUMENT_CLASSIFIER_ARN
        )
        classpath, kwargs = trigger.serialize()
        assert (
            classpath
            == BASE_TRIGGER_CLASSPATH
            + "ComprehendCreateDocumentClassifierCompletedTrigger"
        )
        assert kwargs.get("document_classifier_arn") == self.DOCUMENT_CLASSIFIER_ARN

    @pytest.mark.asyncio
    @mock.patch.object(ComprehendHook, "get_waiter")
    @mock.patch.object(ComprehendHook, "async_conn")
    async def test_run_success(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.__aenter__.return_value = mock.MagicMock()
        mock_get_waiter().wait = AsyncMock()
        trigger = ComprehendCreateDocumentClassifierCompletedTrigger(
            document_classifier_arn=self.DOCUMENT_CLASSIFIER_ARN
        )

        generator = trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent(
            {"status": "success", "document_classifier_arn": self.DOCUMENT_CLASSIFIER_ARN}
        )
        assert_expected_waiter_type(mock_get_waiter, self.EXPECTED_WAITER_NAME)
        mock_get_waiter().wait.assert_called_once()
