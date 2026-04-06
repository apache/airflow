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

from unittest.mock import patch

import pytest

from airflow.providers.microsoft.azure.triggers.message_bus import AzureServiceBusQueueTrigger

from tests_common.test_utils.common_msg_queue import mark_common_msg_queue_test

pytest.importorskip("airflow.providers.common.messaging.providers.base_provider")


class TestAzureServiceBusMessageQueueProvider:
    """Tests for AzureServiceBusMessageQueueProvider."""

    def setup_method(self):
        """Set up the test environment."""
        from airflow.providers.microsoft.azure.queues.asb import AzureServiceBusMessageQueueProvider

        self.provider = AzureServiceBusMessageQueueProvider()

    def test_queue_create(self):
        """Test the creation of the AzureServiceBusMessageQueueProvider."""
        from airflow.providers.common.messaging.providers.base_provider import BaseMessageQueueProvider

        assert isinstance(self.provider, BaseMessageQueueProvider)

    @pytest.mark.parametrize(
        ("scheme", "expected_result"),
        [
            pytest.param("azure+servicebus", True, id="azure_servicebus_scheme"),
            pytest.param("sqs", False, id="sqs_scheme"),
            pytest.param("kafka", False, id="kafka_scheme"),
            pytest.param("redis+pubsub", False, id="redis_pubsub_scheme"),
            pytest.param("google+pubsub", False, id="google_pubsub_scheme"),
            pytest.param("azure", False, id="azure_only_scheme"),
            pytest.param("unknown", False, id="unknown_scheme"),
        ],
    )
    def test_scheme_matches(self, scheme, expected_result):
        """Test the scheme_matches method with various schemes."""
        assert self.provider.scheme_matches(scheme) == expected_result

    def test_trigger_class(self):
        """Test the trigger_class method."""
        assert self.provider.trigger_class() == AzureServiceBusQueueTrigger


@mark_common_msg_queue_test
class TestMessageQueueTriggerIntegration:
    """Test integration with MessageQueueTrigger framework."""

    @pytest.mark.usefixtures("cleanup_providers_manager")
    def test_provider_integrations_with_scheme_param(self):
        """Test that MessageQueueTrigger correctly resolves to AzureServiceBusQueueTrigger."""
        from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
        from airflow.providers.microsoft.azure.triggers.message_bus import AzureServiceBusQueueTrigger

        with patch("airflow.providers.microsoft.azure.triggers.message_bus.MessageHook"):
            trigger = MessageQueueTrigger(scheme="azure+servicebus", queues=["test_queue"])
            assert isinstance(trigger.trigger, AzureServiceBusQueueTrigger)
