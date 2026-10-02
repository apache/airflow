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

from airflow.providers.common.messaging.providers.base_provider import BaseMessageQueueProvider


class KafkaLikeProvider(BaseMessageQueueProvider):
    """Provider overriding the full queue-URI dispatch surface."""

    scheme = "kafka"

    def queue_matches(self, queue: str) -> bool:
        return queue.startswith("kafka://")

    def trigger_class(self):
        raise NotImplementedError

    def trigger_kwargs(self, queue: str, **kwargs) -> dict:
        return {"topic": queue}


class SchemeOnlyProvider(BaseMessageQueueProvider):
    """Minimal provider shape used by scheme-based dispatch (only trigger_class implemented)."""

    scheme = "scheme-only"

    def trigger_class(self):
        raise NotImplementedError


class TestContractEnforcement:
    def test_trigger_class_is_the_only_abstract_method(self):
        assert BaseMessageQueueProvider.trigger_class.__isabstractmethod__ is True
        assert getattr(BaseMessageQueueProvider.queue_matches, "__isabstractmethod__", False) is False
        assert getattr(BaseMessageQueueProvider.trigger_kwargs, "__isabstractmethod__", False) is False
        assert getattr(BaseMessageQueueProvider.scheme_matches, "__isabstractmethod__", False) is False

    def test_subclass_without_trigger_class_fails_loudly(self):
        class IncompleteProvider(BaseMessageQueueProvider):
            scheme = "incomplete"

        with pytest.raises(TypeError, match="trigger_class"):
            IncompleteProvider()

    def test_scheme_only_provider_is_instantiable(self):
        assert SchemeOnlyProvider().scheme == "scheme-only"


class TestSchemeMatches:
    @pytest.mark.parametrize(
        ("scheme", "expected"),
        [
            ("kafka", True),
            ("sqs", False),
            ("kafka://", False),
            ("", False),
            (None, False),
        ],
    )
    def test_subclass_matches_only_its_own_scheme(self, scheme, expected):
        assert KafkaLikeProvider().scheme_matches(scheme) is expected

    def test_base_class_scheme_defaults_to_none_and_matches_nothing(self):
        assert BaseMessageQueueProvider.scheme is None
        assert SchemeOnlyProvider.scheme_matches(SchemeOnlyProvider(), "kafka") is False


class TestQueueDispatchDefaults:
    @pytest.mark.parametrize("queue", ["kafka://topic", "redis+pubsub://channel", ""])
    def test_default_queue_matches_matches_nothing(self, queue):
        assert SchemeOnlyProvider().queue_matches(queue) is False

    def test_default_trigger_kwargs_is_empty(self):
        assert SchemeOnlyProvider().trigger_kwargs("kafka://topic") == {}

    def test_overriding_provider_keeps_its_own_dispatch(self):
        provider = KafkaLikeProvider()

        assert provider.queue_matches("kafka://topic") is True
        assert provider.queue_matches("sqs://queue") is False
        assert provider.trigger_kwargs("kafka://topic") == {"topic": "kafka://topic"}
