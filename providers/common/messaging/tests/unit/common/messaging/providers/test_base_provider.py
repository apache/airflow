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
    """Minimal complete provider used to exercise the base-class contract."""

    scheme = "kafka"

    def queue_matches(self, queue: str) -> bool:
        return queue.startswith("kafka://")

    def trigger_class(self):
        raise NotImplementedError

    def trigger_kwargs(self, queue: str, **kwargs) -> dict:
        return {}


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
        assert KafkaLikeProvider.scheme_matches(BaseMessageQueueProvider(), "kafka") is False


@pytest.mark.parametrize(
    "method_name",
    [
        "queue_matches",
        "trigger_class",
        "trigger_kwargs",
    ],
)
def test_provider_contract_methods_are_marked_abstract(method_name):
    assert getattr(BaseMessageQueueProvider, method_name).__isabstractmethod__ is True


def test_scheme_matches_is_part_of_the_concrete_surface():
    assert getattr(BaseMessageQueueProvider.scheme_matches, "__isabstractmethod__", False) is False
