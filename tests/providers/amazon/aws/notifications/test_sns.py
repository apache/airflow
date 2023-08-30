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

import pytest

from airflow.providers.amazon.aws.notifications.sns import SnsNotifier, send_sns_notification
from airflow.utils.types import NOTSET

PARAM_DEFAULT_VALUE = pytest.param(NOTSET, id="default-value")


class TestSnsNotifier:
    def test_class_and_notifier_are_same(self):
        assert send_sns_notification is SnsNotifier

    @pytest.mark.parametrize("aws_conn_id", ["aws_test_conn_id", None, PARAM_DEFAULT_VALUE])
    @pytest.mark.parametrize("region_name", ["eu-west-2", None, PARAM_DEFAULT_VALUE])
    def test_parameters_propagate_to_hook(self, aws_conn_id, region_name):
        """Test notifier attributes propagate to SnsHook."""
        notifier_kwargs = {
            "target_arn": "arn:aws:sns:us-west-2:123456789098:TopicName",
            "message": "foo-bar",
        }
        if aws_conn_id is not NOTSET:
            notifier_kwargs["aws_conn_id"] = aws_conn_id
        if region_name is not NOTSET:
            notifier_kwargs["region_name"] = region_name

        notifier = SnsNotifier(**notifier_kwargs)
        with mock.patch("airflow.providers.amazon.aws.notifications.sns.SnsHook") as mock_hook:
            hook = notifier.hook
            assert hook is notifier.hook, "Hook property not cached"
            mock_hook.assert_called_once_with(
                aws_conn_id=(aws_conn_id if aws_conn_id is not NOTSET else "aws_default"),
                region_name=(region_name if region_name is not NOTSET else None),
            )
