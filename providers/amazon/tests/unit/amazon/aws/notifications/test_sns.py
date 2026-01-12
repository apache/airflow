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
from airflow.providers.amazon.version_compat import NOTSET

PUBLISH_KWARGS = {
    "target_arn": "arn:aws:sns:us-west-2:123456789098:TopicName",
    "message": "foo-bar",
    "subject": "spam-egg",
    "message_attributes": {},
}


class TestSnsNotifier:
    def test_class_and_notifier_are_same(self):
        assert send_sns_notification is SnsNotifier

    @pytest.mark.parametrize(
        "aws_conn_id",
        [
            pytest.param("aws_test_conn_id", id="custom-conn"),
            pytest.param(None, id="none-conn"),
            pytest.param(NOTSET, id="default-value"),
        ],
    )
    @pytest.mark.parametrize(
        "region_name",
        [
            pytest.param("eu-west-2", id="custom-region"),
            pytest.param(None, id="no-region"),
            pytest.param(NOTSET, id="default-value"),
        ],
    )
    def test_parameters_propagate_to_hook(self, aws_conn_id, region_name):
        """Test notifier attributes propagate to SnsHook."""

        notifier_kwargs = {}
        if aws_conn_id is not NOTSET:
            notifier_kwargs["aws_conn_id"] = aws_conn_id
        if region_name is not NOTSET:
            notifier_kwargs["region_name"] = region_name

        notifier = SnsNotifier(**notifier_kwargs, **PUBLISH_KWARGS)
        with mock.patch("airflow.providers.amazon.aws.notifications.sns.SnsHook") as mock_hook:
            hook = notifier.hook
            assert hook is notifier.hook, "Hook property not cached"
            mock_hook.assert_called_once_with(
                aws_conn_id=(aws_conn_id if aws_conn_id is not NOTSET else "aws_default"),
                region_name=(region_name if region_name is not NOTSET else None),
            )

            # Basic check for notifier
            notifier.notify({})
            mock_hook.return_value.publish_to_target.assert_called_once_with(**PUBLISH_KWARGS)

    @pytest.mark.asyncio
    async def test_async_notify(self):
        notifier = SnsNotifier(**PUBLISH_KWARGS)
        with mock.patch("airflow.providers.amazon.aws.notifications.sns.SnsHook") as mock_hook:
            mock_hook.return_value.apublish_to_target = mock.AsyncMock()

            await notifier.async_notify({})

            mock_hook.return_value.apublish_to_target.assert_called_once_with(**PUBLISH_KWARGS)

    def test_sns_notifier_templated(self, create_dag_without_db):
        notifier = SnsNotifier(
            aws_conn_id="{{ dag.dag_id }}",
            target_arn="arn:aws:sns:{{ var_region }}:{{ var_account }}:{{ var_topic }}",
            message="I, {{ var_username }}",
            subject="{{ var_subject }}",
            message_attributes={"foo": "{{ dag.dag_id }}"},
            region_name="{{ var_region }}",
        )
        with mock.patch("airflow.providers.amazon.aws.notifications.sns.SnsHook") as m:
            notifier(
                {
                    "dag": create_dag_without_db("test_sns_notifier_templated"),
                    "var_username": "Robot",
                    "var_region": "us-west-1",
                    "var_account": "000000000000",
                    "var_topic": "AwesomeTopic",
                    "var_subject": "spam-egg",
                }
            )
            # Hook initialisation
            m.assert_called_once_with(aws_conn_id="test_sns_notifier_templated", region_name="us-west-1")
            # Publish message
            m.return_value.publish_to_target.assert_called_once_with(
                target_arn="arn:aws:sns:us-west-1:000000000000:AwesomeTopic",
                message="I, Robot",
                subject="spam-egg",
                message_attributes={"foo": "test_sns_notifier_templated"},
            )
