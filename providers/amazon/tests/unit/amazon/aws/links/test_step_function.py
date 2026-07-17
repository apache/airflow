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

from airflow.providers.amazon.aws.links.step_function import (
    StateMachineDetailsLink,
    StateMachineExecutionsDetailsLink,
)

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS
from unit.amazon.aws.links.test_base_aws import BaseAwsLinksTestCase

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk.execution_time.comms import XComResult

pytestmark = pytest.mark.db_test


class TestStateMachineDetailsLink(BaseAwsLinksTestCase):
    link_class = StateMachineDetailsLink

    @pytest.mark.parametrize(
        ("state_machine_arn", "expected_url"),
        [
            pytest.param("", "", id="empty-arn"),
            pytest.param(None, "", id="arn-not-set"),
            pytest.param(
                "foo:bar",
                "https://console.aws.amazon.com/states/home?region=eu-west-1#/statemachines/view/foo%3Abar",
                id="arn-set",
            ),
        ],
    )
    def test_extra_link(self, state_machine_arn, expected_url: str, mock_supervisor_comms):
        if AIRFLOW_V_3_0_PLUS and mock_supervisor_comms:
            mock_supervisor_comms.send.return_value = XComResult(
                key=self.link_class.key,
                value={
                    "region_name": "eu-west-1",
                    "aws_domain": self.link_class.get_aws_domain("aws"),
                    "aws_partition": "aws",
                    "state_machine_arn": state_machine_arn,
                },
            )
        self.assert_extra_link_url(
            expected_url=expected_url,
            region_name="eu-west-1",
            aws_partition="aws",
            state_machine_arn=state_machine_arn,
        )


class TestStateMachineExecutionsDetailsLink(BaseAwsLinksTestCase):
    link_class = StateMachineExecutionsDetailsLink

    @pytest.mark.parametrize(
        ("execution_arn", "expected_url"),
        [
            pytest.param("", "", id="empty-arn"),
            pytest.param(None, "", id="arn-not-set"),
            pytest.param(
                "spam:egg:000000000",
                "https://console.aws.amazon.com/states/home?region=eu-west-1"
                "#/v2/executions/details/spam%3Aegg%3A000000000",
                id="arn-set",
            ),
        ],
    )
    def test_extra_link(self, execution_arn, expected_url: str, mock_supervisor_comms):
        if AIRFLOW_V_3_0_PLUS and mock_supervisor_comms:
            mock_supervisor_comms.send.return_value = XComResult(
                key=self.link_class.key,
                value={
                    "region_name": "eu-west-1",
                    "aws_domain": self.link_class.get_aws_domain("aws"),
                    "aws_partition": "aws",
                    "execution_arn": execution_arn,
                },
            )
        self.assert_extra_link_url(
            expected_url=expected_url,
            region_name="eu-west-1",
            aws_partition="aws",
            execution_arn=execution_arn,
        )
