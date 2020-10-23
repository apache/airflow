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

from tests.compat import mock

from airflow.upgrade.problem import RuleStatus


class TestRuleStatus:
    def test_is_success(self):
        assert RuleStatus(rule=mock.MagicMock(), messages=[]).is_success is True
        assert RuleStatus(rule=mock.MagicMock(), messages=["aaa"]).is_success is False

    def test_rule_status_from_rule(self):
        msgs = ["An interesting problem to solve"]
        rule = mock.MagicMock()
        rule.check.return_value = msgs

        result = RuleStatus.from_rule(rule)
        rule.check.assert_called_once_with()
        assert result == RuleStatus(rule, msgs)
