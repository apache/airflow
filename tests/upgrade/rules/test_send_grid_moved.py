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
from unittest import TestCase

from airflow.upgrade.rules.send_grid_moved import SendGridEmailerMovedRule
from tests.test_utils.config import conf_vars


class TestSendGridEmailerMovedRule(TestCase):

    @conf_vars({("email", "email_backend"): "airflow.contrib.utils.sendgrid"})
    def test_invalid_check(self):
        rule = SendGridEmailerMovedRule()

        assert isinstance(rule.description, str)
        assert isinstance(rule.title, str)

        email_provider_path = "airflow.providers.sendgrid.utils.emailer"
        msg = "Email backend option uses airflow.contrib Sendgrid module. " + \
              "Please use new module: {}".format(email_provider_path)
        response = rule.check()
        assert response == [msg]

    @conf_vars({("email", "email_backend"): "airflow.providers.sendgrid.utils.emailer"})
    def test_valid_check(self):
        rule = SendGridEmailerMovedRule()

        assert isinstance(rule.description, str)
        assert isinstance(rule.title, str)

        msg = None
        response = rule.check()
        assert response == msg
