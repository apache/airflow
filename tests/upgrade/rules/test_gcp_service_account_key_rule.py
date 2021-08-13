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

from airflow.upgrade.rules.gcp_service_account_keys_rule import GCPServiceAccountKeyRule
from tests.test_utils.config import conf_vars


class TestGCPServiceAccountKeyRule(TestCase):

    @conf_vars({("kubernetes", "gcp_service_account_keys"): "key_name:key_path"})
    def test_invalid_check(self):
        rule = GCPServiceAccountKeyRule()

        assert isinstance(rule.description, str)
        assert isinstance(rule.title, str)

        msg = """This option has been removed because it is no longer \
supported by the Google Kubernetes Engine. The new recommended \
service account keys for the Google Cloud management method is \
Workload Identity."""
        response = rule.check()
        assert response == [msg]

    @conf_vars({("kubernetes", "gcp_service_account_keys"): ""})
    def test_valid_check(self):
        rule = GCPServiceAccountKeyRule()

        assert isinstance(rule.description, str)
        assert isinstance(rule.title, str)

        msg = None
        response = rule.check()
        assert response == msg
