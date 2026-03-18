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

from airflow.providers.amazon.aws.auth_manager.constants import (
    CONF_AVP_POLICY_STORE_ID_KEY,
    CONF_CONN_ID_KEY,
    CONF_REGION_NAME_KEY,
    CONF_SAML_METADATA_URL_KEY,
    CONF_SECTION_NAME,
)


class TestAwsAuthManagerConstants:
    def test_conf_section_name(self):
        assert CONF_SECTION_NAME == "aws_auth_manager"

    def test_conf_conn_id_key(self):
        assert CONF_CONN_ID_KEY == "conn_id"

    def test_conf_region_name_key(self):
        assert CONF_REGION_NAME_KEY == "region_name"

    def test_conf_saml_metadata_url_key(self):
        assert CONF_SAML_METADATA_URL_KEY == "saml_metadata_url"

    def test_conf_avp_policy_store_id_key(self):
        assert CONF_AVP_POLICY_STORE_ID_KEY == "avp_policy_store_id"
