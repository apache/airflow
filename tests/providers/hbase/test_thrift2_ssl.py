#
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

import ssl
from unittest.mock import MagicMock, patch

from airflow.providers.hbase.thrift2_ssl import create_ssl_context


class TestThrift2SSL:
    """Test Thrift2 SSL context creation."""

    def test_create_ssl_context_verify_modes(self):
        """Test different SSL verify modes."""
        config_none = {"ssl_verify_mode": "CERT_NONE"}
        ssl_context, _ = create_ssl_context(config_none)
        assert ssl_context.check_hostname is False
        assert ssl_context.verify_mode == ssl.CERT_NONE
        
        config_optional = {"ssl_verify_mode": "CERT_OPTIONAL"}
        ssl_context, _ = create_ssl_context(config_optional)
        assert ssl_context.verify_mode == ssl.CERT_OPTIONAL
        
        config_required = {"ssl_verify_mode": "CERT_REQUIRED"}
        ssl_context, _ = create_ssl_context(config_required)
        assert ssl_context.verify_mode == ssl.CERT_REQUIRED

    def test_create_ssl_context_default_verify_mode(self):
        """Test default SSL verify mode."""
        config = {}
        ssl_context, _ = create_ssl_context(config)
        
        assert ssl_context.verify_mode == ssl.CERT_REQUIRED

    def test_create_ssl_context_without_secrets(self):
        """Test SSL context creation without secrets."""
        config = {"ssl_verify_mode": "CERT_NONE"}
        
        ssl_context, temp_files = create_ssl_context(config)
        
        assert ssl_context is not None
        assert len(temp_files) == 0  # No temp files created
