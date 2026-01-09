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

"""
Unit tests for InformaticaLineageExtractor covering all methods.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from airflow.models import Connection
from airflow.providers.informatica.extractors.informatica import InformaticaLineageExtractor
from airflow.providers.informatica.hooks.edc import InformaticaEDCHook


@pytest.fixture
def extractor():
    informatica_hook = InformaticaEDCHook(informatica_edc_conn_id="test_conn")
    return InformaticaLineageExtractor(edc_hook=informatica_hook)


@pytest.fixture(autouse=True)
def setup_connections(create_connection_without_db):
    create_connection_without_db(
        Connection(
            conn_id="test_conn",
            conn_type="http",
            host="testhost",
            schema="https",
            port=443,
            login="user",
            password="pass",
            extra='{"security_domain": "domain"}',
        )
    )


@patch("airflow.providers.informatica.hooks.edc.InformaticaEDCHook._request")
def test_get_object(mock_request, extractor):
    """Test get_object delegates to hook and returns result."""
    mock_request.return_value.json.return_value = {"id": "obj1", "name": "test_obj"}
    result = extractor.get_object("obj1")
    mock_request.assert_called_once()
    assert result["id"] == "obj1"
    assert result["name"] == "test_obj"


@patch("airflow.providers.informatica.hooks.edc.InformaticaEDCHook._request")
def test_create_lineage_link(mock_request, extractor):
    """Test create_lineage_link delegates to hook and returns result."""
    mock_request.return_value.json.return_value = {"success": True}
    result = extractor.create_lineage_link("src_id", "tgt_id")
    mock_request.assert_called_once()
    assert result["success"] is True
