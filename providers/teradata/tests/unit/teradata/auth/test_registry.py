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
"""Tests for TeradataAuthRegistry."""

from __future__ import annotations

import pytest

from airflow.providers.teradata.auth import TeradataAuthRegistry
from airflow.providers.teradata.auth.td2 import TD2AuthMechanism


class TestTeradataAuthRegistry:
    """Test cases for TeradataAuthRegistry."""

    def test_get_mechanism_td2(self):
        """Verify TD2 mechanism can be retrieved."""
        mechanism = TeradataAuthRegistry.get("TD2")
        assert mechanism.mechanism_name == "TD2"

    def test_get_mechanism_ldap(self):
        """Verify LDAP mechanism can be retrieved."""
        mechanism = TeradataAuthRegistry.get("LDAP")
        assert mechanism.mechanism_name == "LDAP"

    def test_get_unregistered_mechanism_raises(self):
        """Verify unregistered mechanism name raises ValueError with supported list."""
        with pytest.raises(ValueError, match="KRB5"):
            TeradataAuthRegistry.get("KRB5")

    def test_register_is_idempotent(self):
        """Verify re-registering an existing mechanism does not raise."""
        TeradataAuthRegistry.register(TD2AuthMechanism())

        assert TeradataAuthRegistry.get("TD2").mechanism_name == "TD2"

    def test_available_mechanisms(self):
        """Verify list of available mechanisms."""
        mechanisms = TeradataAuthRegistry.available_mechanisms()
        assert isinstance(mechanisms, list)
        assert "TD2" in mechanisms
        assert "LDAP" in mechanisms
        assert mechanisms == sorted(mechanisms)

    def test_get_default_mechanism(self):
        """Verify TD2 is the default mechanism."""
        default = TeradataAuthRegistry.get_default()
        assert default.mechanism_name == "TD2"
