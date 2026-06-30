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
"""Tests for TeradataAuthMechanism base class."""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from airflow.providers.teradata.auth.base import TeradataAuthMechanism


class ConcreteAuthMechanism(TeradataAuthMechanism):
    """Concrete implementation for testing abstract base class."""

    @property
    def mechanism_name(self) -> str:
        return "TEST"

    @property
    def display_name(self) -> str:
        return "Test Mechanism"

    def get_connection_config(self, connection, base_config):
        return base_config.copy()

    def validate_config(self, config):
        pass


class TestTeradataAuthMechanism:
    """Test cases for TeradataAuthMechanism abstract base class."""

    def test_abstract_class_cannot_instantiate(self):
        """Verify TeradataAuthMechanism is abstract and cannot be instantiated."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            TeradataAuthMechanism()

    def test_concrete_implementation_instantiates(self):
        """Verify concrete implementation can be instantiated."""
        mechanism = ConcreteAuthMechanism()
        assert mechanism is not None

    def test_mechanism_name_property(self):
        """Verify mechanism_name property is defined."""
        mechanism = ConcreteAuthMechanism()
        assert mechanism.mechanism_name == "TEST"

    def test_display_name_property(self):
        """Verify display_name property is defined."""
        mechanism = ConcreteAuthMechanism()
        assert mechanism.display_name == "Test Mechanism"

    def test_get_connection_config_method(self):
        """Verify get_connection_config method is callable."""
        mechanism = ConcreteAuthMechanism()
        connection = Mock(spec_set=[])
        base_config = {"host": "localhost"}

        result = mechanism.get_connection_config(connection, base_config)

        assert result == base_config

    def test_validate_config_method(self):
        """Verify validate_config method is callable."""
        mechanism = ConcreteAuthMechanism()
        config = {"host": "localhost"}

        mechanism.validate_config(config)
