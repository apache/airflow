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
This module contains the unit tests for the helper methods included in the Amazon System Tests found at
tests/system/providers/amazon/aws/utils/__init__.py
"""
from __future__ import annotations

import io
import os
import sys
from unittest.mock import ANY, patch

import pytest
from moto import mock_ssm

from tests.system.providers.amazon.aws import utils
from tests.system.providers.amazon.aws.utils import (
    DEFAULT_ENV_ID_LEN,
    DEFAULT_ENV_ID_PREFIX,
    ENV_ID_ENVIRON_KEY,
    INVALID_ENV_ID_MSG,
    LOWERCASE_ENV_ID_MSG,
    NO_VALUE_MSG,
    _validate_env_id,
    set_env_id,
)

TEST_NAME: str = "example_test"
ANY_STR: str = "any"

ENV_VALUE: str = "foo"
SSM_VALUE: str = "bar"
DEFAULT_VALUE: str = "baz"


@pytest.fixture(autouse=True)
def provide_test_name():
    with patch.object(utils, "_get_test_name", return_value=TEST_NAME) as name:
        yield name


@mock_ssm
class TestAmazonSystemTestHelpers:
    FETCH_VARIABLE_TEST_CASES = [
        # Format is:
        # (Environment Variable value, Fetched SSM value, Provided Default value, Expected Result)
        (ENV_VALUE, SSM_VALUE, DEFAULT_VALUE, ENV_VALUE),
        (ENV_VALUE, SSM_VALUE, None, ENV_VALUE),
        (ENV_VALUE, None, DEFAULT_VALUE, ENV_VALUE),
        (ENV_VALUE, None, None, ENV_VALUE),
        (None, SSM_VALUE, DEFAULT_VALUE, SSM_VALUE),
        (None, SSM_VALUE, None, SSM_VALUE),
        (None, None, DEFAULT_VALUE, DEFAULT_VALUE),
        # For the (None, None, None ) test case, see: test_fetch_variable_no_value_found_raises_exception
    ]

    @pytest.mark.parametrize(
        "env_value, ssm_value, default_value, expected_result", FETCH_VARIABLE_TEST_CASES
    )
    @patch.object(os, "getenv")
    def test_fetch_variable_success(
        self, mock_getenv, env_value, ssm_value, default_value, expected_result
    ) -> None:
        mock_getenv.return_value = env_value if env_value else ssm_value

        result = utils.fetch_variable(ANY, default_value) if default_value else utils.fetch_variable(ANY_STR)

        assert result == expected_result

    def test_fetch_variable_no_value_found_raises_exception(self):
        # This would be the (None, None, None) test case from above.
        with pytest.raises(ValueError) as raised_exception:
            utils.fetch_variable(ANY_STR)

            assert NO_VALUE_MSG.format(key=ANY_STR) in str(raised_exception.value)

    ENV_ID_TEST_CASES = [
        # Happy Cases
        ("ABCD", True),
        ("AbCd", True),
        ("abcd", True),
        ("ab12", True),
        # Failure Cases
        # Must be alphanumeric
        ("not_alphanumeric", False),
        # Can not be empty
        ("", False),
        # Must start with a letter
        ("1234", False),
        ("12ab", False),
        ("12AB", False),
        ("12Ab", False),
    ]

    @pytest.mark.parametrize("env_id, is_valid", ENV_ID_TEST_CASES)
    def test_validate_env_id_success(self, env_id, is_valid):
        if is_valid:
            captured_output = io.StringIO()
            sys.stdout = captured_output

            result = _validate_env_id(env_id)
            sys.stdout = sys.__stdout__

            assert result == env_id.lower()
            assert result.isalnum()
            if not result == env_id:
                assert LOWERCASE_ENV_ID_MSG in captured_output.getvalue()
        else:
            with pytest.raises(ValueError) as raised_exception:
                _validate_env_id(env_id)

                assert INVALID_ENV_ID_MSG in str(raised_exception.value)

    def test_set_env_id_generates_if_required(self):
        # No environment variable nor SSM value has been found
        result = set_env_id()

        assert len(result) == DEFAULT_ENV_ID_LEN + len(DEFAULT_ENV_ID_PREFIX)
        assert result.isalnum()
        assert result.islower()

    def test_set_env_id_exports_environment_variable(self):
        env_id = set_env_id()

        assert os.environ[ENV_ID_ENVIRON_KEY] == env_id
