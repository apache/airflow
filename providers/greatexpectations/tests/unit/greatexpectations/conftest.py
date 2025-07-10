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
from typing import Generator
from unittest.mock import Mock

import pytest
from great_expectations.expectations import Expectation
from pytest_mock import MockerFixture


@pytest.fixture
def mock_gx(mocker: MockerFixture) -> Generator[Mock, None, None]:
    """Due to constraints from Airflow, GX must be imported locally
    within the Operator, which makes mocking the GX namespace difficult.
    This fixture allows us to globally patch GX.

    One known issue with this approach is that isinstance checks fail against
    mocks.
    """
    mock_gx = Mock()
    mock_gx.expectations.Expectation = Expectation  # required for isinstance check
    mocker.patch.dict("sys.modules", {"great_expectations": mock_gx})
    yield mock_gx
