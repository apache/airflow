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

import pytest

from airflowctl.exceptions import (
    AirflowCtlConnectionException,
    AirflowCtlCredentialNotFoundException,
    AirflowCtlException,
    AirflowCtlKeyringException,
    AirflowCtlNotFoundException,
)


def test_exceptions_follow_expected_hierarchy():
    assert issubclass(AirflowCtlNotFoundException, AirflowCtlException)
    assert issubclass(AirflowCtlCredentialNotFoundException, AirflowCtlNotFoundException)
    assert issubclass(AirflowCtlCredentialNotFoundException, AirflowCtlException)
    assert issubclass(AirflowCtlConnectionException, AirflowCtlException)
    assert issubclass(AirflowCtlKeyringException, AirflowCtlException)


@pytest.mark.parametrize(
    "exception_type",
    [
        AirflowCtlException,
        AirflowCtlNotFoundException,
        AirflowCtlCredentialNotFoundException,
        AirflowCtlConnectionException,
        AirflowCtlKeyringException,
    ],
)
def test_exceptions_can_be_raised_and_caught(exception_type):
    with pytest.raises(exception_type):
        raise exception_type("failure")
