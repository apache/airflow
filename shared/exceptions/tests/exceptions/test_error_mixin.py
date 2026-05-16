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

from airflow_shared.exceptions import AirflowErrorCodeMixin


class SampleErrorCaseException(AirflowErrorCodeMixin, Exception):
    """Sample class 1 using AirflowErrorCodeMixin for unit tests."""

    pass


class SampleHTTPErrorCaseException(AirflowErrorCodeMixin, Exception):
    """Sample class 2 using AirflowErrorCodeMixin for unit tests."""

    pass


class AirflowSampleAPIErrorCaseException(AirflowErrorCodeMixin, Exception):
    """Sample class 2 using AirflowErrorCodeMixin for unit tests."""

    pass


class AirflowSomeParameterInvalid(AirflowErrorCodeMixin, Exception):
    """Sample class 3 using AirflowErrorCodeMixin for unit tests."""

    pass


class TestAirflowErrorCodeMixin:
    """Tests class for AirflowErrorCodeMixin."""

    def test_str_with_both_code_and_message(self):
        """Check exception string when both message and error code are present in exception."""

        exc = SampleErrorCaseException("Provided cron expression is invalid")
        assert str(exc) == "[AERR-SAMPLE-ERROR-CASE] Provided cron expression is invalid"

    def test_str_without_message(self):
        """Check exception string when no exception message is passed."""

        exc = SampleErrorCaseException()
        assert str(exc) == "[AERR-SAMPLE-ERROR-CASE]"

    def test_str_with_message(self):
        """Check exception string with exception message passed."""

        exc = SampleErrorCaseException("This is a test message")
        assert str(exc) == "[AERR-SAMPLE-ERROR-CASE] This is a test message"

    def test_repr_output(self):
        """Check if repr correctly includes error code."""

        exc = SampleErrorCaseException("fail")
        assert repr(exc), "MyCustomException('fail', error_code=AERR-SAMPLE-ERROR-CASE)"

    def test_str_with_other_excs(self):
        """Check exception string when both message and error code are present in exception."""

        exc = SampleHTTPErrorCaseException("Some http exception")
        assert exc.error_code == "AERR-SAMPLE-HTTP-ERROR-CASE"
        assert str(exc) == "[AERR-SAMPLE-HTTP-ERROR-CASE] Some http exception"

        exc = AirflowSampleAPIErrorCaseException("Some api exception")
        assert exc.error_code == "AERR-SAMPLE-API-ERROR-CASE"
        assert str(exc) == "[AERR-SAMPLE-API-ERROR-CASE] Some api exception"

        exc = AirflowSomeParameterInvalid("Some parameter is invalid")
        assert exc.error_code == "AERR-SOME-PARAMETER-INVALID"
        assert str(exc) == "[AERR-SOME-PARAMETER-INVALID] Some parameter is invalid"
