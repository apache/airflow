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

from airflow.providers.amazon.aws.utils.suppress import return_on_error


@pytest.mark.db_test
def test_suppress_function(caplog):
    @return_on_error("error")
    def fn(value: str, exc: Exception | None = None) -> str:
        if exc:
            raise exc
        return value

    caplog.set_level("DEBUG", "airflow.providers.amazon.aws.utils.suppress")
    caplog.clear()

    assert fn("no-error") == "no-error"
    assert not caplog.messages

    assert fn("foo", ValueError("boooo")) == "error"
    assert "Encountered error during execution function/method 'fn'" in caplog.messages

    caplog.clear()
    with pytest.raises(SystemExit, match="42"):
        # We do not plan to catch exception which only based on `BaseExceptions`
        fn("bar", SystemExit(42))
    assert not caplog.messages

    # We catch even serious exceptions, e.g. we do not provide mandatory argument here
    assert fn() == "error"
    assert "Encountered error during execution function/method 'fn'" in caplog.messages


def test_suppress_methods():
    class FakeClass:
        @return_on_error("Oops!… I Did It Again")
        def some_method(self, value, exc: Exception | None = None) -> str:
            if exc:
                raise exc
            return value

        @staticmethod
        @return_on_error(0)
        def some_staticmethod(value, exc: Exception | None = None) -> int:
            if exc:
                raise exc
            return value

        @classmethod
        @return_on_error("It's fine")
        def some_classmethod(cls, value, exc: Exception | None = None) -> str:
            if exc:
                raise exc
            return value

    assert FakeClass().some_method("no-error") == "no-error"
    assert FakeClass.some_staticmethod(42) == 42
    assert FakeClass.some_classmethod("really-no-error-here") == "really-no-error-here"

    assert FakeClass().some_method("foo", KeyError("foo")) == "Oops!… I Did It Again"
    assert FakeClass.some_staticmethod(42, RuntimeError("bar")) == 0
    assert FakeClass.some_classmethod("bar", OSError("Windows detected!")) == "It's fine"
