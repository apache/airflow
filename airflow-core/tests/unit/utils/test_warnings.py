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

import warnings

import pytest

from airflow.utils.warnings import capture_with_reraise


class TestCaptureWithReraise:
    @staticmethod
    def raise_warnings():
        warnings.warn("Foo", UserWarning, stacklevel=2)
        warnings.warn("Bar", UserWarning, stacklevel=2)
        warnings.warn("Baz", UserWarning, stacklevel=2)

    def test_capture_no_warnings(self):
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            with capture_with_reraise() as cw:
                pass
            assert cw == []

    def test_capture_warnings(self):
        with pytest.warns(UserWarning, match="(Foo|Bar|Baz)") as ctx:
            with capture_with_reraise() as cw:
                self.raise_warnings()
            assert len(cw) == 3
        assert len(ctx.list) == 3

    def test_capture_warnings_with_parent_error_filter(self):
        with warnings.catch_warnings(record=True) as records:
            warnings.filterwarnings("error", message="Bar")
            with capture_with_reraise() as cw:
                with pytest.raises(UserWarning, match="Bar"):
                    self.raise_warnings()
            assert len(cw) == 1
        assert len(records) == 1

    def test_capture_warnings_with_parent_ignore_filter(self):
        with warnings.catch_warnings(record=True) as records:
            warnings.filterwarnings("ignore", message="Baz")
            with capture_with_reraise() as cw:
                self.raise_warnings()
            assert len(cw) == 2
        assert len(records) == 2

    def test_capture_warnings_with_filters(self):
        with warnings.catch_warnings(record=True) as records:
            with capture_with_reraise() as cw:
                warnings.filterwarnings("ignore", message="Foo")
                self.raise_warnings()
            assert len(cw) == 2
        assert len(records) == 2

    def test_capture_warnings_with_error_filters(self):
        with warnings.catch_warnings(record=True) as records:
            with capture_with_reraise() as cw:
                warnings.filterwarnings("error", message="Bar")
                with pytest.raises(UserWarning, match="Bar"):
                    self.raise_warnings()
            assert len(cw) == 1
        assert len(records) == 1
