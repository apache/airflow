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
from __future__ import annotations

from unittest import mock

import pytest

pytest.importorskip("wtforms")  # Remove after upgrading to FAB5


class TestValidJson:
    def setup_method(self):
        self.form_field_mock = mock.MagicMock(data='{"valid":"True"}')
        self.form_field_mock.gettext.side_effect = lambda msg: msg
        self.form_mock = mock.MagicMock(spec_set=dict)

    def _validate(self, message=None):
        from airflow.providers.google.cloud.utils.validators import ValidJson

        validator = ValidJson(message=message)

        return validator(self.form_mock, self.form_field_mock)

    def test_form_field_is_none(self):
        self.form_field_mock.data = None

        assert self._validate() is None

    def test_validation_pass(self):
        assert self._validate() is None

    def test_validation_raises_default_message(self):
        from wtforms.validators import ValidationError

        self.form_field_mock.data = "2017-05-04"

        with pytest.raises(ValidationError, match="JSON Validation Error:.*"):
            self._validate()

    def test_validation_raises_custom_message(self):
        from wtforms.validators import ValidationError

        self.form_field_mock.data = "2017-05-04"

        with pytest.raises(ValidationError, match="Invalid JSON"):
            self._validate(
                message="Invalid JSON: {}",
            )
