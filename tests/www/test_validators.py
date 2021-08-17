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

import unittest
from unittest import mock

import pytest

from airflow.www import validators


class TestGreaterEqualThan(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.form_field_mock = mock.MagicMock(data='2017-05-06')
        self.form_field_mock.gettext.side_effect = lambda msg: msg
        self.other_field_mock = mock.MagicMock(data='2017-05-05')
        self.other_field_mock.gettext.side_effect = lambda msg: msg
        self.other_field_mock.label.text = 'other field'
        self.form_stub = {'other_field': self.other_field_mock}
        self.form_mock = mock.MagicMock(spec_set=dict)
        self.form_mock.__getitem__.side_effect = self.form_stub.__getitem__

    def _validate(self, fieldname=None, message=None):
        if fieldname is None:
            fieldname = 'other_field'

        validator = validators.GreaterEqualThan(fieldname=fieldname, message=message)

        return validator(self.form_mock, self.form_field_mock)

    def test_field_not_found(self):
        with pytest.raises(validators.ValidationError, match="^Invalid field name 'some'.$"):
            self._validate(
                fieldname='some',
            )

    def test_form_field_is_none(self):
        self.form_field_mock.data = None

        assert self._validate() is None

    def test_other_field_is_none(self):
        self.other_field_mock.data = None

        assert self._validate() is None

    def test_both_fields_are_none(self):
        self.form_field_mock.data = None
        self.other_field_mock.data = None

        assert self._validate() is None

    def test_validation_pass(self):
        assert self._validate() is None

    def test_validation_raises(self):
        self.form_field_mock.data = '2017-05-04'

        with pytest.raises(
            validators.ValidationError, match="^Field must be greater than or equal to other field.$"
        ):
            self._validate()

    def test_validation_raises_custom_message(self):
        self.form_field_mock.data = '2017-05-04'

        with pytest.raises(
            validators.ValidationError, match="^This field must be greater than or equal to MyField.$"
        ):
            self._validate(
                message="This field must be greater than or equal to MyField.",
            )


class TestValidJson(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.form_field_mock = mock.MagicMock(data='{"valid":"True"}')
        self.form_field_mock.gettext.side_effect = lambda msg: msg
        self.form_mock = mock.MagicMock(spec_set=dict)

    def _validate(self, message=None):

        validator = validators.ValidJson(message=message)

        return validator(self.form_mock, self.form_field_mock)

    def test_form_field_is_none(self):
        self.form_field_mock.data = None

        assert self._validate() is None

    def test_validation_pass(self):
        assert self._validate() is None

    def test_validation_raises_default_message(self):
        self.form_field_mock.data = '2017-05-04'

        with pytest.raises(validators.ValidationError, match="JSON Validation Error:.*"):
            self._validate()

    def test_validation_raises_custom_message(self):
        self.form_field_mock.data = '2017-05-04'

        with pytest.raises(validators.ValidationError, match="Invalid JSON"):
            self._validate(
                message="Invalid JSON: {}",
            )
