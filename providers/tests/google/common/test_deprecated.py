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

from datetime import date
from unittest import mock

import pytest

from airflow.providers.google.common.deprecated import (
    AirflowDeprecationAdapter,
    deprecated,
)

ADAPTER_PATH = "airflow.providers.google.common.deprecated"
ADAPTER_CLASS_PATH = f"{ADAPTER_PATH}.AirflowDeprecationAdapter"


class TestAirflowDeprecationAdapter:
    @mock.patch(f"{ADAPTER_CLASS_PATH}._validate_fields")
    @mock.patch(f"{ADAPTER_CLASS_PATH}._validate_removal_release")
    @mock.patch(f"{ADAPTER_CLASS_PATH}._validate_date")
    def test_init(
        self, mock_validate_date, mock_validate_removal_release, mock_validate_fields
    ):
        mock_date = mock_validate_date.return_value
        mock_release = mock_validate_removal_release.return_value

        given_date = "August 22, 2024"
        given_release = None
        adapter = AirflowDeprecationAdapter(planned_removal_date=given_date)

        mock_validate_date.assert_called_once_with(given_date)
        mock_validate_removal_release.assert_called_once_with(given_release)
        mock_validate_fields.assert_called_once_with()
        assert adapter.planned_removal_date == mock_date
        assert adapter.planned_removal_release == mock_release

    @mock.patch(f"{ADAPTER_PATH}.datetime")
    def test_validate_date(self, mock_datetime):
        value = "August 22, 2024"
        expected_date = date(2024, 8, 22)
        mock_datetime.strptime.return_value.date.return_value = expected_date

        actual_date = AirflowDeprecationAdapter._validate_date(value)

        assert actual_date == expected_date
        mock_datetime.strptime.assert_called_once_with(value, "%B %d, %Y")

    @mock.patch(f"{ADAPTER_PATH}.datetime")
    def test_validate_date_none(self, mock_datetime):
        value = None

        actual_date = AirflowDeprecationAdapter._validate_date(value)

        assert actual_date is None
        assert not mock_datetime.strptime.called

    @pytest.mark.parametrize(
        "invalid_date",
        [
            "August 55, 2024",
            "NotAugust 22, 2024",
            "2024-08-22",
            "Not a date at all",
        ],
    )
    def test_validate_date_error(self, invalid_date):
        expected_error_message = (
            f"Invalid date '{invalid_date}'. "
            f"The expected format is 'Month DD, YYYY', for example 'August 22, 2024'."
        )
        with pytest.raises(ValueError, match=expected_error_message):
            AirflowDeprecationAdapter(planned_removal_date=invalid_date)

    @pytest.mark.parametrize(
        "release_string",
        [
            "apache-airflow==1.2.3",
            "apache-airflow-providers-test==1.2.3",
        ],
    )
    def test_validate_removal_release(self, release_string):
        assert (
            AirflowDeprecationAdapter._validate_removal_release(release_string)
            == release_string
        )

    def test_validate_removal_release_none(self):
        assert AirflowDeprecationAdapter._validate_removal_release(None) is None

    @pytest.mark.parametrize(
        "release_string",
        [
            "invalid-release-string",
            "apache-airflow==2",
            "apache-airflow-providers-test==2",
        ],
    )
    def test_validate_removal_version_error(self, release_string):
        msg = (
            f"\\`{release_string}\\` must follow the format "
            f"\\'apache-airflow\\(-providers-<name>\\)==<X\\.Y\\.Z>\\'."
        )
        with pytest.raises(ValueError, match=msg):
            AirflowDeprecationAdapter._validate_removal_release(release_string)

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"planned_removal_date": "August 22, 2024"},
            {"planned_removal_release": "apache-airflow-providers-test==1.2.3"},
        ],
    )
    def test_validate_fields(self, kwargs):
        adapter = AirflowDeprecationAdapter(**kwargs)
        assert adapter is not None

    def test_validate_fields_error_both_removal_date_and_release(self):
        error_message = (
            "Only one of two parameters must be set: `planned_removal_date` or 'planned_removal_release'. "
            "You specified both."
        )
        with pytest.raises(ValueError, match=error_message):
            AirflowDeprecationAdapter(
                planned_removal_date="August 22, 2024",
                planned_removal_release="apache-airflow==1.2.3",
            )

    @pytest.mark.parametrize(
        "entity, expected_type",
        [
            (AirflowDeprecationAdapter, "class"),
            (AirflowDeprecationAdapter.get_deprecated_msg, "function (or method)"),
        ],
    )
    def test_entity_type(self, entity, expected_type):
        assert AirflowDeprecationAdapter.entity_type(entity) == expected_type

    @pytest.mark.parametrize(
        "module_path, qualified_name, _str, expected_path",
        [
            (
                "test-module",
                "test-qualified-name",
                "test-str",
                "test-module.test-qualified-name",
            ),
            ("test-module", "", "test-str", "test-module"),
            ("", "test-qualified-name", "test-str", "test-str"),
            ("", "", "test-str", "test-str"),
        ],
    )
    def test_entity_path(self, module_path, qualified_name, _str, expected_path):
        mock_entity = mock.MagicMock(
            __module__=module_path,
            __qualname__=qualified_name,
            __str__=mock.MagicMock(return_value=_str),
        )
        assert AirflowDeprecationAdapter.entity_path(mock_entity) == expected_path

    @pytest.mark.parametrize(
        "planned_removal_date, planned_removal_release, expected_message",
        [
            ("August 22, 2024", None, "after August 22, 2024"),
            (None, "apache-airflow==1.2.3", "since version apache-airflow==1.2.3"),
            (
                None,
                "apache-airflow-providers-test==1.2.3",
                "since version apache-airflow-providers-test==1.2.3",
            ),
            (None, None, "in the future"),
        ],
    )
    def test_sunset_message(
        self, planned_removal_date, planned_removal_release, expected_message
    ):
        adapter = AirflowDeprecationAdapter(
            planned_removal_date=planned_removal_date,
            planned_removal_release=planned_removal_release,
        )
        assert adapter.sunset_message() == expected_message

    @pytest.mark.parametrize(
        "use_instead, expected_message",
        [
            (None, "There is no replacement."),
            ("replacement", "Please use `replacement` instead."),
            ("r1, r2", "Please use `r1`, `r2` instead."),
        ],
    )
    def test_replacement_message(self, use_instead, expected_message):
        adapter = AirflowDeprecationAdapter(use_instead=use_instead)
        assert adapter.replacement_message() == expected_message

    @pytest.mark.parametrize(
        "reason, instructions",
        [
            ("Test reason", "Test instructions"),
            ("Test reason", None),
            (None, "Test instructions"),
            (None, None),
        ],
    )
    @mock.patch(f"{ADAPTER_CLASS_PATH}.entity_type")
    @mock.patch(f"{ADAPTER_CLASS_PATH}.entity_path")
    @mock.patch(f"{ADAPTER_CLASS_PATH}.sunset_message")
    @mock.patch(f"{ADAPTER_CLASS_PATH}.replacement_message")
    def get_deprecated_msg(
        self,
        mock_replacement_message,
        mock_sunset_message,
        mock_entity_path,
        mock_entity_type,
        reason,
        instructions,
    ):
        replacement = mock_replacement_message.return_value
        sunset = mock_sunset_message.return_value
        entity_path = mock_entity_path.return_value
        entity_type = mock_entity_type.return_value

        expected_message = f"The {entity_type} `{entity_path}` is deprecated and will be removed {sunset}. {replacement}"
        if reason:
            expected_message += f" The reason is: {reason}"
        if instructions:
            expected_message += f" Instructions: {instructions}"

        mock_wrapped = mock.MagicMock()
        adapter = AirflowDeprecationAdapter(
            reason=mock_wrapped, instructions=instructions
        )

        assert (
            adapter.get_deprecated_msg(mock.MagicMock(), mock.MagicMock())
            == expected_message
        )
        mock_entity_type.assert_called_once_with(entity=mock_wrapped)
        mock_entity_path.assert_called_once_with(entity=mock_wrapped)
        mock_sunset_message.assert_called_once_with()
        mock_replacement_message.assert_called_once_with()


@mock.patch(f"{ADAPTER_PATH}.standard_deprecated")
def test_deprecated(mock_standard_deprecated):
    mock_planned_removal_date = mock.MagicMock()
    mock_planned_removal_release = mock.MagicMock()
    mock_use_instead = mock.MagicMock()
    mock_reason = mock.MagicMock()
    mock_instructions = mock.MagicMock()
    mock_adapter_cls = mock.MagicMock()
    kwargs = {
        "planned_removal_date": mock_planned_removal_date,
        "planned_removal_release": mock_planned_removal_release,
        "use_instead": mock_use_instead,
        "reason": mock_reason,
        "instructions": mock_instructions,
        "adapter_cls": mock_adapter_cls,
    }
    extra_kwargs = {
        "test1": "test1",
        "test2": "test2",
    }
    extra_args = ["test3", "test4"]

    deprecated(*extra_args, **{**kwargs, **extra_kwargs})

    mock_standard_deprecated.assert_called_once_with(
        *extra_args, **{**kwargs, **extra_kwargs}
    )
