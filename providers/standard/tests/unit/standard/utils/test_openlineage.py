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

from unittest.mock import MagicMock, patch

import pytest

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.providers.standard.utils.openlineage import (
    OPENLINEAGE_PROVIDER_MIN_VERSION,
    _get_openlineage_parent_info,
    _inject_openlineage_parent_info_to_dagrun_conf,
    _is_openlineage_provider_accessible,
    safe_inject_openlineage_properties_into_dagrun_conf,
)

OL_UTILS_PATH = "airflow.providers.standard.utils.openlineage"
OL_PROVIDER_PATH = "airflow.providers.openlineage"
OL_MACROS_PATH = f"{OL_PROVIDER_PATH}.plugins.macros"
OL_CONF_PATH = f"{OL_PROVIDER_PATH}.conf"
OL_LISTENER_PATH = f"{OL_PROVIDER_PATH}.plugins.listener"
IMPORTLIB_VERSION = "importlib.metadata.version"


@patch(f"{OL_LISTENER_PATH}.get_openlineage_listener")
@patch(f"{OL_CONF_PATH}.is_disabled")
def test_is_openlineage_provider_accessible(mock_is_disabled, mock_get_listener):
    mock_is_disabled.return_value = False
    mock_get_listener.return_value = True
    assert _is_openlineage_provider_accessible() is True


@patch(f"{OL_LISTENER_PATH}.get_openlineage_listener")
@patch(f"{OL_CONF_PATH}.is_disabled")
def test_is_openlineage_provider_disabled(mock_is_disabled, mock_get_listener):
    mock_is_disabled.return_value = True
    assert _is_openlineage_provider_accessible() is False


@patch(f"{OL_LISTENER_PATH}.get_openlineage_listener")
@patch(f"{OL_CONF_PATH}.is_disabled")
def test_is_openlineage_listener_not_found(mock_is_disabled, mock_get_listener):
    mock_is_disabled.return_value = False
    mock_get_listener.return_value = None
    assert _is_openlineage_provider_accessible() is False


@patch(f"{OL_CONF_PATH}.is_disabled")
def test_is_openlineage_provider_accessible_import_error(mock_is_disabled):
    """Test that ImportError is handled when OpenLineage modules cannot be imported."""
    mock_is_disabled.side_effect = RuntimeError("Should not be called.")
    with patch.dict(
        "sys.modules",
        {
            OL_CONF_PATH: None,
            OL_LISTENER_PATH: None,
        },
    ):
        result = _is_openlineage_provider_accessible()
        assert result is False


def _mock_ol_parent_info():
    """Create a mock OpenLineage parent info dict."""
    return {
        "parentRunId": "test-run-id",
        "parentJobName": "test-job",
        "parentJobNamespace": "test-ns",
        "rootParentRunId": "test-root-run-id",
        "rootParentJobName": "test-root-job",
        "rootParentJobNamespace": "test-root-ns",
    }


def test_get_openlineage_parent_info():
    """Test that _get_openlineage_parent_info calls all macros correctly and returns proper structure."""
    ti = MagicMock()
    expected_values = {
        "parentRunId": "parent-run-id-123",
        "parentJobName": "parent-job-name",
        "parentJobNamespace": "parent-namespace",
        "rootParentRunId": "root-run-id-456",
        "rootParentJobName": "root-job-name",
        "rootParentJobNamespace": "root-namespace",
    }

    def _mock_version(package):
        if package == "apache-airflow-providers-openlineage":
            return OPENLINEAGE_PROVIDER_MIN_VERSION  # Exactly minimum version
        raise Exception(f"Unexpected package: {package}")

    with (
        patch(f"{OL_MACROS_PATH}.lineage_run_id", return_value=expected_values["parentRunId"]) as mock_run_id,
        patch(
            f"{OL_MACROS_PATH}.lineage_job_name", return_value=expected_values["parentJobName"]
        ) as mock_job_name,
        patch(
            f"{OL_MACROS_PATH}.lineage_job_namespace",
            return_value=expected_values["parentJobNamespace"],
        ) as mock_job_namespace,
        patch(
            f"{OL_MACROS_PATH}.lineage_root_run_id",
            return_value=expected_values["rootParentRunId"],
        ) as mock_root_run_id,
        patch(
            f"{OL_MACROS_PATH}.lineage_root_job_name",
            return_value=expected_values["rootParentJobName"],
        ) as mock_root_job_name,
        patch(
            f"{OL_MACROS_PATH}.lineage_root_job_namespace",
            return_value=expected_values["rootParentJobNamespace"],
        ) as mock_root_job_namespace,
        patch(IMPORTLIB_VERSION, side_effect=_mock_version),
    ):
        result = _get_openlineage_parent_info(ti)

        # Verify all macros were called correctly
        mock_run_id.assert_called_once_with(ti)
        mock_job_name.assert_called_once_with(ti)
        mock_job_namespace.assert_called_once()  # No args
        mock_root_run_id.assert_called_once_with(ti)
        mock_root_job_name.assert_called_once_with(ti)
        mock_root_job_namespace.assert_called_once_with(ti)

        # Verify result structure
        assert isinstance(result, dict)
        assert result == expected_values
        assert set(result.keys()) == {
            "parentRunId",
            "parentJobName",
            "parentJobNamespace",
            "rootParentRunId",
            "rootParentJobName",
            "rootParentJobNamespace",
        }


@pytest.mark.parametrize(
    ("provider_version", "should_raise"),
    [
        ("2.7.0", True),  # Below minimum
        ("2.7.9", True),  # Below minimum
        ("2.8.0", False),  # Exactly minimum
        ("2.8.1", False),  # Above minimum
    ],
)
def test_get_openlineage_parent_info_version_check(provider_version, should_raise):
    """Test that _get_openlineage_parent_info raises AirflowOptionalProviderFeatureException when version is insufficient."""
    ti = MagicMock()

    def _mock_version(package):
        if package == "apache-airflow-providers-openlineage":
            return provider_version
        raise Exception(f"Unexpected package: {package}")

    with patch(IMPORTLIB_VERSION, side_effect=_mock_version):
        if should_raise:
            expected_err = (
                f"OpenLineage provider version `{provider_version}` is lower than "
                f"required `{OPENLINEAGE_PROVIDER_MIN_VERSION}`, "
                "skipping function `_get_openlineage_parent_info` execution"
            )
            with pytest.raises(AirflowOptionalProviderFeatureException, match=expected_err):
                _get_openlineage_parent_info(ti)
        else:
            # When version is sufficient, mock all macros to allow execution
            with (
                patch(f"{OL_MACROS_PATH}.lineage_run_id", return_value="run-id"),
                patch(f"{OL_MACROS_PATH}.lineage_job_name", return_value="job-name"),
                patch(f"{OL_MACROS_PATH}.lineage_job_namespace", return_value="job-ns"),
                patch(f"{OL_MACROS_PATH}.lineage_root_run_id", return_value="root-run-id"),
                patch(f"{OL_MACROS_PATH}.lineage_root_job_name", return_value="root-job-name"),
                patch(f"{OL_MACROS_PATH}.lineage_root_job_namespace", return_value="root-job-ns"),
            ):
                result = _get_openlineage_parent_info(ti)
                assert isinstance(result, dict)
                assert "parentRunId" in result


@pytest.mark.parametrize("dr_conf", [None, {}])
def test_inject_parent_info_with_none_or_empty_conf(dr_conf):
    """Test injection with None or empty dict creates new openlineage section."""
    result = _inject_openlineage_parent_info_to_dagrun_conf(dr_conf, _mock_ol_parent_info())
    expected = {"openlineage": _mock_ol_parent_info()}
    assert result == expected


@pytest.mark.parametrize("dr_conf", ["conf as string", ["conf_list"], [{"a": 1}, {"b": 2}]])
def test_inject_parent_info_with_wrong_type_conf_raises_error(dr_conf):
    """Test injection with wrong type of conf raises error (we catch it later on)."""
    with pytest.raises(TypeError):
        _inject_openlineage_parent_info_to_dagrun_conf(dr_conf, _mock_ol_parent_info())


def test_inject_parent_info_with_existing_conf_no_openlineage_key():
    """Test injection with existing conf but no openlineage key."""
    dr_conf = {"some": "other", "config": "value"}
    result = _inject_openlineage_parent_info_to_dagrun_conf(dr_conf, _mock_ol_parent_info())

    expected = {
        "some": "other",
        "config": "value",
        "openlineage": _mock_ol_parent_info(),
    }
    assert result == expected
    # Original conf should not be modified
    assert dr_conf == {"some": "other", "config": "value"}


def test_inject_parent_info_with_existing_openlineage_dict():
    """Test injection with existing openlineage dict merges correctly."""
    dr_conf = {
        "some": "other",
        "openlineage": {
            "existing": "value",
            "otherKey": "otherValue",
        },
    }
    result = _inject_openlineage_parent_info_to_dagrun_conf(dr_conf, _mock_ol_parent_info())

    expected = {
        "some": "other",
        "openlineage": {
            "existing": "value",
            "otherKey": "otherValue",
            **_mock_ol_parent_info(),
        },
    }
    assert result == expected
    # Original conf should not be modified
    assert dr_conf == {
        "some": "other",
        "openlineage": {
            "existing": "value",
            "otherKey": "otherValue",
        },
    }


def test_inject_parent_info_with_non_dict_openlineage_returns_unchanged():
    """Test that non-dict openlineage value returns conf unchanged."""
    dr_conf = {"openlineage": "not-a-dict"}
    result = _inject_openlineage_parent_info_to_dagrun_conf(dr_conf, _mock_ol_parent_info())

    assert result == dr_conf
    assert result is dr_conf  # Should return same object


@pytest.mark.parametrize(
    "forbidden_key",
    [
        "parentRunId",
        "parentJobName",
        "parentJobNamespace",
        "rootParentRunId",
        "rootJobName",
        "rootJobNamespace",
    ],
)
def test_inject_parent_info_with_existing_forbidden_key_returns_unchanged(forbidden_key):
    """Test that existing forbidden keys prevent injection."""
    dr_conf = {
        "openlineage": {
            forbidden_key: "existing-value",
            "otherKey": "value",
        }
    }
    result = _inject_openlineage_parent_info_to_dagrun_conf(dr_conf, _mock_ol_parent_info())

    assert result == dr_conf
    assert result is dr_conf  # Should return same object


def test_inject_parent_info_with_multiple_existing_keys_returns_unchanged():
    """Test that multiple existing forbidden keys are all detected."""
    dr_conf = {
        "openlineage": {
            "parentRunId": "existing-parent-id",
            "rootParentJobName": "existing-root-job",
            "otherKey": "value",
        }
    }
    result = _inject_openlineage_parent_info_to_dagrun_conf(dr_conf, _mock_ol_parent_info())
    assert result == dr_conf
    # Original conf should not be modified
    assert dr_conf == {
        "openlineage": {
            "parentRunId": "existing-parent-id",
            "rootParentJobName": "existing-root-job",
            "otherKey": "value",
        }
    }


def test_safe_inject_returns_unchanged_when_provider_not_accessible():
    """Test returns original conf when OpenLineage provider is not accessible."""
    dr_conf = {"some": "config"}

    with patch(f"{OL_UTILS_PATH}._is_openlineage_provider_accessible", return_value=False):
        result = safe_inject_openlineage_properties_into_dagrun_conf(dr_conf, MagicMock())

        assert result == dr_conf
        assert result is dr_conf  # Should return same object


def test_safe_inject_correctly_injects_openlineage_info():
    """Test that OpenLineage injection works when OL is available and version is sufficient."""
    dr_conf = {"some": "config"}
    expected_result = {
        "some": "config",
        "openlineage": _mock_ol_parent_info(),
    }

    def _mock_version(package):
        if package == "apache-airflow-providers-openlineage":
            return OPENLINEAGE_PROVIDER_MIN_VERSION
        raise Exception(f"Unexpected package: {package}")

    with (
        patch(f"{OL_UTILS_PATH}._is_openlineage_provider_accessible", return_value=True),
        patch(IMPORTLIB_VERSION, side_effect=_mock_version),
        patch(f"{OL_UTILS_PATH}._get_openlineage_parent_info", return_value=_mock_ol_parent_info()),
    ):
        result = safe_inject_openlineage_properties_into_dagrun_conf(dr_conf, MagicMock())

        assert result == expected_result


@pytest.mark.parametrize("dr_conf", [None, {}, "not-a-dict", ["a", "b", "c"]])
def test_safe_inject_handles_none_empty_and_non_dict_conf(dr_conf):
    """Test handles None, empty dict, or non-dict conf without raising error."""

    def _mock_version(package):
        if package == "apache-airflow-providers-openlineage":
            return OPENLINEAGE_PROVIDER_MIN_VERSION
        raise Exception(f"Unexpected package: {package}")

    with (
        patch(f"{OL_UTILS_PATH}._is_openlineage_provider_accessible", return_value=True),
        patch(IMPORTLIB_VERSION, side_effect=_mock_version),
        patch(f"{OL_UTILS_PATH}._get_openlineage_parent_info", return_value=_mock_ol_parent_info()),
    ):
        result = safe_inject_openlineage_properties_into_dagrun_conf(dr_conf, MagicMock())

        if dr_conf is None or isinstance(dr_conf, dict):
            assert result == {"openlineage": _mock_ol_parent_info()}
        else:
            assert result == dr_conf
            assert result is dr_conf


def test_safe_inject_copies_dict_before_passing():
    """Test that dict is copied before being passed to inject function."""
    dr_conf = {"some": "config", "nested": {"key": "value"}}

    with (
        patch(f"{OL_UTILS_PATH}._is_openlineage_provider_accessible", return_value=True),
        patch(f"{OL_UTILS_PATH}._get_openlineage_parent_info", return_value=_mock_ol_parent_info()),
        patch(f"{OL_UTILS_PATH}._inject_openlineage_parent_info_to_dagrun_conf") as mock_inject,
    ):
        expected_result = {**dr_conf, "openlineage": _mock_ol_parent_info()}
        mock_inject.return_value = expected_result
        safe_inject_openlineage_properties_into_dagrun_conf(dr_conf, MagicMock())

        # Verify that a copy was passed (not the original)
        mock_inject.assert_called_once()
        call_args = mock_inject.call_args
        passed_conf = call_args[1]["dr_conf"]  # Keyword argument
        assert passed_conf == dr_conf
        # The copy should be a different object (shallow copy)
        assert passed_conf is not dr_conf


@pytest.mark.parametrize(
    "exception", [ValueError("Test error"), KeyError("Missing key"), RuntimeError("Runtime issue")]
)
def test_safe_inject_preserves_original_conf_on_exception(exception):
    """Test that original conf is preserved when any exception occurs during injection."""
    dr_conf = {"key": "value", "nested": {"deep": "data"}}
    ti = MagicMock()

    with (
        patch(f"{OL_UTILS_PATH}._is_openlineage_provider_accessible", return_value=True),
        patch(f"{OL_UTILS_PATH}._get_openlineage_parent_info", side_effect=exception),
    ):
        result = safe_inject_openlineage_properties_into_dagrun_conf(dr_conf, ti)

        # Should return original conf unchanged
        assert result == {"key": "value", "nested": {"deep": "data"}}
        assert result is dr_conf  # Should return same object


@pytest.mark.parametrize(
    ("provider_version", "should_raise"),
    [
        ("2.7.0", True),  # Below minimum
        ("2.7.9", True),  # Below minimum
        ("2.8.0", False),  # Exactly minimum
        ("2.8.1", False),  # Above minimum
        ("3.0.0", False),  # Well above minimum
    ],
)
def test_safe_inject_with_provider_version_check(provider_version, should_raise):
    """Test that version checking works correctly - exception caught when insufficient, works when sufficient."""
    dr_conf = {"some": "config"}
    ti = MagicMock()
    ol_parent_info = _mock_ol_parent_info()

    def _mock_version(package):
        if package == "apache-airflow-providers-openlineage":
            return provider_version
        raise Exception(f"Unexpected package: {package}")

    with (
        patch(f"{OL_UTILS_PATH}._is_openlineage_provider_accessible", return_value=True),
        patch(IMPORTLIB_VERSION, side_effect=_mock_version),
    ):
        if should_raise:
            # When version is insufficient, _get_openlineage_parent_info will raise
            # The exception should be caught and conf returned unchanged
            result = safe_inject_openlineage_properties_into_dagrun_conf(dr_conf, ti)

            assert result == dr_conf
        else:
            # When version is sufficient, mock _get_openlineage_parent_info to return data
            with patch(f"{OL_UTILS_PATH}._get_openlineage_parent_info", return_value=ol_parent_info):
                result = safe_inject_openlineage_properties_into_dagrun_conf(dr_conf, ti)
                expected = {
                    "some": "config",
                    "openlineage": ol_parent_info,
                }
                assert result == expected


def test_inject_when_provider_not_found():
    """Test that injection handles case when OpenLineage provider package is not found."""
    dr_conf = {"some": "config"}
    ti = MagicMock()

    # Simulate the case where _get_openlineage_parent_info raises AirflowOptionalProviderFeatureException
    # because the provider package is not found (this happens inside require_openlineage_version decorator)
    with (
        patch(f"{OL_UTILS_PATH}._is_openlineage_provider_accessible", return_value=True),
        patch(
            f"{OL_UTILS_PATH}._get_openlineage_parent_info",
            side_effect=AirflowOptionalProviderFeatureException(
                "OpenLineage provider not found or has no version, "
                "skipping function `_get_openlineage_parent_info` execution"
            ),
        ),
    ):
        result = safe_inject_openlineage_properties_into_dagrun_conf(dr_conf, ti)

        assert result == dr_conf
