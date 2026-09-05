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

from contextlib import nullcontext
from typing import Literal
from unittest.mock import MagicMock, patch

import pytest

from airflow.sdk.definitions.param import Param, ParamsDict, process_params
from airflow.sdk.exceptions import ParamValidationError
from airflow.serialization.definitions.param import SerializedParam
from airflow.serialization.serialized_objects import BaseSerialization


class TestParam:
    def test_param_without_schema(self):
        p = Param("test")
        assert p.resolve() == "test"

        p.value = 10
        assert p.resolve() == 10

    def test_null_param(self):
        p = Param()
        with pytest.raises(ParamValidationError, match="No value passed and Param has no default value"):
            p.resolve()
        assert p.resolve(None) is None
        assert p.dump()["value"] is None
        assert not p.has_value

        p = Param(None)
        assert p.resolve() is None
        assert p.resolve(None) is None
        assert p.dump()["value"] is None
        assert not p.has_value

        p = Param(None, type="null")
        assert p.resolve() is None
        assert p.resolve(None) is None
        assert p.dump()["value"] is None
        assert not p.has_value
        with pytest.raises(ParamValidationError):
            p.resolve("test")

    def test_string_param(self):
        p = Param("test", type="string")
        assert p.resolve() == "test"

        p = Param("test")
        assert p.resolve() == "test"

        p = Param("10.0.0.0", type="string", format="ipv4")
        assert p.resolve() == "10.0.0.0"

        p = Param(type="string")
        with pytest.raises(ParamValidationError):
            p.resolve(None)
        with pytest.raises(ParamValidationError, match="No value passed and Param has no default value"):
            p.resolve()

    @pytest.mark.parametrize(
        "dt",
        [
            pytest.param("2022-01-02T03:04:05.678901Z", id="microseconds-zed-timezone"),
            pytest.param("2022-01-02T03:04:05.678Z", id="milliseconds-zed-timezone"),
            pytest.param("2022-01-02T03:04:05+00:00", id="seconds-00-00-timezone"),
            pytest.param("2022-01-02T03:04:05+04:00", id="seconds-custom-timezone"),
        ],
    )
    def test_string_rfc3339_datetime_format(self, dt):
        """Test valid rfc3339 datetime."""
        assert Param(dt, type="string", format="date-time").resolve() == dt

    @pytest.mark.parametrize(
        "dt",
        [
            pytest.param("2022-01-02", id="date"),
            pytest.param("03:04:05", id="time"),
            pytest.param("Thu, 04 Mar 2021 05:06:07 GMT", id="rfc2822-datetime"),
        ],
    )
    def test_string_datetime_invalid_format(self, dt):
        """Test invalid iso8601 and rfc3339 datetime format."""
        with pytest.raises(ParamValidationError, match="is not a 'date-time'"):
            Param(dt, type="string", format="date-time").resolve()

    def test_string_time_format(self):
        """Test string time format."""
        assert Param("03:04:05", type="string", format="time").resolve() == "03:04:05"

        error_pattern = "is not a 'time'"
        with pytest.raises(ParamValidationError, match=error_pattern):
            Param("03:04:05.06", type="string", format="time").resolve()

        with pytest.raises(ParamValidationError, match=error_pattern):
            Param("03:04", type="string", format="time").resolve()

        with pytest.raises(ParamValidationError, match=error_pattern):
            Param("24:00:00", type="string", format="time").resolve()

    @pytest.mark.parametrize(
        "date_string",
        [
            "2021-01-01",
        ],
    )
    def test_string_date_format(self, date_string):
        """Test string date format."""
        assert Param(date_string, type="string", format="date").resolve() == date_string

    # Note that 20120503 behaved differently in 3.11.3 Official python image. It was validated as a date
    # there but it started to fail again in 3.11.4 released on 2023-07-05.
    @pytest.mark.parametrize(
        "date_string",
        [
            "01/01/2021",
            "21 May 1975",
            "20120503",
        ],
    )
    def test_string_date_format_error(self, date_string):
        """Test string date format failures."""
        with pytest.raises(ParamValidationError, match="is not a 'date'"):
            Param(date_string, type="string", format="date").resolve()

    @pytest.mark.parametrize(
        "duration",
        [
            pytest.param("PT15M", id="minutes-only"),
            pytest.param("P1Y", id="years-only"),
            pytest.param("P1W", id="weeks-only"),
            pytest.param("P1D", id="days-only"),
            pytest.param("PT1H", id="hours-only"),
            pytest.param("PT30S", id="seconds-only"),
            pytest.param("P1DT2H", id="days-and-hours"),
            pytest.param("P1Y2M3DT4H5M6S", id="full-duration"),
            pytest.param("PT1.5H", id="fractional-hours-dot"),
        ],
    )
    def test_string_duration_format(self, duration):
        """Test valid ISO 8601 duration strings."""
        assert Param(duration, type="string", format="duration").resolve() == duration

    @pytest.mark.parametrize(
        "duration",
        [
            pytest.param("P", id="bare-P"),
            pytest.param("PT", id="bare-PT"),
            pytest.param("invalid", id="plain-text"),
            pytest.param("15M", id="missing-P-prefix"),
            pytest.param("1Y2M", id="no-P-prefix"),
        ],
    )
    def test_string_duration_format_error(self, duration):
        """Test invalid ISO 8601 duration strings."""
        with pytest.raises(ParamValidationError, match="is not a 'duration'"):
            Param(duration, type="string", format="duration").resolve()

    def test_int_param(self):
        p = Param(5)
        assert p.resolve() == 5

        p = Param(type="integer", minimum=0, maximum=10)
        assert p.resolve(value=5) == 5

        with pytest.raises(ParamValidationError):
            p.resolve(value=20)

    def test_number_param(self):
        p = Param(42, type="number")
        assert p.resolve() == 42

        p = Param(1.2, type="number")
        assert p.resolve() == 1.2

        p = Param("42", type="number")
        with pytest.raises(ParamValidationError):
            p.resolve()

    def test_list_param(self):
        p = Param([1, 2], type="array")
        assert p.resolve() == [1, 2]

    def test_dict_param(self):
        p = Param({"a": 1, "b": 2}, type="object")
        assert p.resolve() == {"a": 1, "b": 2}

    def test_composite_param(self):
        p = Param(type=["string", "number"])
        assert p.resolve(value="abc") == "abc"
        assert p.resolve(value=5.0) == 5.0

    def test_param_with_description(self):
        p = Param(10, description="Sample description")
        assert p.description == "Sample description"

    def test_suppress_exception(self):
        p = Param("abc", type="string", minLength=2, maxLength=4)
        assert p.resolve() == "abc"

        p.value = "long_string"
        assert p.resolve(suppress_exception=True) is None

    def test_explicit_schema(self):
        p = Param("abc", schema={type: "string"})
        assert p.resolve() == "abc"

    def test_custom_param(self):
        class S3Param(Param):
            def __init__(self, path: str):
                schema = {"type": "string", "pattern": r"s3:\/\/(.+?)\/(.+)"}
                super().__init__(default=path, schema=schema)

        p = S3Param("s3://my_bucket/my_path")
        assert p.resolve() == "s3://my_bucket/my_path"

        p = S3Param("file://not_valid/s3_path")
        with pytest.raises(ParamValidationError):
            p.resolve()

    def test_value_saved(self):
        p = Param("hello", type="string")
        assert p.resolve("world") == "world"
        assert p.resolve() == "world"

    def test_dump(self):
        p = Param("hello", description="world", type="string", minLength=2)
        dump = p.dump()
        assert dump == {
            "__class": "airflow.sdk.definitions.param.Param",
            "value": "hello",
            "description": "world",
            "schema": {"type": "string", "minLength": 2},
            "source": None,
        }

    @pytest.mark.parametrize(
        "param",
        [
            Param("my value", description="hello", schema={"type": "string"}),
            Param("my value", description="hello"),
            Param(None, description=None),
            Param([True], type="array", items={"type": "boolean"}),
            Param(),
        ],
    )
    def test_param_serialization(self, param: Param):
        """
        Test to make sure that native Param objects can be correctly serialized
        """

        serializer = BaseSerialization()
        serialized_param = serializer.serialize(param)
        restored_param: Param = serializer.deserialize(serialized_param)

        assert restored_param.value == param.value
        assert isinstance(restored_param, SerializedParam)
        assert restored_param.description == param.description
        assert restored_param.schema == param.schema

    @pytest.mark.parametrize(
        ("default", "should_raise"),
        [
            pytest.param({0, 1, 2}, True, id="default-non-JSON-serializable"),
            pytest.param(None, False, id="default-None"),  # Param init should not warn
            pytest.param({"b": 1}, False, id="default-JSON-serializable"),  # Param init should not warn
        ],
    )
    def test_param_json_validation(self, default, should_raise):
        exception_msg = "All provided parameters must be json-serializable"
        cm = pytest.raises(ParamValidationError, match=exception_msg) if should_raise else nullcontext()
        with cm:
            p = Param(default=default)
        if not should_raise:
            p.resolve()  # when resolved with NOTSET, should not warn.
            p.resolve(value={"a": 1})  # when resolved with JSON-serializable, should not warn.
            with pytest.raises(ParamValidationError, match=exception_msg):
                p.resolve(value={1, 2, 3})  # when resolved with not JSON-serializable, should warn.


class TestParamsDict:
    def test_params_dict(self):
        # Init with a simple dictionary
        pd = ParamsDict(dict_obj={"key": "value"})
        assert isinstance(pd.get_param("key"), Param)
        assert pd["key"] == "value"
        assert pd.suppress_exception is False

        # Init with a dict which contains Param objects
        pd2 = ParamsDict({"key": Param("value", type="string")}, suppress_exception=True)
        assert isinstance(pd2.get_param("key"), Param)
        assert pd2["key"] == "value"
        assert pd2.suppress_exception is True

        # Init with another object of another ParamsDict
        pd3 = ParamsDict(pd2)
        assert isinstance(pd3.get_param("key"), Param)
        assert pd3["key"] == "value"
        assert pd3.suppress_exception is False  # as it's not a deepcopy of pd2

        # Dump the ParamsDict
        assert pd.dump() == {"key": "value"}
        assert pd2.dump() == {"key": "value"}
        assert pd3.dump() == {"key": "value"}

        # Validate the ParamsDict
        plain_dict = pd.validate()
        assert isinstance(plain_dict, dict)
        pd2.validate()
        pd3.validate()

        # Update the ParamsDict
        with pytest.raises(ParamValidationError, match=r"Invalid input for param key: 1 is not"):
            pd3["key"] = 1

        # Should not raise an error as suppress_exception is True
        pd2["key"] = 1
        pd2.validate()

    def test_update(self):
        pd = ParamsDict({"key": Param("value", type="string")})

        pd.update({"key": "a"})
        internal_value = pd.get_param("key")
        assert isinstance(internal_value, Param)
        with pytest.raises(ParamValidationError, match=r"Invalid input for param key: 1 is not"):
            pd.update({"key": 1})

    def test_repr(self):
        pd = ParamsDict({"key": Param("value", type="string")})
        assert repr(pd) == "{'key': 'value'}"

    @pytest.mark.parametrize("source", ("dag", "task"))
    def test_fill_missing_param_source(self, source: Literal["dag", "task"]):
        pd = ParamsDict(
            {
                "key": Param("value", type="string"),
                "key2": "value2",
            }
        )
        pd._fill_missing_param_source(source)
        for param in pd.values():
            assert param.source == source

    def test_fill_missing_param_source_not_overwrite_existing(self):
        pd = ParamsDict(
            {
                "key": Param("value", type="string", source="dag"),
                "key2": "value2",
                "key3": "value3",
            }
        )
        pd._fill_missing_param_source("task")
        for key, expected_source in (
            ("key", "dag"),
            ("key2", "task"),
            ("key3", "task"),
        ):
            assert pd.get_param(key).source == expected_source

    def test_filter_params_by_source(self):
        pd = ParamsDict(
            {
                "key": Param("value", type="string", source="dag"),
                "key2": Param("value", source="task"),
            }
        )
        assert ParamsDict.filter_params_by_source(pd, "dag") == ParamsDict(
            {"key": Param("value", type="string", source="dag")},
        )
        assert ParamsDict.filter_params_by_source(pd, "task") == ParamsDict(
            {
                "key2": Param("value", source="task"),
            }
        )


class TestProcessParamsMasksPassword:
    def _make_dag(self, params: dict) -> MagicMock:
        dag = MagicMock()
        dag.params = ParamsDict(params)
        return dag

    def _make_task(self) -> MagicMock:
        task = MagicMock()
        task.params = None
        return task

    @patch("airflow.sdk.definitions.param.mask_secret")
    def test_masks_string_param_declared_as_password(self, mock_mask_secret):
        dag = self._make_dag({"api_token": Param("default", type="string", format="password")})
        task = self._make_task()

        resolved = process_params(dag, task, {"api_token": "super-secret-value"}, suppress_exception=False)

        assert resolved["api_token"] == "super-secret-value"
        # Called twice for the same value: once against dagrun_conf before the debug log line,
        # once against the final resolved_params after validate(). Both calls register the same
        # string with the masker, which is harmless (SecretsMasker patterns are a set, not a list).
        assert mock_mask_secret.call_count == 2
        mock_mask_secret.assert_called_with("super-secret-value")

    @patch("airflow.sdk.definitions.param.mask_secret")
    def test_does_not_mask_plain_string_param(self, mock_mask_secret):
        dag = self._make_dag({"greeting": Param("default", type="string")})
        task = self._make_task()

        resolved = process_params(dag, task, {"greeting": "hello"}, suppress_exception=False)

        assert resolved["greeting"] == "hello"
        mock_mask_secret.assert_not_called()

    @patch("airflow.sdk.definitions.param.mask_secret")
    def test_ignores_non_string_password_format_param(self, mock_mask_secret):
        # format="password" on a non-string type is not a supported combination;
        # it must not crash and must not attempt to mask a non-string value.
        dag = self._make_dag({"retry_count": Param(3, type="integer", format="password")})
        task = self._make_task()

        resolved = process_params(dag, task, {"retry_count": 5}, suppress_exception=False)

        assert resolved["retry_count"] == 5
        mock_mask_secret.assert_not_called()

    @patch("airflow.sdk.definitions.param.mask_secret")
    def test_does_not_mask_when_key_has_no_declared_param(self, mock_mask_secret):
        dag = self._make_dag({})
        task = self._make_task()

        resolved = process_params(dag, task, {"undeclared_key": "value"}, suppress_exception=False)

        assert resolved["undeclared_key"] == "value"
        mock_mask_secret.assert_not_called()

    @patch("airflow.sdk.definitions.param.logger")
    @patch("airflow.sdk.definitions.param.mask_secret")
    def test_masks_dagrun_conf_value_before_debug_log(self, mock_mask_secret, mock_logger):
        # Regression test: dag_run_conf_overrides_params=True (the default) logs dagrun_conf via
        # logger.debug(). mask_secret() must be called before that log line, not after, or the
        # SecretsMasker filter won't yet know to redact the value from the emitted log record.
        manager = MagicMock()
        manager.attach_mock(mock_mask_secret, "mask_secret")
        manager.attach_mock(mock_logger.debug, "debug")

        dag = self._make_dag({"api_token": Param("default", type="string", format="password")})
        task = self._make_task()

        process_params(dag, task, {"api_token": "super-secret-value"}, suppress_exception=False)

        call_names = [call[0] for call in manager.mock_calls]
        assert "mask_secret" in call_names
        assert "debug" in call_names
        assert call_names.index("mask_secret") < call_names.index("debug"), (
            "mask_secret must be registered before the debug log referencing dagrun_conf"
        )
