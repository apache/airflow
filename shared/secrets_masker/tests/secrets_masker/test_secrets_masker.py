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

import contextlib
import inspect
import logging
import logging.config
import os
import sys
import textwrap
from enum import Enum
from io import StringIO
from unittest.mock import patch

import pytest

from airflow_shared.secrets_masker.secrets_masker import (
    DEFAULT_SENSITIVE_FIELDS,
    RedactedIO,
    SecretsMasker,
    mask_secret,
    merge,
    redact,
    reset_secrets_masker,
)

from tests_common.test_utils.config import env_vars

pytestmark = pytest.mark.enable_redact
PASSWORD = "password"


def configure_secrets_masker_for_test(
    masker: SecretsMasker, min_length: int = 5, sensitive_fields: list[str] = None
):
    """Helper function to configure a SecretsMasker instance for testing."""
    masker.min_length_to_mask = min_length
    if sensitive_fields is None:
        masker.sensitive_variables_fields = list(DEFAULT_SENSITIVE_FIELDS)
    else:
        masker.sensitive_variables_fields = sensitive_fields


def lineno():
    """Returns the current line number in our program."""
    return inspect.currentframe().f_back.f_lineno


class MyEnum(str, Enum):
    testname = "testvalue"
    testname2 = "testvalue2"


@pytest.fixture
def logger(caplog):
    logging.config.dictConfig(
        {
            "version": 1,
            "handlers": {
                __name__: {
                    # Reset later
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stdout",
                }
            },
            "loggers": {
                __name__: {
                    "handlers": [__name__],
                    "level": logging.INFO,
                    "propagate": False,
                }
            },
            "disable_existing_loggers": False,
        }
    )
    formatter = ShortExcFormatter("%(levelname)s %(message)s")
    logger = logging.getLogger(__name__)

    caplog.handler.setFormatter(formatter)
    logger.handlers = [caplog.handler]
    filt = SecretsMasker()
    configure_secrets_masker_for_test(filt)
    SecretsMasker.enable_log_masking()
    logger.addFilter(filt)

    filt.add_mask("password")
    return logger


class TestSecretsMasker:
    def test_message(self, logger, caplog):
        logger.info("XpasswordY")
        assert caplog.text == "INFO X***Y\n"

    def test_args(self, logger, caplog):
        logger.info("Cannot connect to %s", "user:password")

        assert caplog.text == "INFO Cannot connect to user:***\n"

    def test_extra(self, logger, caplog):
        with patch.object(
            logger.handlers[0], "formatter", ShortExcFormatter("%(levelname)s %(message)s %(conn)s")
        ):
            logger.info("Cannot connect", extra={"conn": "user:password"})

            assert caplog.text == "INFO Cannot connect user:***\n"

    def test_exception(self, logger, caplog):
        try:
            conn = "user:password"
            raise RuntimeError("Cannot connect to " + conn)
        except RuntimeError:
            logger.exception("Err")

        line = lineno() - 4

        assert caplog.text == textwrap.dedent(
            f"""\
            ERROR Err
            Traceback (most recent call last):
              File ".../test_secrets_masker.py", line {line}, in test_exception
                raise RuntimeError("Cannot connect to " + conn)
            RuntimeError: Cannot connect to user:***
            """
        )

    def test_exception_not_raised(self, logger, caplog):
        """
        Test that when ``logger.exception`` is called when there is no current exception we still log.

        (This is a "bug" in user code, but we shouldn't die because of it!)
        """
        logger.exception("Err")

        assert caplog.text == textwrap.dedent(
            """\
            ERROR Err
            NoneType: None
            """
        )

    @pytest.mark.xfail(reason="Cannot filter secrets in traceback source")
    def test_exc_tb(self, logger, caplog):
        """
        Show it is not possible to filter secrets in the source.

        It is not possible to (regularly/reliably) filter out secrets that
        appear directly in the source code. This is because the formatting of
        exc_info is not done in the filter, it is done after the filter is
        called, and fixing this "properly" is hard/impossible.

        (It would likely need to construct a custom traceback that changed the
        source. I have no idea if that is even possible)

        This test illustrates that, but ix marked xfail in case someone wants to
        fix this later.
        """
        try:
            raise RuntimeError("Cannot connect to user:password")
        except RuntimeError:
            logger.exception("Err")

        line = lineno() - 4

        assert caplog.text == textwrap.dedent(
            f"""\
            ERROR Err
            Traceback (most recent call last):
              File ".../test_secrets_masker.py", line {line}, in test_exc_tb
                raise RuntimeError("Cannot connect to user:***)
            RuntimeError: Cannot connect to user:***
            """
        )

    def test_masking_in_implicit_context_exceptions(self, logger, caplog):
        """
        Show that redacting password works in context exceptions.
        """
        try:
            try:
                try:
                    raise RuntimeError(f"Cannot connect to user:{PASSWORD}")
                except RuntimeError as ex1:
                    raise RuntimeError(f"Exception: {ex1}")
            except RuntimeError as ex2:
                raise RuntimeError(f"Exception: {ex2}")
        except RuntimeError:
            logger.exception("Err")

        assert "user:password" not in caplog.text
        assert caplog.text.count("user:***") >= 2

    def test_masking_in_explicit_context_exceptions(self, logger, caplog):
        """
        Show that redacting password works in context exceptions.
        """
        exception = None
        try:
            raise RuntimeError(f"Cannot connect to user:{PASSWORD}")
        except RuntimeError as ex:
            exception = ex
        try:
            raise RuntimeError(f"Exception: {exception}") from exception
        except RuntimeError:
            logger.exception("Err")

        line = lineno() - 8

        assert caplog.text == textwrap.dedent(
            f"""\
            ERROR Err
            Traceback (most recent call last):
              File ".../test_secrets_masker.py", line {line}, in test_masking_in_explicit_context_exceptions
                raise RuntimeError(f"Cannot connect to user:{{PASSWORD}}")
            RuntimeError: Cannot connect to user:***

            The above exception was the direct cause of the following exception:

            Traceback (most recent call last):
              File ".../test_secrets_masker.py", line {line + 4}, in test_masking_in_explicit_context_exceptions
                raise RuntimeError(f"Exception: {{exception}}") from exception
            RuntimeError: Exception: Cannot connect to user:***
            """
        )

    @pytest.mark.parametrize(
        ("name", "value", "expected_mask"),
        [
            (None, "secret", {"secret"}),
            ("apikey", "secret", {"secret"}),
            # the value for "apikey", and "password" should end up masked
            (None, {"apikey": "secret", "other": {"val": "innocent", "password": "foo"}}, {"secret"}),
            (None, ["secret", "other"], {"secret", "other"}),
            # When the "sensitive value" is a dict, don't mask anything
            # (Or should this be mask _everything_ under it ?
            ("api_key", {"other": "innoent"}, set()),
            (None, {"password": ""}, set()),
            (None, "", set()),
        ],
    )
    def test_mask_secret(self, name, value, expected_mask):
        filt = SecretsMasker()
        configure_secrets_masker_for_test(filt)

        filt.add_mask(value, name)
        assert filt.patterns == expected_mask

    @pytest.mark.parametrize(
        ("patterns", "name", "value", "expected"),
        [
            ({"secret"}, None, "secret", "***"),
            (
                {"secret", "foo"},
                None,
                {"apikey": "secret", "other": {"val": "innocent", "password": "foo"}},
                {"apikey": "***", "other": {"val": "innocent", "password": "***"}},
            ),
            ({"secret", "other"}, None, ["secret", "other"], ["***", "***"]),
            # We don't mask dict _keys_.
            ({"secret", "other"}, None, {"data": {"secret": "secret"}}, {"data": {"secret": "***"}}),
            # Non string dict keys
            ({"secret", "other"}, None, {1: {"secret": "secret"}}, {1: {"secret": "***"}}),
            (
                # Since this is a sensitive name, all the values should be redacted!
                {"secret"},
                "api_key",
                {"other": "innoent", "nested": ["x", "y"]},
                {"other": "***", "nested": ["***", "***"]},
            ),
            (
                # Test that masking still works based on name even when no patterns given
                set(),
                "env",
                {"api_key": "masked based on key name", "other": "foo"},
                {"api_key": "***", "other": "foo"},
            ),
        ],
    )
    def test_redact(self, patterns, name, value, expected):
        filt = SecretsMasker()
        configure_secrets_masker_for_test(filt)
        for val in patterns:
            filt.add_mask(val)

        assert filt.redact(value, name) == expected

    @pytest.mark.parametrize(
        ("name", "value", "expected"),
        [
            ("api_key", "pass", "*️⃣*️⃣*️⃣"),
            ("api_key", ("pass",), ("*️⃣*️⃣*️⃣",)),
            (None, {"data": {"secret": "secret"}}, {"data": {"secret": "*️⃣*️⃣*️⃣"}}),
            # Non string dict keys
            (None, {1: {"secret": "secret"}}, {1: {"secret": "*️⃣*️⃣*️⃣"}}),
            (
                "api_key",
                {"other": "innoent", "nested": ["x", "y"]},
                {"other": "*️⃣*️⃣*️⃣", "nested": ["*️⃣*️⃣*️⃣", "*️⃣*️⃣*️⃣"]},
            ),
        ],
    )
    def test_redact_replacement(self, name, value, expected):
        filt = SecretsMasker()
        configure_secrets_masker_for_test(filt)

        assert filt.redact(value, name, replacement="*️⃣*️⃣*️⃣") == expected

    def test_redact_filehandles(self, caplog):
        filt = SecretsMasker()
        configure_secrets_masker_for_test(filt)
        with open("/dev/null", "w") as handle:
            assert filt.redact(handle, None) == handle

        # We shouldn't have logged a warning here
        assert caplog.messages == []

    @pytest.mark.parametrize(
        ("val", "expected", "max_depth"),
        [
            (["abcdef"], ["***"], None),
            (["abcdef"], ["***"], 1),
            ([[[["abcdef"]]]], [[[["***"]]]], None),
            ([[[[["abcdef"]]]]], [[[[["***"]]]]], None),
            # Items below max depth aren't redacted
            ([[[[[["abcdef"]]]]]], [[[[[["abcdef"]]]]]], None),
            ([["abcdef"]], [["abcdef"]], 1),
        ],
    )
    def test_redact_max_depth(self, val, expected, max_depth):
        secrets_masker = SecretsMasker()
        configure_secrets_masker_for_test(secrets_masker)
        secrets_masker.add_mask("abcdef")
        with patch(
            "airflow_shared.secrets_masker.secrets_masker._secrets_masker", return_value=secrets_masker
        ):
            got = redact(val, max_depth=max_depth)
            assert got == expected

    def test_redact_with_str_type(self, logger, caplog):
        """
        SecretsMasker's re replacer has issues handling a redactable item of type
        `str` with required constructor args. This test ensures there is a shim in
        place that avoids any issues.
        See: https://github.com/apache/airflow/issues/19816#issuecomment-983311373
        """

        class StrLikeClassWithRequiredConstructorArg(str):
            def __init__(self, required_arg):
                pass

        text = StrLikeClassWithRequiredConstructorArg("password")
        logger.info("redacted: %s", text)

        # we expect the object's __str__() output to be logged (no warnings due to a failed masking)
        assert caplog.messages == ["redacted: ***"]

    @pytest.mark.parametrize(
        ("state", "expected"),
        [
            (MyEnum.testname, "testvalue"),
        ],
    )
    def test_redact_state_enum(self, logger, caplog, state, expected):
        logger.info("State: %s", state)
        assert caplog.text == f"INFO State: {expected}\n"
        assert "TypeError" not in caplog.text

    def test_reset_secrets_masker(
        self,
    ):
        secrets_masker = SecretsMasker()
        configure_secrets_masker_for_test(secrets_masker)
        secrets_masker.add_mask("mask_this")
        secrets_masker.add_mask("and_this")
        secrets_masker.add_mask("maybe_this_too")

        val = ["mask_this", "and_this", "maybe_this_too"]

        with patch(
            "airflow_shared.secrets_masker.secrets_masker._secrets_masker", return_value=secrets_masker
        ):
            got = redact(val)
            assert got == ["***"] * 3

            reset_secrets_masker()

            got = redact(val)
            assert got == val

    def test_property_for_log_masking(self, monkeypatch):
        """Test that log masking enable/disable methods."""

        # store the original state before any patching
        state = SecretsMasker.mask_secrets_in_logs

        with monkeypatch.context() as mp:
            mp.setattr(SecretsMasker, "mask_secrets_in_logs", True)

            masker1 = SecretsMasker()
            masker2 = SecretsMasker()

            assert masker1.is_log_masking_enabled()
            assert masker2.is_log_masking_enabled()

            masker2.disable_log_masking()
            assert not masker1.is_log_masking_enabled()
            assert not masker2.is_log_masking_enabled()

            masker1.enable_log_masking()
            assert masker1.is_log_masking_enabled()
            assert masker2.is_log_masking_enabled()

        # assert we restored the original state
        assert SecretsMasker.mask_secrets_in_logs == state


class TestShouldHideValueForKey:
    @pytest.mark.parametrize(
        ("key", "expected_result"),
        [
            ("", False),
            (None, False),
            ("key", False),
            ("google_api_key", True),
            ("GOOGLE_API_KEY", True),
            ("GOOGLE_APIKEY", True),
            (1, False),
        ],
    )
    def test_hiding_defaults(self, key, expected_result):
        masker = SecretsMasker()
        configure_secrets_masker_for_test(masker)
        assert expected_result == masker.should_hide_value_for_key(key)

    @pytest.mark.parametrize(
        ("sensitive_variable_fields", "key", "expected_result"),
        [
            ("key", "TRELLO_KEY", True),
            ("key", "TRELLO_API_KEY", True),
            ("key", "GITHUB_APIKEY", True),
            ("key, token", "TRELLO_TOKEN", True),
            ("mysecretword, mysensitivekey", "GITHUB_mysecretword", True),
            (None, "TRELLO_API", False),
            ("token", "TRELLO_KEY", False),
            ("token, mysecretword", "TRELLO_KEY", False),
        ],
    )
    def test_hiding_config(self, sensitive_variable_fields, key, expected_result):
        env_value = str(sensitive_variable_fields) if sensitive_variable_fields is not None else ""
        with env_vars({"AIRFLOW__CORE__SENSITIVE_VAR_CONN_NAMES": env_value}):
            masker = SecretsMasker()
            sensitive_fields = list(DEFAULT_SENSITIVE_FIELDS)
            if sensitive_variable_fields:
                additional_fields = {
                    field.strip() for field in sensitive_variable_fields.split(",") if field.strip()
                }
                sensitive_fields = list(DEFAULT_SENSITIVE_FIELDS | additional_fields)

            configure_secrets_masker_for_test(masker, sensitive_fields=sensitive_fields)
            assert expected_result == masker.should_hide_value_for_key(key)


class ShortExcFormatter(logging.Formatter):
    """Don't include full path in exc_info messages"""

    def formatException(self, exc_info):
        formatted = super().formatException(exc_info)
        return formatted.replace(__file__, ".../" + os.path.basename(__file__))


class TestRedactedIO:
    @pytest.fixture(scope="class", autouse=True)
    def reset_secrets_masker(self):
        self.secrets_masker = SecretsMasker()
        configure_secrets_masker_for_test(self.secrets_masker)
        with patch(
            "airflow_shared.secrets_masker.secrets_masker._secrets_masker",
            return_value=self.secrets_masker,
        ):
            mask_secret(PASSWORD)
            yield

    def test_redacts_from_print(self, capsys):
        # Without redacting, password is printed.
        print(PASSWORD)
        stdout = capsys.readouterr().out
        assert stdout == f"{PASSWORD}\n"
        assert "***" not in stdout

        # With context manager, password is redacted.
        with contextlib.redirect_stdout(RedactedIO()):
            print(PASSWORD)
        stdout = capsys.readouterr().out
        assert stdout == "***\n"

    def test_write(self, capsys):
        RedactedIO().write(PASSWORD)
        stdout = capsys.readouterr().out
        assert stdout == "***"

    def test_input_builtin(self, monkeypatch):
        """
        Test that when redirect is inplace the `input()` builtin works.

        This is used by debuggers!
        """
        monkeypatch.setattr(sys, "stdin", StringIO("a\n"))
        with contextlib.redirect_stdout(RedactedIO()):
            assert input() == "a"


class TestMaskSecretAdapter:
    @pytest.fixture(autouse=True)
    def reset_secrets_masker_and_skip_escape(self):
        self.secrets_masker = SecretsMasker()
        configure_secrets_masker_for_test(self.secrets_masker)
        with patch(
            "airflow_shared.secrets_masker.secrets_masker._secrets_masker",
            return_value=self.secrets_masker,
        ):
            with patch("airflow_shared.secrets_masker.secrets_masker.re.escape", lambda x: x):
                yield

    def test_calling_mask_secret_adds_adaptations_for_returned_str(self):
        import urllib.parse

        with env_vars({"AIRFLOW__LOGGING__SECRET_MASK_ADAPTER": "urllib.parse.quote"}):
            # Manually configure the adapter since we don't read from config anymore
            self.secrets_masker.secret_mask_adapter = urllib.parse.quote
            mask_secret("secret<>&", None)

        assert self.secrets_masker.patterns == {"secret%3C%3E%26", "secret<>&"}

    def test_calling_mask_secret_adds_adaptations_for_returned_iterable(self):
        import urllib.parse

        with env_vars({"AIRFLOW__LOGGING__SECRET_MASK_ADAPTER": "urllib.parse.urlparse"}):
            # Manually configure the adapter since we don't read from config anymore
            self.secrets_masker.secret_mask_adapter = urllib.parse.urlparse
            mask_secret("https://airflow.apache.org/docs/apache-airflow/stable", "password")

        assert self.secrets_masker.patterns == {
            "https",
            "airflow.apache.org",
            "/docs/apache-airflow/stable",
            "https://airflow.apache.org/docs/apache-airflow/stable",
        }

    def test_calling_mask_secret_not_set(self):
        with env_vars({"AIRFLOW__LOGGING__SECRET_MASK_ADAPTER": ""}):
            # Ensure no adapter is set
            self.secrets_masker.secret_mask_adapter = None
            mask_secret("a secret")

        assert self.secrets_masker.patterns == {"a secret"}

    @pytest.mark.parametrize(
        ("secret", "should_be_masked", "is_first_short", "comment"),
        [
            ("abc", False, True, "short secret with first warning"),
            ("def", False, False, "short secret with no warning"),
            ("airflow", False, False, "keyword that should be skipped"),
            ("valid_secret", True, False, "valid secret that should be masked"),
        ],
    )
    def test_add_mask_short_secrets_and_skip_keywords(
        self, caplog, secret, should_be_masked, is_first_short, comment
    ):
        if is_first_short:
            SecretsMasker._has_warned_short_secret = False
        else:
            SecretsMasker._has_warned_short_secret = True

        filt = SecretsMasker()
        configure_secrets_masker_for_test(filt, min_length=5)

        caplog.clear()

        filt.add_mask(secret)

        if is_first_short:
            assert "Skipping masking for a secret as it's too short" in caplog.text
            assert len(caplog.records) == 1
        else:
            assert "Skipping masking for a secret as it's too short" not in caplog.text

        if should_be_masked:
            assert secret in filt.patterns
        else:
            assert secret not in filt.patterns

        caplog.clear()

        if should_be_masked:
            assert filt.replacer is not None


class TestStructuredVsUnstructuredMasking:
    def test_structured_sensitive_fields_always_masked(self):
        secrets_masker = SecretsMasker()
        configure_secrets_masker_for_test(secrets_masker, min_length=5)

        short_password = "pwd"
        short_token = "tk"
        short_api_key = "key"

        test_data = {
            "password": short_password,
            "api_key": short_token,
            "connection": {"secret": short_api_key},
        }

        with patch(
            "airflow_shared.secrets_masker.secrets_masker._secrets_masker", return_value=secrets_masker
        ):
            redacted_data = redact(test_data)

            assert redacted_data["password"] == "***"
            assert redacted_data["api_key"] == "***"
            assert redacted_data["connection"]["secret"] == "***"

    def test_unstructured_text_min_length_enforced(self):
        secrets_masker = SecretsMasker()
        min_length = 5
        configure_secrets_masker_for_test(secrets_masker, min_length=min_length)

        short_secret = "abc"
        long_secret = "abcdef"

        with patch(
            "airflow_shared.secrets_masker.secrets_masker._secrets_masker", return_value=secrets_masker
        ):
            secrets_masker.add_mask(short_secret)
            secrets_masker.add_mask(long_secret)

            assert short_secret not in secrets_masker.patterns
            assert long_secret in secrets_masker.patterns

            test_data = f"Containing {short_secret} and {long_secret}"
            redacted = secrets_masker.redact(test_data)

            assert short_secret in redacted
            assert long_secret not in redacted
            assert "***" in redacted


class TestContainerTypesRedaction:
    def test_kubernetes_env_var_redaction(self):
        class MockV1EnvVar:
            def __init__(self, name, value):
                self.name = name
                self.value = value

            def to_dict(self):
                return {"name": self.name, "value": self.value}

        secret_env_var = MockV1EnvVar("password", "secret_password")
        normal_env_var = MockV1EnvVar("app_name", "my_app")

        secrets_masker = SecretsMasker()
        configure_secrets_masker_for_test(secrets_masker)

        with patch(
            "airflow_shared.secrets_masker.secrets_masker._secrets_masker", return_value=secrets_masker
        ):
            with patch(
                "airflow_shared.secrets_masker.secrets_masker._is_v1_env_var",
                side_effect=lambda a: isinstance(a, MockV1EnvVar),
            ):
                redacted_secret = redact(secret_env_var)
                redacted_normal = redact(normal_env_var)

                assert redacted_secret["value"] == "***"
                assert redacted_normal["value"] == "my_app"

    def test_deeply_nested_mixed_structures(self):
        nested_data = {
            "level1": {
                "normal_key": "normal_value",
                "password": "secret_pass",
                "level2": [
                    {"api_key": "secret_key", "user": "normal_user"},
                    ("token", "secret_token"),
                    {"nested_list": ["normal", "password=secret"]},
                ],
            }
        }

        secrets_masker = SecretsMasker()
        configure_secrets_masker_for_test(secrets_masker)

        secrets_masker.add_mask("secret_token")
        secrets_masker.add_mask("password=secret")

        with patch(
            "airflow_shared.secrets_masker.secrets_masker._secrets_masker", return_value=secrets_masker
        ):
            redacted_data = redact(nested_data)

            assert redacted_data["level1"]["normal_key"] == "normal_value"
            assert redacted_data["level1"]["password"] == "***"
            assert redacted_data["level1"]["level2"][0]["api_key"] == "***"
            assert redacted_data["level1"]["level2"][0]["user"] == "normal_user"
            assert redacted_data["level1"]["level2"][1][1] == "***"

            nested_list_str = str(redacted_data["level1"]["level2"][2]["nested_list"])
            assert "password=secret" not in nested_list_str
            assert "password=***" in nested_list_str or "***" in nested_list_str


class TestEdgeCases:
    def test_circular_references(self):
        circular_dict: dict[str, any] = {"key": "value", "password": "secret_password"}
        circular_dict["self_ref"] = circular_dict

        secrets_masker = SecretsMasker()
        configure_secrets_masker_for_test(secrets_masker)

        with patch(
            "airflow_shared.secrets_masker.secrets_masker._secrets_masker", return_value=secrets_masker
        ):
            redacted_data = redact(circular_dict)

            assert redacted_data["key"] == "value"
            assert redacted_data["password"] == "***"

            assert isinstance(redacted_data["self_ref"], dict)

    def test_regex_special_chars_in_secrets(self):
        regex_secrets = ["password+with*chars", "token.with[special]chars", "api_key^that$needs(escaping)"]

        secrets_masker = SecretsMasker()
        configure_secrets_masker_for_test(secrets_masker)

        for secret in regex_secrets:
            secrets_masker.add_mask(secret)

        test_string = f"Contains {regex_secrets[0]} and {regex_secrets[1]} and {regex_secrets[2]}"

        redacted = secrets_masker.redact(test_string)

        for secret in regex_secrets:
            assert secret not in redacted

        assert redacted.count("***") == 3
        assert redacted.startswith("Contains ")
        assert " and " in redacted


class TestDirectMethodCalls:
    def test_redact_all_directly(self):
        secrets_masker = SecretsMasker()
        configure_secrets_masker_for_test(secrets_masker)

        test_data = {
            "string": "should_be_masked",
            "number": 12345,
            "boolean": True,
            "list": ["item1", "item2"],
            "dict": {"k1": "v1", "k2": "v2"},
            "nested": {"tuple": ("a", "b", "c"), "set": {"x", "y", "z"}},
        }

        result = secrets_masker._redact_all(test_data, depth=0, replacement="***")

        assert result["string"] == "***"
        assert result["number"] == 12345
        assert result["boolean"] is True
        assert all(val == "***" for val in result["list"])
        assert all(val == "***" for val in result["dict"].values())
        assert all(val == "***" for val in result["nested"]["tuple"])
        assert isinstance(result["nested"]["set"], tuple)
        assert all(val == "***" for val in result["nested"]["set"])


class TestMixedDataScenarios:
    def test_mixed_structured_unstructured_data(self):
        secrets_masker = SecretsMasker()
        configure_secrets_masker_for_test(secrets_masker)

        unstructured_secret = "this_is_a_secret_pattern"
        secrets_masker.add_mask(unstructured_secret)

        mixed_data = {
            "normal_field": "normal_value",
            "password": "short_pw",
            "description": f"Text containing {unstructured_secret} that should be masked",
            "nested": {"token": "tk", "info": "No secrets here"},
        }

        with patch(
            "airflow_shared.secrets_masker.secrets_masker._secrets_masker", return_value=secrets_masker
        ):
            redacted_data = redact(mixed_data)

            assert redacted_data["normal_field"] == "normal_value"
            assert redacted_data["password"] == "***"
            assert unstructured_secret not in redacted_data["description"]
            assert "***" in redacted_data["description"]
            assert redacted_data["nested"]["token"] == "***"
            assert redacted_data["nested"]["info"] == "No secrets here"


class TestSecretsMaskerMerge:
    """Test the merge functionality for restoring original values from redacted data."""

    def setup_method(self):
        self.masker = SecretsMasker()
        configure_secrets_masker_for_test(self.masker)

    @pytest.mark.parametrize(
        ("new_value", "old_value", "name", "expected"),
        [
            ("***", "original_secret", "password", "original_secret"),
            ("new_secret", "original_secret", "password", "new_secret"),
            ("***", "original_value", "normal_field", "***"),
            ("new_value", "original_value", "normal_field", "new_value"),
            ("***", "original_value", None, "***"),
            ("new_value", "original_value", None, "new_value"),
        ],
    )
    def test_merge_simple_strings(self, new_value, old_value, name, expected):
        result = self.masker.merge(new_value, old_value, name)
        assert result == expected

    @pytest.mark.parametrize(
        ("old_data", "new_data", "expected"),
        [
            (
                {
                    "password": "original_password",
                    "api_key": "original_api_key",
                    "normal_field": "original_normal",
                },
                {
                    "password": "***",
                    "api_key": "new_api_key",
                    "normal_field": "new_normal",
                },
                {
                    "password": "original_password",
                    "api_key": "new_api_key",
                    "normal_field": "new_normal",
                },
            ),
            (
                {
                    "config": {"password": "original_password", "host": "original_host"},
                    "credentials": {"api_key": "original_api_key", "username": "original_user"},
                },
                {
                    "config": {
                        "password": "***",
                        "host": "new_host",
                    },
                    "credentials": {
                        "api_key": "new_api_key",
                        "username": "new_user",
                    },
                },
                {
                    "config": {
                        "password": "original_password",
                        "host": "new_host",
                    },
                    "credentials": {
                        "api_key": "new_api_key",
                        "username": "new_user",
                    },
                },
            ),
        ],
    )
    def test_merge_dictionaries(self, old_data, new_data, expected):
        result = self.masker.merge(new_data, old_data)
        assert result == expected

    @pytest.mark.parametrize(
        ("old_data", "new_data", "name", "expected"),
        [
            # Lists
            (
                ["original_item1", "original_item2", "original_item3"],
                ["new_item1", "new_item2"],
                None,
                ["new_item1", "new_item2"],
            ),
            (
                ["original_item1", "original_item2"],
                ["new_item1", "new_item2", "new_item3", "new_item4"],
                None,
                ["new_item1", "new_item2", "new_item3", "new_item4"],
            ),
            (
                ["secret1", "secret2", "secret3"],
                ["***", "new_secret2", "***"],
                "password",
                ["secret1", "new_secret2", "secret3"],
            ),
            (
                ["value1", "value2", "value3"],
                ["***", "new_value2", "***"],
                "normal_list",
                ["***", "new_value2", "***"],
            ),
            # Tuples
            (
                ("original_item1", "original_item2", "original_item3"),
                ("new_item1", "new_item2"),
                None,
                ("new_item1", "new_item2"),
            ),
            (
                ("original_item1", "original_item2"),
                ("new_item1", "new_item2", "new_item3", "new_item4"),
                None,
                ("new_item1", "new_item2", "new_item3", "new_item4"),
            ),
            (
                ("secret1", "secret2", "secret3"),
                ("***", "new_secret2", "***"),
                "password",
                ("secret1", "new_secret2", "secret3"),
            ),
            (
                ("value1", "value2", "value3"),
                ("***", "new_value2", "***"),
                "normal_tuple",
                ("***", "new_value2", "***"),
            ),
            # Sets
            (
                {"original_item1", "original_item2", "original_item3"},
                {"new_item1", "new_item2"},
                None,
                {"new_item1", "new_item2"},
            ),
            (
                {"original_item1", "original_item2"},
                {"new_item1", "new_item2", "new_item3", "new_item4"},
                None,
                {"new_item1", "new_item2", "new_item3", "new_item4"},
            ),
            (
                {"secret1", "secret2", "secret3"},
                {"***", "new_secret2", "***"},
                "password",
                {"***", "new_secret2", "***"},
            ),
            (
                {"value1", "value2", "value3"},
                {"***", "new_value2", "***"},
                "normal_tuple",
                {"***", "new_value2", "***"},
            ),
        ],
    )
    def test_merge_collections(self, old_data, new_data, name, expected):
        result = self.masker.merge(new_data, old_data, name)
        assert result == expected

    def test_merge_mismatched_types(self):
        old_data = {"key": "value"}
        new_data = "some_string"  # Different type

        # When types don't match, prefer the new item
        expected = "some_string"

        result = self.masker.merge(new_data, old_data)
        assert result == expected

    def test_merge_with_missing_keys(self):
        old_data = {"password": "original_password", "old_only_key": "old_value", "common_key": "old_common"}

        new_data = {
            "password": "***",
            "new_only_key": "new_value",
            "common_key": "new_common",
        }

        expected = {
            "password": "original_password",
            "new_only_key": "new_value",
            "common_key": "new_common",
        }

        result = self.masker.merge(new_data, old_data)
        assert result == expected

    def test_merge_complex_redacted_structures(self):
        old_data = {
            "some_config": {
                "nested_password": "original_nested_password",
                "passwords": ["item1", "item2"],
            },
            "normal_field": "normal_value",
        }

        new_data = {
            "some_config": {"nested_password": "***", "passwords": ["***", "new_item2"]},
            "normal_field": "new_normal_value",
        }

        result = self.masker.merge(new_data, old_data)
        expected = {
            "some_config": {
                "nested_password": "original_nested_password",
                "passwords": ["item1", "new_item2"],
            },
            "normal_field": "new_normal_value",
        }
        assert result == expected

    def test_merge_partially_redacted_structures(self):
        old_data = {
            "config": {
                "password": "original_password",
                "host": "original_host",
                "nested": {"api_key": "original_api_key", "timeout": 30},
            }
        }

        new_data = {
            "config": {
                "password": "***",
                "host": "new_host",
                "nested": {
                    "api_key": "***",
                    "timeout": 60,
                },
            }
        }

        expected = {
            "config": {
                "password": "original_password",
                "host": "new_host",
                "nested": {
                    "api_key": "original_api_key",
                    "timeout": 60,
                },
            }
        }

        result = self.masker.merge(new_data, old_data)
        assert result == expected

    def test_merge_max_depth(self):
        old_data = {"level1": {"level2": {"level3": {"password": "original_password"}}}}
        new_data = {"level1": {"level2": {"level3": {"password": "***"}}}}

        result = merge(new_data, old_data, max_depth=1)
        assert result == new_data

        result = self.masker.merge(new_data, old_data, max_depth=10)
        assert result["level1"]["level2"]["level3"]["password"] == "original_password"

    def test_merge_enum_values(self):
        old_enum = MyEnum.testname
        new_enum = MyEnum.testname2

        result = self.masker.merge(new_enum, old_enum)
        assert result == new_enum
        assert isinstance(result, MyEnum)

    def test_merge_round_trip(self):
        # Original data with sensitive information
        original_config = {
            "database": {"host": "db.example.com", "password": "super_secret_password", "username": "admin"},
            "api": {"api_key": "secret_api_key_12345", "endpoint": "https://api.example.com", "timeout": 30},
            "app_name": "my_application",
        }

        # Step 1: Redact the original data
        redacted_dict = self.masker.redact(original_config)

        # Verify sensitive fields are redacted
        assert redacted_dict["database"]["password"] == "***"
        assert redacted_dict["api"]["api_key"] == "***"
        assert redacted_dict["database"]["host"] == "db.example.com"

        # Step 2: User modifies some fields
        updated_dict = redacted_dict.copy()
        updated_dict["database"]["host"] = "new-db.example.com"
        updated_dict["api"]["timeout"] = 60
        updated_dict["api"]["api_key"] = "new_api_key_67890"
        # User left password as "***" (unchanged)

        # Step 3: Merge to restore unchanged sensitive values
        final_dict = self.masker.merge(updated_dict, original_config)

        # Verify the results
        assert final_dict["database"]["password"] == "super_secret_password"  # Restored
        assert final_dict["database"]["host"] == "new-db.example.com"  # User modification kept
        assert final_dict["api"]["api_key"] == "new_api_key_67890"  # User modification kept
        assert final_dict["api"]["timeout"] == 60  # User modification kept
        assert final_dict["app_name"] == "my_application"  # Unchanged


class TestKubernetesImportAvoidance:
    """Test that secrets masker doesn't import kubernetes unnecessarily."""

    def test_no_k8s_import_when_not_needed(self):
        """Ensure kubernetes is not imported when masking non-k8s secrets."""
        # Ensure kubernetes is not already imported
        k8s_modules = [m for m in sys.modules if m.startswith("kubernetes")]
        if k8s_modules:
            pytest.skip("Kubernetes already imported, cannot test import avoidance")

        masker = SecretsMasker()
        configure_secrets_masker_for_test(masker)

        masker.add_mask("test_secret", "password")
        redacted = masker.redact({"password": "test_secret", "user": "admin"})

        assert redacted["password"] == "***"
        assert redacted["user"] == "admin"

        assert "kubernetes.client" not in sys.modules

    def test_k8s_objects_still_detected_when_imported(self):
        """Ensure V1EnvVar objects are still properly detected when k8s is imported."""
        pytest.importorskip("kubernetes")

        from kubernetes.client import V1EnvVar

        # Create a V1EnvVar object with a sensitive name
        env_var = V1EnvVar(name="password", value="secret123")

        masker = SecretsMasker()
        configure_secrets_masker_for_test(masker)

        # Redact the V1EnvVar object - the name field is sensitive
        redacted = masker.redact(env_var)

        # Should be redacted since "password" is a sensitive field name
        assert redacted["value"] == "***"
        assert redacted["name"] == "password"
