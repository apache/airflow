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

from airflow.models import Connection
from airflow.utils.log.secrets_masker import (
    RedactedIO,
    SecretsMasker,
    mask_secret,
    redact,
    should_hide_value_for_key,
)
from airflow.utils.state import DagRunState, JobState, State, TaskInstanceState

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.enable_redact
p = "password"


class MyEnum(str, Enum):
    testname = "testvalue"


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
            logger.handlers[0],
            "formatter",
            ShortExcFormatter("%(levelname)s %(message)s %(conn)s"),
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
        source. I have no idead if that is even possible)

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
                    raise RuntimeError(f"Cannot connect to user:{p}")
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
            raise RuntimeError(f"Cannot connect to user:{p}")
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
                raise RuntimeError(f"Cannot connect to user:{{p}}")
            RuntimeError: Cannot connect to user:***

            The above exception was the direct cause of the following exception:

            Traceback (most recent call last):
              File ".../test_secrets_masker.py", line {line+4}, in test_masking_in_explicit_context_exceptions
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
            (
                None,
                {"apikey": "secret", "other": {"val": "innocent", "password": "foo"}},
                {"secret", "foo"},
            ),
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
            (
                {"secret", "other"},
                None,
                {"data": {"secret": "secret"}},
                {"data": {"secret": "***"}},
            ),
            # Non string dict keys
            (
                {"secret", "other"},
                None,
                {1: {"secret": "secret"}},
                {1: {"secret": "***"}},
            ),
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
        for val in patterns:
            filt.add_mask(val)

        assert filt.redact(value, name) == expected

    def test_redact_filehandles(self, caplog):
        filt = SecretsMasker()
        with open("/dev/null", "w") as handle:
            assert filt.redact(handle, None) == handle

        # We shouldn't have logged a warning here
        assert caplog.messages == []

    @pytest.mark.parametrize(
        ("val", "expected", "max_depth"),
        [
            (["abc"], ["***"], None),
            (["abc"], ["***"], 1),
            ([[[["abc"]]]], [[[["***"]]]], None),
            ([[[[["abc"]]]]], [[[[["***"]]]]], None),
            # Items below max depth aren't redacted
            ([[[[[["abc"]]]]]], [[[[[["abc"]]]]]], None),
            ([["abc"]], [["abc"]], 1),
        ],
    )
    def test_redact_max_depth(self, val, expected, max_depth):
        secrets_masker = SecretsMasker()
        secrets_masker.add_mask("abc")
        with patch(
            "airflow.utils.log.secrets_masker._secrets_masker",
            return_value=secrets_masker,
        ):
            got = redact(val, max_depth=max_depth)
            assert got == expected

    def test_redact_with_str_type(self, logger, caplog):
        """
        SecretsMasker's re2 replacer has issues handling a redactable item of type
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
        "state, expected",
        [
            (DagRunState.SUCCESS, "success"),
            (TaskInstanceState.FAILED, "failed"),
            (JobState.RUNNING, "running"),
            ([DagRunState.SUCCESS, DagRunState.RUNNING], ["success", "running"]),
            (
                [TaskInstanceState.FAILED, TaskInstanceState.SUCCESS],
                ["failed", "success"],
            ),
            (
                State.failed_states,
                frozenset([TaskInstanceState.FAILED, TaskInstanceState.UPSTREAM_FAILED]),
            ),
            (MyEnum.testname, "testvalue"),
        ],
    )
    def test_redact_state_enum(self, logger, caplog, state, expected):
        logger.info("State: %s", state)
        assert caplog.text == f"INFO State: {expected}\n"
        assert "TypeError" not in caplog.text

    def test_masking_quoted_strings_in_connection(self, logger, caplog):
        secrets_masker = next(
            fltr for fltr in logger.filters if isinstance(fltr, SecretsMasker)
        )
        with patch(
            "airflow.utils.log.secrets_masker._secrets_masker",
            return_value=secrets_masker,
        ):
            test_conn_attributes = dict(
                conn_type="scheme",
                host="host/location",
                schema="schema",
                login="user",
                password="should_be_hidden!",
                port=1234,
                extra=None,
            )
            conn = Connection(**test_conn_attributes)
            logger.info(conn.get_uri())
            assert "should_be_hidden" not in caplog.text


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
        assert expected_result == should_hide_value_for_key(key)

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
        from airflow.utils.log.secrets_masker import get_sensitive_variables_fields

        with conf_vars(
            {("core", "sensitive_var_conn_names"): str(sensitive_variable_fields)}
        ):
            get_sensitive_variables_fields.cache_clear()
            try:
                assert expected_result == should_hide_value_for_key(key)
            finally:
                get_sensitive_variables_fields.cache_clear()


class ShortExcFormatter(logging.Formatter):
    """Don't include full path in exc_info messages"""

    def formatException(self, exc_info):
        formatted = super().formatException(exc_info)
        return formatted.replace(__file__, ".../" + os.path.basename(__file__))


def lineno():
    """Returns the current line number in our program."""
    return inspect.currentframe().f_back.f_lineno


class TestRedactedIO:
    @pytest.fixture(scope="class", autouse=True)
    def reset_secrets_masker(self):
        self.secrets_masker = SecretsMasker()
        with patch(
            "airflow.utils.log.secrets_masker._secrets_masker",
            return_value=self.secrets_masker,
        ):
            mask_secret(p)
            yield

    def test_redacts_from_print(self, capsys):
        # Without redacting, password is printed.
        print(p)
        stdout = capsys.readouterr().out
        assert stdout == f"{p}\n"
        assert "***" not in stdout

        # With context manager, password is redacted.
        with contextlib.redirect_stdout(RedactedIO()):
            print(p)
        stdout = capsys.readouterr().out
        assert stdout == "***\n"

    def test_write(self, capsys):
        RedactedIO().write(p)
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
        with patch(
            "airflow.utils.log.secrets_masker._secrets_masker",
            return_value=self.secrets_masker,
        ):
            with patch("airflow.utils.log.secrets_masker.re2.escape", lambda x: x):
                yield

    def test_calling_mask_secret_adds_adaptations_for_returned_str(self):
        with conf_vars({("logging", "secret_mask_adapter"): "urllib.parse.quote"}):
            mask_secret("secret<>&", None)

        assert self.secrets_masker.patterns == {"secret%3C%3E%26", "secret<>&"}

    def test_calling_mask_secret_adds_adaptations_for_returned_iterable(self):
        with conf_vars({("logging", "secret_mask_adapter"): "urllib.parse.urlparse"}):
            mask_secret(
                "https://airflow.apache.org/docs/apache-airflow/stable", "password"
            )

        assert self.secrets_masker.patterns == {
            "https",
            "airflow.apache.org",
            "/docs/apache-airflow/stable",
            "https://airflow.apache.org/docs/apache-airflow/stable",
        }

    def test_calling_mask_secret_not_set(self):
        with conf_vars({("logging", "secret_mask_adapter"): None}):
            mask_secret("a secret")

        assert self.secrets_masker.patterns == {"a secret"}
