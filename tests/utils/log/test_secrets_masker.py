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
from tests.test_utils.config import conf_vars

pytestmark = pytest.mark.enable_redact
PASSWORD = "password"


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
              File ".../test_secrets_masker.py", line {line+4}, in test_masking_in_explicit_context_exceptions
                raise RuntimeError(f"Exception: {{exception}}") from exception
            RuntimeError: Exception: Cannot connect to user:***
            """
        )

    def test_redact_exception_with_context_simple(self):
        """
        Test _redact_exception_with_context_or_cause with a simple exception without context or cause.
        """
        masker = SecretsMasker()

        masker.add_mask(PASSWORD)

        exc = RuntimeError(f"Cannot connect to user:{PASSWORD}")
        masker._redact_exception_with_context_or_cause(exc)

        assert "password" not in str(exc.args[0])
        assert "user:***" in str(exc.args[0])

    def test_redact_exception_with_implicit_context(self):
        """
        Test _redact_exception_with_context with exception __context__ (implicit chaining).
        """
        masker = SecretsMasker()

        masker.add_mask(PASSWORD)

        try:
            try:
                raise RuntimeError(f"Inner error with {PASSWORD}")
            except RuntimeError:
                raise RuntimeError(f"Outer error with {PASSWORD}")
        except RuntimeError as exc:
            captured_exc = exc

        masker._redact_exception_with_context_or_cause(captured_exc)
        assert "password" not in str(captured_exc.args[0])
        assert "password" not in str(captured_exc.__context__.args[0])

    def test_redact_exception_with_explicit_cause(self):
        """
        Test _redact_exception_with_context with exception __cause__ (explicit chaining).
        """
        masker = SecretsMasker()

        masker.add_mask(PASSWORD)

        try:
            inner = RuntimeError(f"Cause error: {PASSWORD}")
            raise RuntimeError(f"Main error: {PASSWORD}") from inner
        except RuntimeError as exc:
            captured_exc = exc

        masker._redact_exception_with_context_or_cause(captured_exc)
        assert "password" not in str(captured_exc.args[0])
        assert "password" not in str(captured_exc.__cause__.args[0])

    def test_redact_exception_with_circular_context_reference(self):
        """
        Test _redact_exception_with_context handles circular references without infinite recursion.
        """
        masker = SecretsMasker()

        masker.add_mask(PASSWORD)

        exc1 = RuntimeError(f"Error with {PASSWORD}")
        exc2 = RuntimeError(f"Another error with {PASSWORD}")
        # Create circular reference
        exc1.__context__ = exc2
        exc2.__context__ = exc1

        # Should not raise RecursionError
        masker._redact_exception_with_context_or_cause(exc1)

        assert "password" not in str(exc1.args[0])
        assert "password" not in str(exc2.args[0])

    def test_redact_exception_with_max_context_recursion_depth(self):
        """
        Test _redact_exception_with_context respects MAX_RECURSION_DEPTH.
        Once the depth limit is reached, the remaining exception chain is not traversed;
        instead, it is truncated and replaced with a sentinel exception indicating the
        recursion limit was hit, and further chaining is dropped.
        """
        masker = SecretsMasker()

        masker.add_mask(PASSWORD)

        # Create a long chain of exceptions
        exc_chain = RuntimeError(f"Error 0 with {PASSWORD}")
        current = exc_chain
        for i in range(1, 10):
            new_exc = RuntimeError(f"Error {i} with {PASSWORD}")
            current.__context__ = new_exc
            current = new_exc

        masker._redact_exception_with_context_or_cause(exc_chain)

        # Verify redaction happens up to MAX_RECURSION_DEPTH
        # The check is `len(visited) >= MAX_RECURSION_DEPTH` before adding current exception
        # So it processes exactly MAX_RECURSION_DEPTH exceptions (0 through MAX_RECURSION_DEPTH-1)
        current = exc_chain
        for i in range(10):
            assert current, "We should always get some exception here"
            if i < masker.MAX_RECURSION_DEPTH:
                # Should be redacted within the depth limit
                assert "password" not in str(current.args[0]), f"Exception {i} should be redacted"
            else:
                assert "hit recursion limit" in str(
                    current.args[0]
                ), f"Exception {i} should indicate recursion depth limit hit"
                assert "password" not in str(current.args[0]), f"Exception {i} should not be present"
                assert (
                    current.__context__ is None
                ), f"Exception {i} should not have a context due to depth limit"
                break
            current = current.__context__

    def test_redact_exception_with_circular_cause_reference(self):
        """
        Test _redact_exception_with_context_or_cause handles circular __cause__ references without infinite recursion.
        """
        masker = SecretsMasker()

        masker.add_mask(PASSWORD)

        exc1 = RuntimeError(f"Error with {PASSWORD}")
        exc2 = RuntimeError(f"Another error with {PASSWORD}")
        # Create circular reference using __cause__
        exc1.__cause__ = exc2
        exc2.__cause__ = exc1

        # Should not raise RecursionError
        masker._redact_exception_with_context_or_cause(exc1)

        assert "password" not in str(exc1.args[0])
        assert "password" not in str(exc2.args[0])

    def test_redact_exception_with_max_cause_recursion_depth(self):
        """
        Test _redact_exception_with_context_or_cause respects MAX_RECURSION_DEPTH for __cause__ chains.
        Exceptions beyond the depth limit should be skipped (not redacted).
        """
        masker = SecretsMasker()

        masker.add_mask(PASSWORD)

        # Create a long chain of exceptions using __cause__
        exc_chain = RuntimeError(f"Error 0 with {PASSWORD}")
        current = exc_chain
        for i in range(1, 10):
            new_exc = RuntimeError(f"Error {i} with {PASSWORD}")
            current.__cause__ = new_exc
            current = new_exc

        masker._redact_exception_with_context_or_cause(exc_chain)

        # Verify redaction happens up to MAX_RECURSION_DEPTH
        # The check is `len(visited) >= MAX_RECURSION_DEPTH` before adding current exception
        # So it processes exactly MAX_RECURSION_DEPTH exceptions (0 through MAX_RECURSION_DEPTH-1)
        current = exc_chain
        for i in range(10):
            assert current, "We should always get some exception here"
            if i < masker.MAX_RECURSION_DEPTH:
                # Should be redacted within the depth limit
                assert "password" not in str(current.args[0]), f"Exception {i} should be redacted"
            else:
                assert "hit recursion limit" in str(
                    current.args[0]
                ), f"Exception {i} should indicate recursion depth limit hit"
                assert "password" not in str(current.args[0]), f"Exception {i} should not be present"
                assert current.__cause__ is None, f"Exception {i} should not have a cause due to depth limit"
                break
            current = current.__cause__

    def test_redact_exception_with_mixed_cause_and_context_linear(self):
        """
        Test _redact_exception_with_context_or_cause with mixed __cause__ and __context__ in a linear chain.
        This simulates: exception with cause, which has context, which has cause, etc.
        """
        masker = SecretsMasker()

        masker.add_mask(PASSWORD)

        # Build a chain: exc1 -> (cause) -> exc2 -> (context) -> exc3 -> (cause) -> exc4
        exc4 = RuntimeError(f"Error 4 with {PASSWORD}")
        exc3 = RuntimeError(f"Error 3 with {PASSWORD}")
        exc3.__cause__ = exc4
        exc2 = RuntimeError(f"Error 2 with {PASSWORD}")
        exc2.__context__ = exc3
        exc1 = RuntimeError(f"Error 1 with {PASSWORD}")
        exc1.__cause__ = exc2

        masker._redact_exception_with_context_or_cause(exc1)

        # All exceptions should be redacted
        assert "password" not in str(exc1.args[0])
        assert "password" not in str(exc2.args[0])
        assert "password" not in str(exc3.args[0])
        assert "password" not in str(exc4.args[0])

    def test_redact_exception_with_mixed_cause_and_context_branching(self):
        """
        Test with an exception that has both __cause__ and __context__ pointing to different exceptions.
        This creates a branching structure.
        """
        masker = SecretsMasker()

        masker.add_mask(PASSWORD)

        # Create branching structure:
        #     exc1
        #    /    \
        # cause  context
        #  |       |
        # exc2   exc3
        exc2 = RuntimeError(f"Cause error with {PASSWORD}")
        exc3 = RuntimeError(f"Context error with {PASSWORD}")
        exc1 = RuntimeError(f"Main error with {PASSWORD}")
        exc1.__cause__ = exc2
        exc1.__context__ = exc3

        masker._redact_exception_with_context_or_cause(exc1)

        # All three should be redacted
        assert "password" not in str(exc1.args[0])
        assert "password" not in str(exc2.args[0])
        assert "password" not in str(exc3.args[0])

    def test_redact_exception_with_mixed_circular_reference(self):
        """
        Test with circular references involving both __cause__ and __context__.
        """
        masker = SecretsMasker()

        masker.add_mask(PASSWORD)

        # Create circular mixed reference: exc1 -cause-> exc2 -context-> exc1
        exc1 = RuntimeError(f"Error 1 with {PASSWORD}")
        exc2 = RuntimeError(f"Error 2 with {PASSWORD}")
        exc1.__cause__ = exc2
        exc2.__context__ = exc1

        # Should not raise RecursionError
        masker._redact_exception_with_context_or_cause(exc1)

        assert "password" not in str(exc1.args[0])
        assert "password" not in str(exc2.args[0])

    def test_redact_exception_with_mixed_deep_chain(self):
        """
        Test with a deep chain alternating between __cause__ and __context__.
        """
        masker = SecretsMasker()

        masker.add_mask(PASSWORD)

        # Create alternating chain exceeding MAX_RECURSION_DEPTH
        exceptions = [RuntimeError(f"Error {i} with {PASSWORD}") for i in range(10)]

        # Link them: 0 -cause-> 1 -context-> 2 -cause-> 3 -context-> 4 ...
        for i in range(len(exceptions) - 1):
            if i % 2 == 0:
                exceptions[i].__cause__ = exceptions[i + 1]
            else:
                exceptions[i].__context__ = exceptions[i + 1]

        masker._redact_exception_with_context_or_cause(exceptions[0])

        # Check that first MAX_RECURSION_DEPTH are redacted, rest hit the limit
        for i in range(min(len(exceptions), masker.MAX_RECURSION_DEPTH)):
            assert "password" not in str(exceptions[i].args[0]), f"Exception {i} should be redacted"

    def test_redact_exception_with_mixed_diamond_structure(self):
        """
        Test with diamond structure: top exception has both cause and context that converge to same exception.
                exc1
               /    \
           cause  context
             |      |
           exc2   exc3
             \\     /
              exc4
        """
        masker = SecretsMasker()

        masker.add_mask(PASSWORD)

        exc4 = RuntimeError(f"Bottom error with {PASSWORD}")
        exc2 = RuntimeError(f"Left error with {PASSWORD}")
        exc2.__cause__ = exc4
        exc3 = RuntimeError(f"Right error with {PASSWORD}")
        exc3.__context__ = exc4
        exc1 = RuntimeError(f"Top error with {PASSWORD}")
        exc1.__cause__ = exc2
        exc1.__context__ = exc3

        masker._redact_exception_with_context_or_cause(exc1)

        # All should be redacted, exc4 should be visited only once
        assert "password" not in str(exc1.args[0])
        assert "password" not in str(exc2.args[0])
        assert "password" not in str(exc3.args[0])
        assert "password" not in str(exc4.args[0])

    def test_redact_exception_with_immutable_args(self):
        """
        Test _redact_exception_with_context handles exceptions with immutable args gracefully.
        """
        masker = SecretsMasker()

        masker.add_mask(PASSWORD)

        class ImmutableException(Exception):
            @property
            def args(self):
                return (f"Immutable error with {PASSWORD}",)

        exc = ImmutableException()
        # Should not raise AttributeError
        masker._redact_exception_with_context_or_cause(exc)
        # Note: Since args is immutable, it won't be redacted, but the method shouldn't crash

    def test_redact_exception_with_same_cause_and_context(self):
        """
        Test _redact_exception_with_context when __cause__ is same as __context__.
        """
        masker = SecretsMasker()

        masker.add_mask(PASSWORD)

        try:
            inner = RuntimeError(f"Base error: {PASSWORD}")
            raise RuntimeError(f"Derived error: {PASSWORD}") from inner
        except RuntimeError as exc:
            # Make __context__ same as __cause__
            exc.__context__ = exc.__cause__
            captured_exc = exc

        masker._redact_exception_with_context_or_cause(captured_exc)
        assert "password" not in str(captured_exc.args[0])
        assert "password" not in str(captured_exc.__cause__.args[0])
        # Should only process __cause__ once (optimization check)

    @pytest.mark.parametrize(
        ("name", "value", "expected_mask"),
        [
            (None, "secret", {"secret"}),
            ("apikey", "secret", {"secret"}),
            # the value for "apikey", and "password" should end up masked
            (None, {"apikey": "secret", "other": {"val": "innocent", "password": "foo"}}, {"secret", "foo"}),
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
        with patch("airflow.utils.log.secrets_masker._secrets_masker", return_value=secrets_masker):
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
            ([TaskInstanceState.FAILED, TaskInstanceState.SUCCESS], ["failed", "success"]),
            (State.failed_states, frozenset([TaskInstanceState.FAILED, TaskInstanceState.UPSTREAM_FAILED])),
            (MyEnum.testname, "testvalue"),
        ],
    )
    def test_redact_state_enum(self, logger, caplog, state, expected):
        logger.info("State: %s", state)
        assert caplog.text == f"INFO State: {expected}\n"
        assert "TypeError" not in caplog.text

    def test_masking_quoted_strings_in_connection(self, logger, caplog):
        secrets_masker = next(fltr for fltr in logger.filters if isinstance(fltr, SecretsMasker))
        with patch("airflow.utils.log.secrets_masker._secrets_masker", return_value=secrets_masker):
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

        with conf_vars({("core", "sensitive_var_conn_names"): str(sensitive_variable_fields)}):
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
        with patch("airflow.utils.log.secrets_masker._secrets_masker", return_value=self.secrets_masker):
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
        with patch("airflow.utils.log.secrets_masker._secrets_masker", return_value=self.secrets_masker):
            with patch("airflow.utils.log.secrets_masker.re2.escape", lambda x: x):
                yield

    def test_calling_mask_secret_adds_adaptations_for_returned_str(self):
        with conf_vars({("logging", "secret_mask_adapter"): "urllib.parse.quote"}):
            mask_secret("secret<>&", None)

        assert self.secrets_masker.patterns == {"secret%3C%3E%26", "secret<>&"}

    def test_calling_mask_secret_adds_adaptations_for_returned_iterable(self):
        with conf_vars({("logging", "secret_mask_adapter"): "urllib.parse.urlparse"}):
            mask_secret("https://airflow.apache.org/docs/apache-airflow/stable", "password")

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
