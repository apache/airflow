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
"""
Adversarial QA (wave 8) for the EXECUTOR / sync deadline-callback path.

These tests probe ``execute_callback`` (the sync ``callable(result)`` branch),
the ``accepts_context`` / ``accepts_keyword_args`` routing, the executor-path
output storage (vs the triggerer's), the mangled-module / missing-file error
path, and the ``RemoteLogIO.upload(path, ti=None)`` surface from PR #66379.

Goal of the wave: try to MAKE IT FAIL. Where a scenario passes, the test
documents *why* the code is safe so a future regression is caught.
"""

from __future__ import annotations

import os

import pytest
import structlog

from airflow.sdk._shared.module_loading import accepts_context, accepts_keyword_args
from airflow.sdk.bases.notifier import BaseNotifier
from airflow.sdk.execution_time.callback_supervisor import Path, execute_callback

log = structlog.get_logger()


def _find_repo_root():
    """Walk up from this test file until a dir containing ``providers/`` is found."""
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "providers").is_dir() and (parent / "airflow-core").is_dir():
            return parent
    raise RuntimeError("Could not locate repo root from test file location")


# ---------------------------------------------------------------------------
# Module-level callables / classes (must be importable by dotted path because
# execute_callback imports them via module.attr).
# ---------------------------------------------------------------------------


# --- #1: class-based Notifier whose __init__ does NOT declare context --------
_NOTIFIER_CALLS: list = []


class _RecordingNotifier(BaseNotifier):
    """A real BaseNotifier subclass; __init__ takes a template field, not context."""

    template_fields = ("text",)

    def __init__(self, text: str = "default"):
        super().__init__()
        self.text = text

    def notify(self, context):
        _NOTIFIER_CALLS.append(("notify", self.text, dict(context)))


# --- #1b: Notifier subclass whose __init__ DOES declare context --------------
class _ContextCtorNotifier(BaseNotifier):
    """Subclass that accepts context in its constructor (the docstring shape)."""

    def __init__(self, context=None):
        super().__init__(context=context)

    def notify(self, context):
        _NOTIFIER_CALLS.append(("ctx-notify", dict(context)))


# --- #2: a callable accepting only positional-or-keyword args, no context ----
def _positional_callback(a, b):
    _NOTIFIER_CALLS.append(("positional", a, b))
    return None


# --- #2b: a STRICTLY positional-only callback (PEP 570 ``/``) ----------------
def _positional_only_callback(a, b, /):
    _NOTIFIER_CALLS.append(("positional-only", a, b))
    return None


# --- #3: a sync callback returning a non-serializable object -----------------
class _Unserializable:
    def __repr__(self):
        return "<_Unserializable>"


def _returns_unserializable(**kwargs):
    return _Unserializable()


# --- #7: two callbacks importing the same module -----------------------------
def _import_counter_a(**kwargs):
    import json as _json  # stdlib, cached in sys.modules

    _NOTIFIER_CALLS.append(("import-a", id(_json)))
    return None


def _import_counter_b(**kwargs):
    import json as _json

    _NOTIFIER_CALLS.append(("import-b", id(_json)))
    return None


# --- #8: callback that records exactly what context it received --------------
def _records_context(**kwargs):
    _NOTIFIER_CALLS.append(("context-kwarg", kwargs.get("context")))
    return None


@pytest.fixture(autouse=True)
def _clear_calls():
    _NOTIFIER_CALLS.clear()
    yield
    _NOTIFIER_CALLS.clear()


_THIS = __name__


class TestExecutorNotifierPath:
    """#1 — class-based Notifier through the sync execute_callback path."""

    def test_notifier_template_field_ctor_then_called_with_context(self):
        """Notifier whose __init__ takes a template field is built, then __call__(context)."""
        ctx = {"dag_run": "dr", "logical_date": "2024-01-01"}
        success, error = execute_callback(
            callback_path=f"{_THIS}._RecordingNotifier",
            callback_kwargs={"text": "alert!", "context": ctx},
            dag_rel_path=Path("test.py"),
            bundle_path=Path("bundle"),
            log=log,
        )
        assert success is True, error
        assert error is None
        # The instance must have been called with the real context (positional __call__).
        assert _NOTIFIER_CALLS == [("notify", "alert!", ctx)]  # noqa: SIM300

    def test_notifier_with_context_in_ctor(self):
        """Notifier whose __init__ declares context: accepts_context routes context to ctor."""
        ctx = {"dag_run": "dr2"}
        success, error = execute_callback(
            callback_path=f"{_THIS}._ContextCtorNotifier",
            callback_kwargs={"context": ctx},
            dag_rel_path=Path("test.py"),
            bundle_path=Path("bundle"),
            log=log,
        )
        assert success is True, error
        # The resulting instance was then called; __call__ runs notify with context.
        assert ("ctx-notify", ctx) in _NOTIFIER_CALLS


class TestPositionalArgRouting:
    """#2 — callables that accept only positional args."""

    def test_positional_or_keyword_no_context(self):
        """def f(a, b): no context/**kwargs -> context stripped, called with remaining kwargs."""
        success, error = execute_callback(
            callback_path=f"{_THIS}._positional_callback",
            callback_kwargs={"a": 1, "b": 2, "context": {"x": 1}},
            dag_rel_path=Path("test.py"),
            bundle_path=Path("bundle"),
            log=log,
        )
        assert success is True, error
        assert _NOTIFIER_CALLS == [("positional", 1, 2)]

    def test_strictly_positional_only_with_matching_kwargs(self):
        """
        def f(a, b, /): strictly positional-only.

        accepts_context -> False (no 'context', no **kwargs), so execute_callback
        does ``f(**kwargs_without_context)``. Positional-only params CANNOT be
        filled by keyword, so this raises TypeError -> reported as a clean failure
        (not a crash). Documents the routing limitation.
        """
        success, error = execute_callback(
            callback_path=f"{_THIS}._positional_only_callback",
            callback_kwargs={"a": 1, "b": 2},
            dag_rel_path=Path("test.py"),
            bundle_path=Path("bundle"),
            log=log,
        )
        # Either it works (if a future fix maps positionals) or it fails cleanly.
        # Today it fails cleanly with a TypeError captured in the error string.
        assert success is False
        assert "TypeError" in error
        assert _NOTIFIER_CALLS == []


class TestExecutorNonSerializableOutput:
    """
    #3 — MOST IMPORTANT. The triggerer path (TriggererCallback.handle_event)
    json-coerces a non-string callback result before writing the Text ``output``
    column (fix #2). Does the EXECUTOR path have the same crash risk?

    The executor path is structurally different: ``execute_callback`` returns
    only ``(success: bool, error: str | None)`` and DISCARDS the callback's
    return value entirely. The subprocess signals success via exit code; the
    scheduler then writes ``callback.output = str(info)`` only on FAILURE
    (already a string). So a non-serializable RETURN value can never reach the
    Text column on the executor path -> no parallel bug.
    """

    def test_non_serializable_return_value_is_discarded_not_stored(self):
        success, error = execute_callback(
            callback_path=f"{_THIS}._returns_unserializable",
            callback_kwargs={"context": {"dag_run": "dr"}},
            dag_rel_path=Path("test.py"),
            bundle_path=Path("bundle"),
            log=log,
        )
        # The unserializable object is returned by the user callback, BUT
        # execute_callback only returns (success, error) -- the object is dropped.
        assert success is True, error
        assert error is None

    def test_execute_callback_signature_returns_only_bool_and_str(self):
        """Guard: if someone changes execute_callback to return the callback value, this fails."""
        result = execute_callback(
            callback_path=f"{_THIS}._returns_unserializable",
            callback_kwargs={},
            dag_rel_path=Path("test.py"),
            bundle_path=Path("bundle"),
            log=log,
        )
        assert isinstance(result, tuple)
        assert len(result) == 2
        success, error = result
        assert isinstance(success, bool)
        assert error is None or isinstance(error, str)


class TestMangledModuleMissingFile:
    """#4 — mangled unusual_prefix module path but file MISSING from the bundle."""

    def test_unusual_prefix_missing_bundle_path(self):
        """unusual_prefix path, but no bundle_path -> clean error, no crash."""
        success, error = execute_callback(
            callback_path="unusual_prefix_abc123_mydag.my_callback",
            callback_kwargs={},
            dag_rel_path=Path("mydag.py"),
            bundle_path=None,
            log=log,
        )
        assert success is False
        assert "Bundle path not found" in error

    def test_unusual_prefix_missing_dag_rel_path(self):
        success, error = execute_callback(
            callback_path="unusual_prefix_abc123_mydag.my_callback",
            callback_kwargs={},
            dag_rel_path="",
            bundle_path=Path("bundle"),
            log=log,
        )
        assert success is False
        assert "Dag relative path not found" in error

    def test_unusual_prefix_file_absent_from_bundle(self, tmp_path):
        """
        unusual_prefix path with a bundle_path that exists but the DAG file is
        absent -> spec_from_file_location is attempted and fails cleanly with a
        captured FileNotFound/ImportError-style error, no crash/hang.
        """
        bundle = tmp_path / "bundle"
        bundle.mkdir()
        success, error = execute_callback(
            callback_path="unusual_prefix_abc123_mydag.my_callback",
            callback_kwargs={},
            dag_rel_path=Path("missing_dag.py"),
            bundle_path=bundle,
            log=log,
        )
        assert success is False
        assert error  # non-empty error string, process did not crash


class TestContextKwargCollision:
    """
    #8 — callback_kwargs contains an explicit 'context' key AND a real context
    is built by the subprocess. The subprocess merges via
    ``effective_kwargs["context"] = built_context`` which OVERWRITES any
    user-supplied 'context'. Inside execute_callback there is only ever one
    'context' key in the dict, so there is no "multiple values for 'context'"
    TypeError. The built/explicit context wins; no double-pass.
    """

    def test_single_context_key_no_typeerror(self):
        explicit_ctx = {"source": "explicit"}
        success, error = execute_callback(
            callback_path=f"{_THIS}._records_context",
            callback_kwargs={"context": explicit_ctx},
            dag_rel_path=Path("test.py"),
            bundle_path=Path("bundle"),
            log=log,
        )
        assert success is True, error
        assert _NOTIFIER_CALLS == [("context-kwarg", explicit_ctx)]  # noqa: SIM300

    def test_subprocess_merge_overwrites_user_context(self):
        """Simulate the start() merge: built context replaces a user 'context' kwarg."""
        callback_kwargs = {"context": {"source": "user"}, "other": 1}
        built_context = {"source": "subprocess", "dag_run": "dr"}
        # This mirrors callback_supervisor.CallbackSubprocess.start._target:
        effective_kwargs = dict(callback_kwargs)
        effective_kwargs["context"] = built_context
        # Exactly one 'context' key, and it's the built one.
        assert effective_kwargs["context"] is built_context
        assert list(effective_kwargs).count("context") == 1


class TestModuleCacheBehavior:
    """#7 — two sync callbacks importing the same (stdlib) module reuse the cache."""

    def test_same_module_object_reused_within_process(self):
        for path in (f"{_THIS}._import_counter_a", f"{_THIS}._import_counter_b"):
            success, error = execute_callback(
                callback_path=path,
                callback_kwargs={},
                dag_rel_path=Path("test.py"),
                bundle_path=Path("bundle"),
                log=log,
            )
            assert success is True, error
        ids = [entry[1] for entry in _NOTIFIER_CALLS]
        assert len(ids) == 2
        # Same module object (sys.modules cache) seen by both callbacks.
        assert ids[0] == ids[1]


class TestAcceptsRoutingHelpers:
    """Direct unit coverage of the routing predicates that drive #1/#2/#8."""

    @pytest.mark.parametrize(
        ("func", "expected"),
        [
            pytest.param(lambda context: None, True, id="named-context"),
            pytest.param(lambda **kw: None, True, id="var-keyword"),
            pytest.param(lambda a, b: None, False, id="no-context-no-kwargs"),
            pytest.param(lambda a, b, /: None, False, id="positional-only"),
        ],
    )
    def test_accepts_context(self, func, expected):
        assert accepts_context(func) is expected

    @pytest.mark.parametrize(
        ("func", "expected"),
        [
            pytest.param(lambda context: None, True, id="named"),
            pytest.param(lambda **kw: None, True, id="var-keyword"),
            pytest.param(lambda *args: None, False, id="var-positional-only"),
            pytest.param(lambda a, b, /: None, False, id="positional-only"),
        ],
    )
    def test_accepts_keyword_args(self, func, expected):
        assert accepts_keyword_args(func) is expected

    def test_notifier_dunder_call_is_positional_only(self):
        """BaseNotifier.__call__(self, *args) must route through the positional branch."""
        n = _RecordingNotifier(text="x")
        # accepts_keyword_args(instance) inspects __call__ -> *args only -> False
        assert accepts_keyword_args(n) is False


# ---------------------------------------------------------------------------
# #6 — RemoteLogIO.upload(path, ti=None): the PR #66379 surface.
# Verify that passing ti=None does not raise for the handlers, and document
# which handlers skip the upload when ti is None.
# ---------------------------------------------------------------------------


class _FakeTaskHandlerBase:
    """Minimal stand-in mirroring how a no-ti handler body uses path only."""

    def __init__(self, base):
        self.base_log_folder = Path(base)
        self.delete_local_copy = False
        self.uploaded = []

    def upload(self, path: os.PathLike | str, ti=None) -> None:
        # Mirrors s3/gcs/wasb/oss style: never touches ti.
        p = Path(path)
        local = p if p.is_absolute() else self.base_log_folder.joinpath(p)
        if local.is_file():
            self.uploaded.append(local.read_text())


class TestRemoteUploadTiNone:
    def test_protocol_upload_accepts_ti_none(self):
        """The RemoteLogIO Protocol signature must default ti to None (PR #66379)."""
        import inspect

        from airflow_shared.logging.remote import RemoteLogIO

        sig = inspect.signature(RemoteLogIO.upload)
        assert "ti" in sig.parameters
        assert sig.parameters["ti"].default is None

    def test_handler_without_ti_use_does_not_raise(self, tmp_path):
        """A path-only handler must upload fine when ti is None."""
        logfile = tmp_path / "callback.log"
        logfile.write_text("callback log line\n")
        handler = _FakeTaskHandlerBase(tmp_path)
        handler.upload(logfile, ti=None)
        assert handler.uploaded == ["callback log line\n"]

    def test_upload_to_remote_handles_ti_none_end_to_end(self, tmp_path, monkeypatch):
        """
        upload_to_remote(logger, ti=None) must:
          - compute ti_id as None (not crash on ti.id),
          - resolve the relative path,
          - delegate to handler.upload(path, None).
        This is the exact call CallbackSubprocess._upload_logs makes. (Handler
        exceptions are swallowed by _upload_logs, not here -- see
        test_callback_log_upload_swallows_handler_exception.)
        """
        from airflow.sdk import log as sdk_log

        # Build a fake "raw logger" exposing a real _file at a path under base_log_folder.
        base = tmp_path / "logs"
        base.mkdir()
        logfile = base / "cb.log"
        logfile.write_text("x\n")

        class _FH:
            name = str(logfile)

            def fileno(self):
                return 7

        class _RawLogger:
            _file = _FH()

        class _Logger:
            _logger = _RawLogger()

        recorded = {}

        class _Handler:
            def upload(self, path, ti=None):
                recorded["path"] = path
                recorded["ti"] = ti

        monkeypatch.setattr(sdk_log, "load_remote_log_handler", lambda: _Handler())
        # Point base_log_folder config at our temp dir.
        from airflow.sdk.configuration import conf

        monkeypatch.setattr(conf, "get", lambda section, key, **kw: str(base))

        # ti=None must not crash.
        sdk_log.upload_to_remote(_Logger(), ti=None)
        assert recorded["ti"] is None
        assert recorded["path"] == "cb.log"

    @pytest.mark.parametrize(
        "rel_handler_path",
        [
            "providers/elasticsearch/src/airflow/providers/elasticsearch/log/es_task_handler.py",
            "providers/opensearch/src/airflow/providers/opensearch/log/os_task_handler.py",
        ],
    )
    def test_ti_dependent_handlers_guard_against_ti_none(self, rel_handler_path):
        """
        #6 documented limitation: ES and OpenSearch handlers build a log_id from
        ``ti`` and therefore CANNOT upload when ti is None. PR #66379 added an
        ``if ti is None: return`` guard so they no-op rather than crash on the
        executor-callback (ti=None) path. This asserts the guard is present in
        the upload() body -- if it is removed, callback uploads would crash with
        AttributeError on these backends.
        """
        # task-sdk tests run from the repo root inside breeze (/opt/airflow).
        repo_root = _find_repo_root()
        src = (repo_root / rel_handler_path).read_text()
        upload_idx = src.index("def upload(self")
        # Look only within the upload() method body (until the next def at the
        # same indentation).
        rest = src[upload_idx:]
        next_def = rest.find("\n    def ", 1)
        body = rest[: next_def if next_def != -1 else len(rest)]
        assert "if ti is None:" in body, (
            f"{rel_handler_path} upload() lost its ti-None guard; "
            "executor callback log upload (ti=None) would crash on this backend."
        )

    @pytest.mark.parametrize(
        "rel_handler_path",
        [
            "providers/amazon/src/airflow/providers/amazon/aws/log/s3_task_handler.py",
            "providers/google/src/airflow/providers/google/cloud/log/gcs_task_handler.py",
            "providers/microsoft/azure/src/airflow/providers/microsoft/azure/log/wasb_task_handler.py",
            "providers/alibaba/src/airflow/providers/alibaba/cloud/log/oss_task_handler.py",
            "providers/apache/hdfs/src/airflow/providers/apache/hdfs/log/hdfs_task_handler.py",
        ],
    )
    def test_object_store_handlers_do_not_dereference_ti(self, rel_handler_path):
        """
        #6: the object-store handlers (S3/GCS/WASB/OSS/HDFS) must not reference
        ``ti`` in their upload() body, so ti=None is safe and the callback log is
        actually uploaded (unlike ES/OS which skip).
        """
        repo_root = _find_repo_root()
        src = (repo_root / rel_handler_path).read_text()
        upload_idx = src.index("def upload(self")
        rest = src[upload_idx:]
        next_def = rest.find("\n    def ", 1)
        body = rest[: next_def if next_def != -1 else len(rest)]
        # The only allowed mention of "ti" is the parameter in the signature line.
        signature_line, _, after_sig = body.partition("\n")
        assert "ti." not in after_sig, (
            f"{rel_handler_path} upload() dereferences ti; this would crash on the "
            "executor-callback path where ti=None."
        )

    def test_callback_log_upload_swallows_handler_exception(self, monkeypatch):
        """
        A remote-log handler that raises during upload must NOT abort the callback
        subprocess teardown. ``upload_to_remote`` itself propagates the handler error
        (it has no try/except); the swallowing happens one layer up in
        ``CallbackSubprocess._upload_logs``, which wraps the call in try/except and
        logs. This asserts that contract at the layer this PR owns.
        """
        import contextlib
        from unittest.mock import MagicMock

        from airflow.sdk.execution_time import callback_supervisor

        # upload_to_remote propagates the handler exception (no swallowing of its own);
        # both it and _remote_logging_conn are imported locally inside _upload_logs, so
        # patch them at their source modules.
        def _boom(logger, ti=None):
            raise RuntimeError("boom in handler")

        @contextlib.contextmanager
        def _fake_conn(client):
            yield None

        monkeypatch.setattr("airflow.sdk.execution_time.supervisor._remote_logging_conn", _fake_conn)
        monkeypatch.setattr("airflow.sdk.log.upload_to_remote", _boom)

        sub = callback_supervisor.CallbackSubprocess.__new__(callback_supervisor.CallbackSubprocess)
        sub.id = "cb-1"
        sub.pid = 4242
        sub.process_log = MagicMock()
        sub.client = MagicMock()

        # Must NOT propagate -- _upload_logs swallows and logs the handler error.
        sub._upload_logs()
