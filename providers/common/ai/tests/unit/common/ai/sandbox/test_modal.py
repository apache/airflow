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

import sys
import threading

import pytest

from airflow.providers.common.ai.sandbox.base import (
    SandboxError,
    SandboxSpec,
    SandboxTerminalError,
)

from unit.common.ai.sandbox.fake_modal import (
    FakeProcess,
    FileInfo,
    build_fake_modal,
)


@pytest.fixture
def modal_module(monkeypatch):
    """
    Install the fake ``modal`` and hand back a freshly imported backend module bound to it.

    The backend imports ``modal`` at module scope, so the import has to happen while the
    fake is in ``sys.modules``. Re-importing also keeps the tests honest about not sharing
    state, and lets the suite run whether or not the real SDK is installed.
    """
    fake = build_fake_modal()
    monkeypatch.setitem(sys.modules, "modal", fake)
    monkeypatch.setitem(sys.modules, "modal.exception", fake.exception)
    monkeypatch.delitem(sys.modules, "airflow.providers.common.ai.sandbox.modal", raising=False)
    import airflow.providers.common.ai.sandbox.modal as backend_module

    yield fake, backend_module
    # The next test re-imports from scratch, so nothing here should linger.
    sys.modules.pop("airflow.providers.common.ai.sandbox.modal", None)


@pytest.fixture
def fake(modal_module):
    return modal_module[0]


@pytest.fixture
def backend_class(modal_module):
    return modal_module[1].ModalSandboxBackend


@pytest.fixture
def backend(backend_class):
    return backend_class(app_name="test-app", sandbox_timeout=600, idle_timeout=60)


def _created(backend, fake, spec=None):
    """Create one sandbox and return ``(handle, fake_sandbox)``."""
    handle = backend.create(spec=spec)
    return handle, fake.Sandbox.by_id[handle]


class TestVendorContract:
    """
    Guards the calls the fake cannot: the fake accepts any argument, by design.

    Skipped where the extra is not installed, which is most CI jobs, so this is a
    tripwire for a Modal release that renames something rather than a gate.
    """

    @pytest.fixture
    def real_modal(self):
        return pytest.importorskip("modal", reason="needs the 'modal' extra")

    def test_create_accepts_every_argument_the_backend_passes(self, real_modal):
        import inspect

        parameters = inspect.signature(real_modal.Sandbox.create).parameters
        for name in (
            "app",
            "image",
            "name",
            "tags",
            "timeout",
            "idle_timeout",
            "workdir",
            "env",
            "cpu",
            "memory",
            "gpu",
            "region",
            "cloud",
            "block_network",
            "outbound_domain_allowlist",
        ):
            assert name in parameters, f"Sandbox.create no longer takes {name!r}"

    def test_the_calls_behind_the_tools_still_exist(self, real_modal):
        import inspect

        from modal.sandbox import _SandboxFilesystem

        assert list(inspect.signature(_SandboxFilesystem.write_bytes).parameters)[1:3] == [
            "data",
            "remote_path",
        ], "write_bytes takes data first; swapping them type-checks and corrupts every write"
        for name in ("read_bytes", "list_files", "stat"):
            assert hasattr(_SandboxFilesystem, name)
        assert "timeout" in inspect.signature(real_modal.Sandbox.exec).parameters
        assert hasattr(real_modal.Sandbox, "from_id")

    def test_the_exception_hierarchy_the_classifier_relies_on_holds(self, real_modal):
        exception = real_modal.exception
        assert issubclass(exception.ConflictError, exception.InvalidError)
        assert issubclass(exception.SandboxTimeoutError, exception.TimeoutError)
        assert issubclass(exception.ExecTimeoutError, exception.TimeoutError)
        assert not issubclass(exception.SandboxFilesystemNotFoundError, exception.NotFoundError), (
            "a missing file must not be classified as a missing sandbox"
        )
        assert not issubclass(exception.PermissionDeniedError, exception.AuthError)


class TestInit:
    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            ({"image": ""}, "image"),
            ({"app_name": ""}, "app_name"),
            ({"sandbox_timeout": 0}, "sandbox_timeout"),
            ({"sandbox_timeout": -5}, "sandbox_timeout"),
            ({"idle_timeout": 0}, "idle_timeout"),
            ({"idle_timeout": 7200}, "idle_timeout"),
            ({"sandbox_timeout": 60}, "at least 120"),
            ({"workdir": "relative/path"}, "absolute"),
            ({"cpu": 0}, "cpu"),
            ({"memory": -1}, "memory"),
            ({"egress_enforcement": "nope"}, "egress_enforcement"),
        ],
    )
    def test_rejects_invalid_configuration(self, backend_class, kwargs, match):
        with pytest.raises(ValueError, match=match):
            backend_class(**kwargs)

    def test_defaults_leave_idle_reclamation_off(self, backend_class, fake):
        """
        One sandbox serves a whole run and nothing keeps it warm between tool calls.

        Live against modal 1.5.5, a 100s gap with ``idle_timeout=60`` reclaimed the sandbox
        and took the agent's files with it, so this is off unless a Dag author asks for it.
        """
        backend = backend_class()
        _, sandbox = _created(backend, fake, SandboxSpec())

        assert sandbox.create_kwargs["idle_timeout"] is None

    def test_construction_touches_nothing_remote(self, backend_class, fake):
        """Constructors run at Dag-parse time, so nothing may be looked up or authenticated."""
        backend_class(image="python:3.13-slim", workdir="/srv")

        assert fake.App.lookups == []
        assert fake.Sandbox.created == []
        assert fake.Image.registries == []


class TestSpecEnforcement:
    """A backend must refuse a restriction it cannot actually apply."""

    def test_block_network_maps_exactly(self, backend, fake):
        _, sandbox = _created(backend, fake, SandboxSpec())

        assert sandbox.create_kwargs["block_network"] is True
        assert "outbound_domain_allowlist" not in sandbox.create_kwargs

    def test_open_network_sets_neither_flag(self, backend, fake):
        _, sandbox = _created(backend, fake, SandboxSpec(block_network=False))

        assert "block_network" not in sandbox.create_kwargs
        assert "outbound_domain_allowlist" not in sandbox.create_kwargs

    def test_allowlist_is_refused_by_default(self, backend, fake):
        spec = SandboxSpec(block_network=True, allow_egress_to=["pypi.org"])

        with pytest.raises(SandboxTerminalError, match="egress_enforcement='sni'"):
            backend.create(spec=spec)
        assert fake.Sandbox.created == [], "refused before anything was provisioned"

    def test_allowlist_message_names_what_is_not_enforced(self, backend):
        """The Dag author has to learn that DNS stays open, not just that this is approximate."""
        with pytest.raises(SandboxTerminalError, match="DNS"):
            backend.create(spec=SandboxSpec(block_network=True, allow_egress_to=["pypi.org"]))

    def test_allowlist_under_sni_becomes_a_domain_allowlist(self, backend_class, fake):
        backend = backend_class(egress_enforcement="sni")

        _, sandbox = _created(backend, fake, SandboxSpec(block_network=True, allow_egress_to=["a", "b"]))

        assert sandbox.create_kwargs["outbound_domain_allowlist"] == ["a", "b"]
        # Modal rejects the two together, and an allowlist alone already denies the rest.
        assert "block_network" not in sandbox.create_kwargs

    def test_allowlist_without_block_network_is_contradictory(self, backend_class, fake):
        backend = backend_class(egress_enforcement="sni")

        with pytest.raises(SandboxTerminalError, match="block_network"):
            backend.create(spec=SandboxSpec(block_network=False, allow_egress_to=["pypi.org"]))
        assert fake.Sandbox.created == []

    def test_empty_allowlist_with_block_network_means_no_egress(self, backend, fake):
        """An empty allowlist is 'no egress at all' per the contract, not an allowlist."""
        _, sandbox = _created(backend, fake, SandboxSpec(block_network=True, allow_egress_to=[]))

        assert sandbox.create_kwargs["block_network"] is True

    @pytest.mark.parametrize(
        "host",
        [
            "*",
            "https://pypi.org",
            "pypi.org:443",
            "pypi.org/simple",
            "pypi .org",
            "",
            "*.*.pypi.org",
        ],
    )
    def test_refuses_an_allowlist_entry_that_is_not_a_hostname(self, backend_class, fake, host):
        """
        Modal sends these strings on unvalidated and matches them against the TLS hostname.

        A URL or a ``host:port`` would match nothing and a bare ``*`` would match
        everything, either way telling the author egress is restricted when it is not
        restricted the way they wrote it.
        """
        backend = backend_class(egress_enforcement="sni")

        with pytest.raises(SandboxTerminalError, match="hostnames"):
            backend.create(spec=SandboxSpec(block_network=True, allow_egress_to=[host]))
        assert fake.Sandbox.created == []

    @pytest.mark.parametrize("host", ["10.0.0.5", "169.254.169.254", "443", "::1"])
    def test_refuses_an_address_where_a_name_belongs(self, backend_class, fake, host):
        """A client sends no TLS hostname for an address, so allowlisting one allows nothing."""
        backend = backend_class(egress_enforcement="sni")

        with pytest.raises(SandboxTerminalError, match="hostnames"):
            backend.create(spec=SandboxSpec(block_network=True, allow_egress_to=[host]))

    def test_refuses_a_bare_string_instead_of_a_list(self, backend_class, fake):
        """
        ``str`` satisfies ``Sequence[str]``, so this would be read one character at a time.

        A name without dots would then pass every character check and allow nothing, which
        is the silent mismatch this validation exists to prevent.
        """
        backend = backend_class(egress_enforcement="sni")

        with pytest.raises(SandboxTerminalError, match="not one string"):
            backend.create(spec=SandboxSpec(block_network=True, allow_egress_to="pypiorg"))
        assert fake.Sandbox.created == []

    @pytest.mark.parametrize("host", ["pypi.org", "*.pythonhosted.org", "a-b.example.co.uk", "localhost"])
    def test_accepts_hostnames_and_wildcard_labels(self, backend_class, fake, host):
        backend = backend_class(egress_enforcement="sni")

        _, sandbox = _created(backend, fake, SandboxSpec(block_network=True, allow_egress_to=[host]))

        assert sandbox.create_kwargs["outbound_domain_allowlist"] == [host]

    def test_no_spec_states_no_requirements(self, backend, fake):
        _, sandbox = _created(backend, fake, None)

        assert "block_network" not in sandbox.create_kwargs
        assert sandbox.create_kwargs["env"] is None


class TestEnvironment:
    def test_env_reaches_the_sandbox(self, backend, fake):
        _, sandbox = _created(backend, fake, SandboxSpec(env={"HF_TOKEN": "secret"}))

        assert sandbox.create_kwargs["env"] == {"HF_TOKEN": "secret"}

    def test_no_env_injects_nothing(self, backend, fake):
        _, sandbox = _created(backend, fake, SandboxSpec())

        assert sandbox.create_kwargs["env"] is None

    @pytest.mark.parametrize("env", [{"PORT": 8080}, {"FLAG": True}, {"NOTHING": None}, {5: "x"}])
    def test_refuses_a_non_string_env_entry(self, backend, fake, env):
        """
        ``SandboxSpec.env`` is typed as strings and nothing enforces it.

        Refused here, with the key named, rather than failing deeper in Modal's own
        serialization where the message would not say which entry was wrong.
        """
        with pytest.raises(SandboxTerminalError, match="strings"):
            backend.create(spec=SandboxSpec(env=env))  # type: ignore[arg-type]
        assert fake.Sandbox.created == [], "refused before anything was provisioned"


class TestCreate:
    def test_passes_configuration_through(self, backend_class, fake):
        backend = backend_class(
            image="python:3.13-slim",
            app_name="my-app",
            sandbox_timeout=900,
            idle_timeout=120,
            workdir="/srv",
            cpu=2,
            memory=4096,
            gpu="A10G",
            region="us-east",
            cloud="aws",
            tags={"dag_id": "my_dag"},
        )

        handle, sandbox = _created(backend, fake, SandboxSpec(env={"TOKEN": "x"}))

        assert fake.App.lookups == [("my-app", True)]
        assert fake.Image.registries == ["python:3.13-slim"]
        assert sandbox.create_kwargs["timeout"] == 900
        assert sandbox.create_kwargs["idle_timeout"] == 120
        assert sandbox.create_kwargs["workdir"] == "/srv"
        assert sandbox.create_kwargs["cpu"] == 2
        assert sandbox.create_kwargs["memory"] == 4096
        assert sandbox.create_kwargs["gpu"] == "A10G"
        assert sandbox.create_kwargs["region"] == "us-east"
        assert sandbox.create_kwargs["cloud"] == "aws"
        assert sandbox.create_kwargs["env"] == {"TOKEN": "x"}
        assert handle == sandbox.object_id

    def test_accepts_a_prepared_image_as_well_as_a_registry_tag(self, backend_class, fake):
        """
        An image carrying the packages an agent needs is the alternative to opening egress.

        A default sandbox has the standard library and no DNS, so installing a package at
        run time cannot work; baking it into the image means it does not have to.
        """
        prepared = object()
        backend = backend_class(image=prepared)

        _, sandbox = _created(backend, fake, SandboxSpec())

        assert sandbox.create_kwargs["image"] is prepared
        assert fake.Image.registries == [], "a prepared image is not looked up as a tag"

    def test_names_and_tags_the_sandbox_for_correlation(self, backend, fake):
        _, sandbox = _created(backend, fake, SandboxSpec())

        name = sandbox.create_kwargs["name"]
        assert name.startswith("airflow-sandbox-")
        assert sandbox.create_kwargs["tags"]["airflow_sandbox"] == name

    def test_caller_tags_are_kept(self, backend_class, fake):
        backend = backend_class(tags={"dag_id": "d", "task_id": "t"})

        _, sandbox = _created(backend, fake, SandboxSpec())

        assert sandbox.create_kwargs["tags"]["dag_id"] == "d"
        assert sandbox.create_kwargs["tags"]["task_id"] == "t"

    def test_app_is_looked_up_per_create_so_concurrent_runs_cannot_race(self, backend, fake):
        """One backend instance is shared by concurrent runs, so no lookup result is memoized."""
        handles: list[str] = []
        threads = [threading.Thread(target=lambda: handles.append(backend.create())) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        assert len(set(handles)) == 4, "each run gets its own sandbox"
        assert len(fake.App.lookups) == 4
        for handle in handles:
            assert backend._sandboxes[handle].object_id == handle

    def test_credential_failure_is_terminal(self, backend, fake):
        fake.Sandbox.create_error = fake.exception.AuthError("bad token")

        with pytest.raises(SandboxTerminalError, match="credentials"):
            backend.create(spec=SandboxSpec())

    @pytest.mark.parametrize("error_name", ["ConnectionError", "Error", "AuthError", "NotFoundError"])
    def test_every_create_failure_is_terminal(self, backend, fake, error_name):
        """
        Nothing raised from create can reach the model, so nothing may be labelled as if it could.

        The toolset provisions the sandbox outside the block that turns a recoverable error
        into a ModelRetry (``toolsets/sandbox.py``), and no prompt fixes a bad image or an
        unreachable control plane anyway. Airflow's own retry is the right handler.
        """
        fake.Sandbox.create_error = getattr(fake.exception, error_name)("nope")

        with pytest.raises(SandboxTerminalError):
            backend.create(spec=SandboxSpec())

    def test_a_missing_app_is_not_reported_as_a_dead_sandbox(self, backend, fake):
        fake.Sandbox.create_error = fake.exception.NotFoundError("no such image")

        with pytest.raises(SandboxTerminalError, match="could not find"):
            backend.create(spec=SandboxSpec())


class TestRunCommand:
    def test_runs_through_a_shell_and_returns_both_streams(self, backend, fake):
        handle, sandbox = _created(backend, fake)
        sandbox.process = FakeProcess(returncode=0, stdout=[b"out\n"], stderr=[b"err\n"])

        result = backend.run_command(handle, "echo out", timeout=30, max_output_bytes=1024)

        assert (result.exit_code, result.stdout, result.stderr) == (0, "out\n", "err\n")
        assert result.timed_out is False
        exec_call = next(call for call in sandbox.calls if call[0] == "exec")
        assert exec_call[1] == ("sh", "-c", "echo out")
        assert exec_call[2]["timeout"] == 30
        assert exec_call[2]["text"] is False, "bytes, so the cap is applied in bytes"

    @pytest.mark.parametrize(
        ("timeout", "expected"),
        [(0.4, 1), (1.0, 1), (1.2, 2), (29.9, 30)],
    )
    def test_rounds_the_deadline_up_to_whole_seconds(self, backend, fake, timeout, expected):
        """Modal takes integer seconds and reads 0 as 'no timeout', so rounding down would unbound it."""
        handle, sandbox = _created(backend, fake)

        backend.run_command(handle, "true", timeout=timeout, max_output_bytes=1024)

        assert next(c for c in sandbox.calls if c[0] == "exec")[2]["timeout"] == expected

    def test_deadline_is_reported_as_a_timeout_and_keeps_the_sandbox(self, backend, fake):
        """Modal signals a deadline with returncode -1, not an exception, and does not tear down."""
        handle, sandbox = _created(backend, fake)
        sandbox.process = FakeProcess(returncode=-1)

        result = backend.run_command(handle, "sleep 30", timeout=2, max_output_bytes=1024)

        assert result.timed_out is True
        assert result.sandbox_terminated is False, "files from earlier calls are still there"

    def test_sigkill_well_inside_the_budget_is_not_a_timeout(self, backend, fake):
        """A guest killing itself lands nowhere near its deadline: measured at 0.04s of 30s."""
        handle, sandbox = _created(backend, fake)
        sandbox.process = FakeProcess(returncode=137, stderr=[b"Killed\n"])

        result = backend.run_command(handle, "hog", timeout=30, max_output_bytes=1024)

        assert (result.exit_code, result.timed_out) == (137, False)

    def test_a_deadline_reported_as_sigkill_is_still_a_timeout(self, backend, fake, monkeypatch):
        """
        Modal does not always report a deadline the same way.

        The same ``sleep 30`` at a two second budget has been seen returning 137 instead of
        -1, at the same elapsed time and from the same SDK version, in about a quarter of
        runs in some environments. Reading the status alone would hand the model a bare
        ``[exit code: 137]`` for a command that simply ran long, so elapsed time decides.
        """
        handle, sandbox = _created(backend, fake)
        sandbox.process = FakeProcess(returncode=137)
        ticking = [0.0]

        def advancing_clock() -> float:
            ticking[0] += 1.5
            return ticking[0]

        monkeypatch.setattr("airflow.providers.common.ai.sandbox.modal.time.monotonic", advancing_clock)

        result = backend.run_command(handle, "sleep 30", timeout=1, max_output_bytes=1024)

        assert result.timed_out is True
        assert result.exit_code == 137

    def test_exec_timeout_exception_is_still_handled(self, backend, fake):
        """Not raised by modal 1.5.5, whose own comment says it eventually should be."""
        handle, sandbox = _created(backend, fake)
        sandbox.process = FakeProcess(wait_error=fake.exception.ExecTimeoutError("deadline"))

        result = backend.run_command(handle, "sleep 30", timeout=2, max_output_bytes=1024)

        assert result.timed_out is True

    def test_nonzero_exit_is_output_not_an_error(self, backend, fake):
        handle, sandbox = _created(backend, fake)
        sandbox.process = FakeProcess(returncode=3, stderr=[b"boom\n"])

        result = backend.run_command(handle, "false", timeout=30, max_output_bytes=1024)

        assert (result.exit_code, result.stderr) == (3, "boom\n")

    def test_keeps_the_tail_when_output_exceeds_the_cap(self, backend, fake):
        handle, sandbox = _created(backend, fake)
        chunks = [f"line-{i}\n".encode() for i in range(1, 1001)]
        sandbox.process = FakeProcess(stdout=chunks)

        result = backend.run_command(handle, "spew", timeout=30, max_output_bytes=40)

        assert result.stdout_truncated is True
        assert len(result.stdout.encode()) <= 40
        assert result.stdout.strip().endswith("line-1000"), "the tail carries the error and the status"
        assert "line-1\n" not in result.stdout

    def test_caps_a_single_chunk_larger_than_the_whole_budget(self, backend, fake):
        """One transport chunk can exceed the cap on its own, and must still be cut."""
        handle, sandbox = _created(backend, fake)
        sandbox.process = FakeProcess(stdout=[b"x" * 5000 + b"\ntail\n"])

        result = backend.run_command(handle, "spew", timeout=30, max_output_bytes=64)

        assert result.stdout_truncated is True
        assert len(result.stdout.encode()) <= 64
        assert result.stdout.strip().endswith("tail")

    @pytest.mark.parametrize(
        "chunk",
        [
            b"x" * 60000 + b"\n",
            b"x" * 60000,
            b'{"one":"very long line"}' * 5000 + b"\n",
        ],
        ids=["long_line_with_newline", "long_line_without_newline", "json_one_liner"],
    )
    def test_one_line_longer_than_the_budget_still_reaches_the_model(self, backend, fake, chunk):
        """
        Dropping the leading partial line must not empty the window.

        A single line longer than the cap has its newline at the very end, so dropping
        through it leaves nothing, and the toolset only labels a non-empty stream: the
        model would be told "(no output)" for a command that produced megabytes. Hits
        ``cat one_line.json``, ``base64 -w0``, minified JS, single-line CSV.
        """
        handle, sandbox = _created(backend, fake)
        sandbox.process = FakeProcess(stdout=[chunk])

        result = backend.run_command(handle, "cat big.json", timeout=30, max_output_bytes=51200)

        assert result.stdout != ""
        assert result.stdout_truncated is True
        assert len(result.stdout.encode()) <= 51200
        assert result.stdout.strip().endswith(chunk.strip()[-20:].decode())

    def test_a_long_line_followed_by_a_short_one_keeps_the_window(self, backend, fake):
        """
        Dropping the partial head must not cost most of the budget either.

        A 200 KB line then a short one puts the window's only newline near its end, so
        dropping through it would hand the model 18 bytes of a 50 KiB budget. The
        formatter already marks output as cut, so a partial first line misleads nobody.
        """
        handle, sandbox = _created(backend, fake)
        sandbox.process = FakeProcess(stdout=[b"L" * 200_000 + b"\nshort tail\n"])

        result = backend.run_command(handle, "spew", timeout=30, max_output_bytes=51200)

        assert len(result.stdout.encode()) > 51200 // 2
        assert result.stdout.strip().endswith("short tail")
        assert "L" in result.stdout, "the long line's tail survives rather than being dropped"

    def test_drops_a_leading_partial_line(self, backend, fake):
        handle, sandbox = _created(backend, fake)
        sandbox.process = FakeProcess(stdout=[b"aaaaaaaaaa\nbbbb\ncccc\n"])

        result = backend.run_command(handle, "spew", timeout=30, max_output_bytes=12)

        assert not result.stdout.startswith("a"), "no fragment presented as a whole record"

    def test_streams_are_capped_independently(self, backend, fake):
        """A large stderr must not crowd stdout out of the result."""
        handle, sandbox = _created(backend, fake)
        sandbox.process = FakeProcess(stdout=[b"keep-me\n"], stderr=[b"e" * 4000 + b"\n"])

        result = backend.run_command(handle, "spew", timeout=30, max_output_bytes=32)

        assert result.stdout == "keep-me\n"
        assert result.stdout_truncated is False
        assert result.stderr_truncated is True

    def test_invalid_utf8_is_replaced_not_raised(self, backend, fake):
        handle, sandbox = _created(backend, fake)
        sandbox.process = FakeProcess(stdout=[b"\xff\xfe binary"])

        result = backend.run_command(handle, "cat /bin/sh", timeout=30, max_output_bytes=1024)

        assert "binary" in result.stdout

    def test_stream_dying_mid_read_marks_truncation_and_still_reports(self, backend, fake):
        """The exit status decides what happened; a severed stream only costs output."""
        handle, sandbox = _created(backend, fake)
        sandbox.process = FakeProcess(
            returncode=0, stdout=[b"partial\n"], stream_error=fake.exception.ConnectionError("gone")
        )

        result = backend.run_command(handle, "spew", timeout=30, max_output_bytes=1024)

        assert result.stdout == "partial\n"
        assert result.stdout_truncated is True

    def test_refuses_a_command_that_cannot_fit_the_sandbox_lifetime(self, backend, fake):
        handle, _ = _created(backend, fake)

        with pytest.raises(SandboxError, match="lifetime") as caught:
            backend.run_command(handle, "sleep 9999", timeout=9999, max_output_bytes=1024)
        assert not isinstance(caught.value, SandboxTerminalError), "the model can ask for less"

    def test_shortens_a_command_to_the_life_the_sandbox_has_left(self, backend_class, fake, monkeypatch):
        """
        A command that fits the configured lifetime can still outlive an old sandbox.

        Shortened rather than refused, because refusing is unactionable for the inherited
        file operations: they run on a fixed internal budget that neither the model nor the
        Dag author can lower, so once a sandbox has less of that budget left than the budget
        itself -- which happens to every sandbox eventually -- ``read_file`` would fail for
        the rest of its life with advice nobody could follow.
        """
        backend = backend_class(sandbox_timeout=600)
        clock = [1000.0]
        monkeypatch.setattr("airflow.providers.common.ai.sandbox.modal.time.monotonic", lambda: clock[0])
        handle, sandbox = _created(backend, fake)
        clock[0] += 500  # 100s of life left

        backend.run_command(handle, "sleep 200", timeout=200, max_output_bytes=1024)

        deadline = next(c for c in sandbox.calls if c[0] == "exec")[2]["timeout"]
        assert deadline == 100, "shortened to what is left rather than refused"

    def test_reports_the_deadline_it_actually_applied(self, backend_class, fake, monkeypatch):
        """
        A shortened command that times out must not be reported with the number it asked for.

        Otherwise the model is told it had 200s, concludes the work needs longer, and comes
        back asking for more -- when the real constraint was never its request.
        """
        backend = backend_class(sandbox_timeout=600)
        clock = [1000.0]
        monkeypatch.setattr("airflow.providers.common.ai.sandbox.modal.time.monotonic", lambda: clock[0])
        handle, sandbox = _created(backend, fake)
        clock[0] += 500  # 100s of life left
        sandbox.process = FakeProcess(returncode=-1)

        result = backend.run_command(handle, "sleep 200", timeout=200, max_output_bytes=1024)

        assert result.timed_out is True
        assert result.applied_timeout == 100

    def test_the_inherited_file_budget_survives_a_short_lived_sandbox(self, backend_class, fake, monkeypatch):
        """
        The case that made refusing wrong: ``read_file`` runs a fixed 120s helper.

        Measured live, a sandbox created at the 120s floor already reports 119s left by the
        time the guard sees it, because provisioning takes a second or two, so a refusal
        made the tool impossible for that sandbox's entire life.
        """
        backend = backend_class(sandbox_timeout=120)
        clock = [1000.0]
        monkeypatch.setattr("airflow.providers.common.ai.sandbox.modal.time.monotonic", lambda: clock[0])
        handle, sandbox = _created(backend, fake)
        clock[0] += 2  # provisioning ate two seconds
        sandbox.process = FakeProcess(stdout=[b"7\ncGF5bG9hZA==\n"])

        assert backend.read_file(handle, "/workspace/out.txt", max_bytes=1024) == b"payload"

    def test_says_nothing_about_the_lifetime_of_a_sandbox_it_did_not_create(self, backend, fake):
        """A handle from elsewhere has a lifetime this backend cannot know, so it invents none."""
        handle, sandbox = _created(backend, fake)
        other = type(backend)(sandbox_timeout=600)

        assert other.run_command(handle, "true", timeout=590, max_output_bytes=1024).exit_code == 0

    @pytest.mark.parametrize(
        ("returncode", "probe_error", "expect_terminated"),
        [
            (137, None, False),
            (137, "NotFoundError", True),
            (128, "NotFoundError", True),
            (-1, None, False),
            (-1, "ConflictError", True),
            (3, "NotFoundError", False),
        ],
        ids=[
            "guest_sigkill_in_a_live_sandbox",
            "sandbox_terminated_under_the_command",
            "sandbox_destroyed_under_the_command",
            "plain_deadline",
            "sandbox_expired_at_its_own_timeout",
            "ordinary_failure_asks_nothing",
        ],
    )
    def test_detects_a_sandbox_that_died_under_the_command(
        self, backend, fake, returncode, probe_error, expect_terminated
    ):
        """
        A dead sandbox reports like an ordinary failure, so a signal exit or a deadline is
        followed by the cheapest possible command to find out which it was.

        Live against modal 1.5.5: a sandbox terminated mid-command returns 128 with its
        plumbing text on stderr, one that hits its own ``sandbox_timeout`` returns -1, and
        in both cases ``poll()`` still says None while an ``exec`` raises at once. An
        ordinary non-zero exit asks nothing, so the common path costs no round trip.
        """
        handle, sandbox = _created(backend, fake)
        sandbox.processes = [FakeProcess(returncode=returncode)]
        # The command runs; the liveness probe after it is what fails.
        sandbox.exec_errors = [None, getattr(fake.exception, probe_error)("gone") if probe_error else None]

        result = backend.run_command(handle, "work", timeout=30, max_output_bytes=1024)

        assert result.sandbox_terminated is expect_terminated
        assert result.exit_code == returncode
        if expect_terminated:
            assert handle not in backend._sandboxes, "a dead handle is not kept"

    @pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan")])
    def test_rejects_an_impossible_timeout(self, backend, fake, timeout):
        handle, _ = _created(backend, fake)

        with pytest.raises(ValueError, match="timeout"):
            backend.run_command(handle, "true", timeout=timeout, max_output_bytes=1024)

    @pytest.mark.parametrize(
        ("error_name", "terminal"),
        [
            ("AuthError", True),
            ("NotFoundError", True),
            ("SandboxTerminatedError", True),
            ("SandboxTimeoutError", True),
            ("InvalidError", True),
            ("ConflictError", True),
            ("SandboxFilesystemNotFoundError", False),
            ("SandboxFilesystemPermissionError", False),
            ("ConnectionError", False),
            ("Error", False),
        ],
    )
    def test_classifies_failures(self, backend, fake, error_name, terminal):
        """
        Terminal fails the task; recoverable reaches the model as a retry.

        ``ConflictError`` derives from ``InvalidError`` and ``SandboxTimeoutError`` shares a
        parent with ``ExecTimeoutError``, so this is where the ordering of those checks is
        pinned down.
        """
        handle, sandbox = _created(backend, fake)
        sandbox.exec_error = getattr(fake.exception, error_name)("failed")

        with pytest.raises(SandboxError) as caught:
            backend.run_command(handle, "true", timeout=30, max_output_bytes=1024)
        assert isinstance(caught.value, SandboxTerminalError) is terminal

    def test_a_shutting_down_sandbox_says_so(self, backend, fake):
        """
        ``ConflictError`` is a subclass of ``InvalidError`` and means something else.

        Observed live as "Modal Sandbox is shutting down" on a sandbox reaching the end of
        its life, which is not the request being wrong.
        """
        handle, sandbox = _created(backend, fake)
        sandbox.exec_error = fake.exception.ConflictError("Modal Sandbox is shutting down.")

        with pytest.raises(SandboxTerminalError, match="shutting down"):
            backend.run_command(handle, "true", timeout=30, max_output_bytes=1024)

    def test_ambiguous_failure_on_a_dead_sandbox_is_terminal(self, backend, fake):
        handle, sandbox = _created(backend, fake)
        sandbox.exec_error = fake.exception.ConnectionError("unreachable")
        sandbox.poll_result = 137

        with pytest.raises(SandboxTerminalError, match="no longer running"):
            backend.run_command(handle, "true", timeout=30, max_output_bytes=1024)

    def test_looks_up_a_handle_it_did_not_create(self, backend, backend_class, fake):
        """A handle can outlive the instance that made it, so the cache cannot be the only path."""
        handle, sandbox = _created(backend, fake)
        sandbox.process = FakeProcess(stdout=[b"ok\n"])
        other = backend_class()

        assert other.run_command(handle, "true", timeout=30, max_output_bytes=1024).stdout == "ok\n"

    def test_unknown_handle_is_terminal(self, backend, fake):
        with pytest.raises(SandboxTerminalError, match="gone"):
            backend.run_command("sb-does-not-exist", "true", timeout=30, max_output_bytes=1024)


class TestFileOperations:
    def test_write_file_uses_the_native_api(self, backend, fake):
        handle, sandbox = _created(backend, fake)

        backend.write_file(handle, "/workspace/out.txt", b"payload")

        assert ("write_bytes", "/workspace/out.txt", b"payload") in sandbox.calls
        assert not any(call[0] == "exec" for call in sandbox.calls), "no shell round trip"

    @pytest.mark.parametrize(
        ("given", "expected"),
        [
            ("out.txt", "/workspace/out.txt"),
            ("./out.txt", "/workspace/out.txt"),
            ("nested/out.txt", "/workspace/nested/out.txt"),
            ("/abs/out.txt", "/abs/out.txt"),
            ("/abs/../out.txt", "/abs/../out.txt"),
            ("//workspace//nested///out.txt", "/workspace/nested/out.txt"),
        ],
    )
    def test_resolves_paths_against_the_working_directory(self, backend_class, fake, given, expected):
        """Modal's filesystem API takes absolute paths only, and must agree with the shell's cwd."""
        backend = backend_class(workdir="/workspace")
        handle, sandbox = _created(backend, fake)

        backend.write_file(handle, given, b"x")

        assert ("write_bytes", expected, b"x") in sandbox.calls

    def test_asks_the_sandbox_for_its_workdir_once_when_unset(self, backend_class, fake):
        backend = backend_class(workdir=None)
        handle, sandbox = _created(backend, fake)
        sandbox.processes = [FakeProcess(stdout=[b"/srv\n"])]

        backend.write_file(handle, "a.txt", b"x")
        backend.write_file(handle, "b.txt", b"y")

        assert ("write_bytes", "/srv/a.txt", b"x") in sandbox.calls
        assert ("write_bytes", "/srv/b.txt", b"y") in sandbox.calls
        assert len([call for call in sandbox.calls if call[0] == "exec"]) == 1, "cached after the first ask"

    def test_unusable_workdir_answer_is_recoverable(self, backend_class, fake):
        backend = backend_class(workdir=None)
        handle, sandbox = _created(backend, fake)
        sandbox.processes = [FakeProcess(returncode=1, stderr=[b"pwd: not found\n"])]

        with pytest.raises(SandboxError, match="working directory"):
            backend.write_file(handle, "a.txt", b"x")

    def test_list_directory_marks_directories(self, backend, fake):
        handle, sandbox = _created(backend, fake)
        sandbox.listing = [FileInfo("sub", kind="directory"), FileInfo("file.txt")]

        assert backend.list_directory(handle, "/workspace") == [("sub", True), ("file.txt", False)]

    def test_a_symlink_is_not_reported_as_a_directory(self, backend, fake):
        """
        Modal types a symlink as a symlink whatever it points at, and so does ``find -printf``.

        Reporting one as a directory would be a guess about its target that neither backend
        makes, so a symlinked directory lists without a trailing slash on both.
        """
        handle, sandbox = _created(backend, fake)
        sandbox.listing = [FileInfo("link-to-dir", kind="symlink")]

        assert backend.list_directory(handle, "/workspace") == [("link-to-dir", False)]

    def test_list_directory_uses_the_native_api(self, backend, fake):
        handle, sandbox = _created(backend, fake)

        backend.list_directory(handle, "/workspace")

        assert ("list_files", "/workspace") in sandbox.calls
        assert not any(call[0] == "exec" for call in sandbox.calls)

    def test_read_file_stays_on_the_shell_path(self, backend, fake):
        """
        Not overridden on purpose: Modal's ``read_bytes`` takes no length, so it cannot honour
        ``max_bytes``. The inherited implementation caps inside the guest with ``head -c``.
        """
        handle, sandbox = _created(backend, fake)
        sandbox.process = FakeProcess(stdout=[b"7\ncGF5bG9hZA==\n"])

        assert backend.read_file(handle, "/workspace/out.txt", max_bytes=1024) == b"payload"
        assert any(call[0] == "exec" for call in sandbox.calls)
        assert not any(call[0] == "read_bytes" for call in sandbox.calls)

    def test_missing_file_is_recoverable(self, backend, fake):
        handle, sandbox = _created(backend, fake)
        sandbox.filesystem_error = fake.exception.SandboxFilesystemNotFoundError("no such path")

        with pytest.raises(SandboxError) as caught:
            backend.list_directory(handle, "/workspace/nope")
        assert not isinstance(caught.value, SandboxTerminalError)

    def test_dead_sandbox_during_a_file_operation_is_terminal(self, backend, fake):
        handle, sandbox = _created(backend, fake)
        sandbox.filesystem_error = fake.exception.SandboxTerminatedError("gone")

        with pytest.raises(SandboxTerminalError):
            backend.write_file(handle, "/workspace/out.txt", b"x")


class TestDestroy:
    def test_terminates_without_waiting(self, backend, fake):
        """``terminate(wait=True)`` took 31s when measured, and this runs in every teardown."""
        handle, sandbox = _created(backend, fake)

        backend.destroy(handle)

        assert ("terminate", False) in sandbox.calls
        assert sandbox.terminated is True

    def test_is_idempotent(self, backend, fake):
        handle, sandbox = _created(backend, fake)

        backend.destroy(handle)
        backend.destroy(handle)

        assert handle not in backend._sandboxes

    def test_forgets_a_missing_sandbox_quietly(self, backend):
        backend.destroy("sb-never-existed")

    def test_survives_a_failed_termination(self, backend, fake, caplog):
        """The model's work is already paid for, so a teardown blip must not fail the task."""
        handle, sandbox = _created(backend, fake)
        sandbox.terminate_error = fake.exception.ConnectionError("blip")

        backend.destroy(handle)

        assert "Could not terminate" in caplog.text

    def test_can_be_called_while_a_command_is_outstanding(self, backend, fake):
        """
        A cancelled tool call leaves a thread inside a Modal call, and teardown runs anyway.

        Threads are not preemptible, so the two overlap. Nothing may deadlock or raise.
        """
        handle, sandbox = _created(backend, fake)
        released = threading.Event()
        entered = threading.Event()

        class BlockingStream:
            def __iter__(self):
                entered.set()
                released.wait(timeout=5)
                yield b"late\n"

        process = FakeProcess(returncode=0)
        process.stdout = BlockingStream()
        sandbox.process = process
        result: list = []
        worker = threading.Thread(
            target=lambda: result.append(
                backend.run_command(handle, "slow", timeout=30, max_output_bytes=1024)
            )
        )
        worker.start()
        assert entered.wait(timeout=5)

        backend.destroy(handle)
        released.set()
        worker.join(timeout=10)

        assert not worker.is_alive()
        assert sandbox.terminated is True
        assert len(result) == 1
        assert result[0].exit_code == 0
