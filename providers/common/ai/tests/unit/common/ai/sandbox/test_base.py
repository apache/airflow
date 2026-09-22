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

import inspect
import subprocess
from types import SimpleNamespace

import pytest
import time_machine

from airflow.providers.common.ai.sandbox.base import (
    EXPIRES_AT_TAG,
    HOLDER_TAG,
    NETWORK_TAG,
    OWNER_TAG,
    SandboxBackend,
    SandboxError,
    SandboxExecResult,
    SandboxFileTooLargeError,
    SandboxSpec,
    SandboxTerminalError,
    _new_sandbox_name,
    _validate_positive_finite,
    dag_run_owner,
    decode_network_policy,
    encode_network_policy,
)

from unit.common.ai.sandbox.fake_tags import TaggedBackend


class TestValidation:
    @pytest.mark.parametrize("bad", [0, -1, float("inf"), float("-inf"), float("nan")])
    def test_rejects_non_positive_or_non_finite(self, bad):
        with pytest.raises(ValueError, match="thing must be a positive finite number"):
            _validate_positive_finite(bad, "thing")

    @pytest.mark.parametrize("good", [1, 0.5, 3600])
    def test_accepts_positive_finite(self, good):
        _validate_positive_finite(good, "thing")


class TestSandboxName:
    def test_is_prefixed_for_operator_cleanup(self):
        assert _new_sandbox_name().startswith("airflow-sandbox-")

    def test_is_unique_per_call(self):
        assert len({_new_sandbox_name() for _ in range(100)}) == 100


class TestSandboxSpec:
    def test_defaults_deny_egress_and_inject_nothing(self):
        # The safe starting point: a sandbox that cannot phone home and carries
        # none of the worker's environment.
        spec = SandboxSpec()

        assert spec.block_network is True
        assert spec.env is None
        assert spec.allow_egress_to is None
        assert spec.owner is None

    def test_is_frozen_so_a_backend_cannot_mutate_the_authors_intent(self):
        spec = SandboxSpec()

        with pytest.raises(AttributeError):
            spec.block_network = False  # type: ignore[misc]


class TestSandboxExecResult:
    def test_defaults_are_the_success_case(self):
        result = SandboxExecResult(exit_code=0, stdout="", stderr="")

        assert not result.timed_out
        assert not result.stdout_truncated
        assert not result.stderr_truncated
        assert not result.sandbox_terminated


class TestErrorHierarchy:
    def test_terminal_is_a_sandbox_error(self):
        # The toolset catches SandboxTerminalError first, so ordering matters.
        assert issubclass(SandboxTerminalError, SandboxError)

    def test_file_too_large_is_recoverable(self):
        assert issubclass(SandboxFileTooLargeError, SandboxError)
        assert not issubclass(SandboxFileTooLargeError, SandboxTerminalError)

    def test_file_too_large_carries_the_numbers_for_the_message(self):
        e = SandboxFileTooLargeError("/w/big", 100, 10)

        assert (e.path, e.size_bytes, e.max_bytes) == ("/w/big", 100, 10)
        assert "/w/big" in str(e)


class TestBackendContract:
    def test_cannot_be_instantiated_without_implementing_everything(self):
        with pytest.raises(TypeError):
            SandboxBackend()  # type: ignore[abstract]

    def test_only_lifecycle_and_command_are_required(self):
        # A new backend should have to write three methods, not six: the file
        # operations are expressible as shell commands and ship as defaults.
        required = {
            n
            for n in dir(SandboxBackend)
            if getattr(getattr(SandboxBackend, n, None), "__isabstractmethod__", False)
        }

        assert required == {"create", "run_command", "destroy"}

    def test_file_operations_are_overridable_defaults(self):
        for name in ("read_file", "write_file", "list_directory"):
            method = getattr(SandboxBackend, name)
            assert not getattr(method, "__isabstractmethod__", False)

    def test_create_takes_the_spec_as_a_keyword_only_argument(self):
        sig = inspect.signature(SandboxBackend.create)

        assert sig.parameters["spec"].kind is inspect.Parameter.KEYWORD_ONLY
        assert sig.parameters["spec"].default is None


class _LocalShellBackend(SandboxBackend):
    """
    Minimal backend that runs each script through a real shell.

    Implements only the three required methods, which is the point: everything
    below exercises the inherited file operations, and it runs the actual shell
    recipes rather than asserting against a mock of them.
    """

    name = "local"

    def __init__(self, root: str) -> None:
        self.root = root

    def create(self, *, spec=None) -> str:
        return "local"

    def run_command(self, sandbox, command, *, timeout, max_output_bytes):
        proc = subprocess.run(
            ["sh", "-c", command], capture_output=True, cwd=self.root, timeout=timeout, check=False
        )
        out = proc.stdout[:max_output_bytes]
        return SandboxExecResult(
            exit_code=proc.returncode,
            stdout=out.decode(errors="replace"),
            stderr=proc.stderr.decode(errors="replace")[:max_output_bytes],
            stdout_truncated=len(proc.stdout) > max_output_bytes,
        )

    def destroy(self, sandbox) -> None:
        pass


@pytest.fixture
def local(tmp_path):
    return _LocalShellBackend(str(tmp_path))


class TestDefaultFileOperations:
    """The inherited implementations, against a real shell."""

    def test_write_then_read_round_trips(self, local, tmp_path):
        local.write_file("s", str(tmp_path / "a.txt"), b"hello world")

        assert local.read_file("s", str(tmp_path / "a.txt"), max_bytes=100) == b"hello world"

    def test_write_creates_parent_directories(self, local, tmp_path):
        local.write_file("s", str(tmp_path / "deep" / "nested" / "a.txt"), b"x")

        assert (tmp_path / "deep" / "nested" / "a.txt").read_bytes() == b"x"

    def test_a_missing_file_is_an_error_not_an_empty_read(self, local, tmp_path):
        # Regression: the size probe failing used to fall through to an empty
        # base64 decode, so a missing path read back as a zero-byte file.
        with pytest.raises(SandboxError, match="does not exist"):
            local.read_file("s", str(tmp_path / "nope.txt"), max_bytes=100)

    def test_a_directory_is_an_error_not_an_empty_read(self, local, tmp_path):
        # Regression: ``stat`` succeeds on a directory and the pipeline's exit
        # status is base64's, so a directory used to read back as an empty file
        # and the model was told the directory was a file with no lines.
        (tmp_path / "sub").mkdir()
        (tmp_path / "sub" / "inner.txt").write_text("x")

        with pytest.raises(SandboxError, match="is a directory"):
            local.read_file("s", str(tmp_path / "sub"), max_bytes=100)

    def test_a_symlink_to_a_directory_is_an_error_too(self, local, tmp_path):
        (tmp_path / "sub").mkdir()
        (tmp_path / "link").symlink_to(tmp_path / "sub")

        with pytest.raises(SandboxError, match="is a directory"):
            local.read_file("s", str(tmp_path / "link"), max_bytes=100)

    def test_oversized_file_is_refused(self, local, tmp_path):
        (tmp_path / "big.bin").write_bytes(b"x" * 500)

        with pytest.raises(SandboxFileTooLargeError):
            local.read_file("s", str(tmp_path / "big.bin"), max_bytes=100)

    def test_a_zero_length_stream_is_still_bounded(self, local):
        # /dev/zero reports size 0, so only the guest-side head -c stops it.
        with pytest.raises(SandboxFileTooLargeError):
            local.read_file("s", "/dev/zero", max_bytes=1024)

    @pytest.mark.parametrize(
        "name",
        ["plain.txt", "with space.txt", "with'quote.txt", 'with"double.txt', "semi;colon.txt", "$dollar.txt"],
    )
    def test_hostile_filenames_round_trip(self, local, tmp_path, name):
        # The recipes interpolate the path into a shell command, so quoting is
        # the thing most likely to break.
        local.write_file("s", str(tmp_path / name), b"payload")

        assert local.read_file("s", str(tmp_path / name), max_bytes=100) == b"payload"

    def test_binary_content_survives(self, local, tmp_path):
        blob = bytes(range(256))
        local.write_file("s", str(tmp_path / "b.bin"), blob)

        assert local.read_file("s", str(tmp_path / "b.bin"), max_bytes=1000) == blob

    def test_list_directory_marks_directories(self, local, tmp_path):
        (tmp_path / "a.txt").write_text("x")
        (tmp_path / "sub").mkdir()

        assert sorted(local.list_directory("s", str(tmp_path))) == [("a.txt", False), ("sub", True)]

    def test_list_directory_survives_a_newline_in_a_name(self, local, tmp_path):
        (tmp_path / "new\nline.txt").write_text("x")

        assert local.list_directory("s", str(tmp_path)) == [("new\nline.txt", False)]

    def test_listing_a_missing_directory_is_an_error(self, local, tmp_path):
        with pytest.raises(SandboxError):
            local.list_directory("s", str(tmp_path / "nope"))


class TestDagRunOwner:
    def test_names_the_dag_and_the_run(self):
        context = {"ti": SimpleNamespace(dag_id="my_dag", run_id="manual__2026-01-01T00:00:00+00:00")}

        assert dag_run_owner(context) == "my_dag/manual__2026-01-01T00:00:00+00:00"


class _RacingBackend(TaggedBackend):
    """A store in which someone else's claim lands between our check and our write."""

    def write_tags(self, sandbox, tags):
        super().write_tags(sandbox, {**tags, HOLDER_TAG: "someone/faster"})


class TestNetworkPolicyTag:
    @pytest.mark.parametrize(
        "spec",
        [
            SandboxSpec(),
            SandboxSpec(block_network=False),
            SandboxSpec(allow_egress_to=["pypi.org"], allow_egress_to_cidrs=["203.0.113.0/24"]),
        ],
    )
    def test_round_trips_the_network_fields_and_nothing_else(self, spec):
        decoded = decode_network_policy(encode_network_policy(spec))

        assert decoded == SandboxSpec(
            block_network=spec.block_network,
            allow_egress_to=list(spec.allow_egress_to) if spec.allow_egress_to else None,
            allow_egress_to_cidrs=list(spec.allow_egress_to_cidrs) if spec.allow_egress_to_cidrs else None,
        )

    def test_the_encoding_is_stable(self):
        # Sorted keys and no whitespace: the same policy stamps the same string.
        assert encode_network_policy(SandboxSpec()) == (
            '{"allow_egress_to":[],"allow_egress_to_cidrs":[],"block_network":true}'
        )

    @pytest.mark.parametrize("value", [None, "", "not json", '{"block_network": true}', "[1, 2]"])
    def test_anything_else_decodes_to_nothing(self, value):
        # Better to say nothing about the network than to describe one nobody asked for.
        assert decode_network_policy(value) is None


class TestAttachablePolicy:
    """
    The ownership rules, written once on the base class over two tag primitives.

    What they stop: a run reaching the wrong sandbox by mistake, including through a bad
    XCom, and two runs sharing one workspace. What they do not stop, and do not claim
    to: anyone holding the vendor credential, who can rewrite the tags.
    """

    def test_a_bare_handle_is_never_enough(self):
        backend = TaggedBackend({"sb": {}})

        with pytest.raises(SandboxTerminalError, match="carries no owner"):
            backend.attach("sb", owner="dag/run", holder="dag/task")

    def test_the_wrong_owner_is_refused_and_named(self):
        backend = TaggedBackend({"sb": {OWNER_TAG: "other_dag/run"}})

        with pytest.raises(SandboxTerminalError, match="not owned by 'dag/run'.*'other_dag/run'"):
            backend.attach("sb", owner="dag/run", holder="dag/task")
        assert HOLDER_TAG not in backend.tags["sb"], "a refused attach must leave no claim"

    def test_a_sandbox_held_by_another_task_is_refused(self):
        backend = TaggedBackend({"sb": {OWNER_TAG: "o", HOLDER_TAG: "dag/other_task"}})

        with pytest.raises(SandboxTerminalError, match="already held by 'dag/other_task'"):
            backend.attach("sb", owner="o", holder="dag/task")

    def test_the_same_holder_may_attach_again(self):
        # A retry of the agent task is the same holder, and the previous attempt may
        # have died without releasing. It must find its files, not a locked door.
        backend = TaggedBackend({"sb": {OWNER_TAG: "o", HOLDER_TAG: "dag/task"}})

        backend.attach("sb", owner="o", holder="dag/task")

        assert backend.tags["sb"][HOLDER_TAG] == "dag/task"

    def test_attaching_marks_the_holder_and_keeps_every_other_tag(self):
        backend = TaggedBackend({"sb": {OWNER_TAG: "o", "team": "data"}})

        backend.attach("sb", owner="o", holder="dag/task")

        assert backend.tags["sb"] == {OWNER_TAG: "o", "team": "data", HOLDER_TAG: "dag/task"}

    def test_a_claim_that_lost_the_race_is_refused_not_kept(self):
        # No conditional write exists, so the write is read back: whoever lost sees the
        # other holder and backs off instead of both runs proceeding in silence.
        backend = _RacingBackend({"sb": {OWNER_TAG: "o"}})

        with pytest.raises(SandboxTerminalError, match="claimed by 'someone/faster' while 'dag/task'"):
            backend.attach("sb", owner="o", holder="dag/task")

    @time_machine.travel(1_000_000, tick=False)
    def test_the_remaining_lifetime_comes_from_the_stamped_expiry(self):
        backend = TaggedBackend({"sb": {OWNER_TAG: "o", EXPIRES_AT_TAG: str(1_000_000 + 600)}})

        assert backend.attach("sb", owner="o", holder="h").remaining_lifetime == 600.0

    @time_machine.travel(1_000_000, tick=False)
    def test_an_expired_stamp_reports_zero_not_a_negative_number(self):
        backend = TaggedBackend({"sb": {OWNER_TAG: "o", EXPIRES_AT_TAG: str(1_000_000 - 60)}})

        assert backend.attach("sb", owner="o", holder="h").remaining_lifetime == 0.0

    @pytest.mark.parametrize("tags", [{OWNER_TAG: "o"}, {OWNER_TAG: "o", EXPIRES_AT_TAG: "soon"}])
    def test_no_usable_expiry_means_no_claim_about_the_lifetime(self, tags):
        backend = TaggedBackend({"sb": tags})

        assert backend.attach("sb", owner="o", holder="h").remaining_lifetime is None

    def test_the_network_policy_comes_back_as_a_spec(self):
        stamped = encode_network_policy(SandboxSpec(allow_egress_to_cidrs=["203.0.113.7/32"]))
        backend = TaggedBackend({"sb": {OWNER_TAG: "o", NETWORK_TAG: stamped}})

        attached = backend.attach("sb", owner="o", holder="h")

        assert attached.network == SandboxSpec(block_network=True, allow_egress_to_cidrs=["203.0.113.7/32"])

    def test_no_network_stamp_means_no_claim_about_the_network(self):
        backend = TaggedBackend({"sb": {OWNER_TAG: "o"}})

        assert backend.attach("sb", owner="o", holder="h").network is None

    def test_release_clears_only_the_callers_claim(self):
        backend = TaggedBackend({"sb": {OWNER_TAG: "o", HOLDER_TAG: "someone_else"}})

        backend.release("sb", holder="h")

        assert backend.tags["sb"][HOLDER_TAG] == "someone_else"

    def test_release_is_idempotent(self):
        backend = TaggedBackend({"sb": {OWNER_TAG: "o", HOLDER_TAG: "h"}})

        backend.release("sb", holder="h")
        backend.release("sb", holder="h")

        assert backend.tags["sb"] == {OWNER_TAG: "o"}

    def test_a_missing_sandbox_is_terminal_for_both(self):
        backend = TaggedBackend({})

        with pytest.raises(SandboxTerminalError):
            backend.attach("sb", owner="o", holder="h")
        with pytest.raises(SandboxTerminalError):
            backend.release("sb", holder="h")
