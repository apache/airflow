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

import json
from subprocess import CompletedProcess
from unittest import mock

import pytest
from click.testing import CliRunner

from airflow_breeze.commands.release_management_commands import (
    OS_DEPS_IMAGE_PLATFORMS,
    build_os_deps_images,
    compute_os_deps_image_inputs_hash,
    extract_dockerfile_stage,
    get_os_deps_image_build_args,
    get_os_deps_image_builds,
    publish_os_deps_images,
    release_prod_images,
)
from airflow_breeze.global_constants import (
    ALL_PYTHON_VERSION_TO_PATCHLEVEL_VERSION,
    HARDENED_OS_DEPS_IMAGE_DIGESTS,
    OS_DEPS_IMAGE_TYPES,
    get_os_deps_image_python_tag,
    get_os_deps_image_tag,
    get_pinned_os_deps_image,
)
from airflow_breeze.params.build_ci_params import BuildCiParams
from airflow_breeze.params.build_prod_params import BuildProdParams
from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH

REPOSITORY = "ghcr.io/apache/airflow/base/os-deps"
MIRROR = "ghcr.io/apache/airflow/base/python"
PYTHON_TAG = f"{ALL_PYTHON_VERSION_TO_PATCHLEVEL_VERSION['3.12']}-debian13-dev"
PYTHON_DIGEST = "sha256:" + "12" * 32
OS_DEPS_DIGEST = "sha256:" + "ab" * 32
PINS = {
    f"ci-{PYTHON_TAG}": "sha256:" + "c1" * 32,
    f"dev-{PYTHON_TAG}": "sha256:" + "d1" * 32,
    f"runtime-{PYTHON_TAG}": "sha256:" + "e1" * 32,
}
IMAGE_TYPES = {image_type.name: image_type for image_type in OS_DEPS_IMAGE_TYPES}
PATCH_PREFIX = "airflow_breeze.commands.release_management_commands"


def _build_args(params) -> dict[str, str]:
    values = params.prepare_arguments_for_docker_build_command()
    return dict(value.split("=", 1) for value in values[1::2] if "=" in value)


def _pinned(image_type: str) -> str:
    tag = f"{image_type}-{PYTHON_TAG}"
    return f"{REPOSITORY}:{tag}@{PINS[tag]}"


class FakeRegistry:
    """Answers run_command for buildx: images are looked up, built and copied in a dict of tags."""

    def __init__(self, digests: dict[str, str] | None = None):
        self.digests: dict[str, str] = dict(digests or {})
        self.builds: list[list[str]] = []
        self.copies: list[tuple[list[str], list[str]]] = []

    def __call__(self, cmd, **kwargs):
        if cmd[2] == "build":
            self.builds.append(cmd)
            self.digests[cmd[cmd.index("--tag") + 1]] = "sha256:" + "0f" * 32
            return CompletedProcess(args=cmd, returncode=0)
        if cmd[3] == "inspect":
            digest = self.digests.get(cmd[4])
            stdout = json.dumps({"digest": digest}) if digest else ""
            return CompletedProcess(args=cmd, returncode=0 if digest else 1, stdout=stdout)
        targets = [cmd[i + 1] for i, arg in enumerate(cmd) if arg == "--tag"]
        sources = [arg for i, arg in enumerate(cmd[4:], start=4) if arg != "--tag" and cmd[i - 1] != "--tag"]
        self.copies.append((sources, targets))
        for target in targets:
            self.digests[target] = OS_DEPS_DIGEST
        return CompletedProcess(args=cmd, returncode=0)


@pytest.fixture
def registry():
    fake = FakeRegistry({f"{MIRROR}:{PYTHON_TAG}": PYTHON_DIGEST})
    with mock.patch(f"{PATCH_PREFIX}.run_command", autospec=True, side_effect=fake):
        yield fake


def test_os_deps_image_tags():
    assert get_os_deps_image_tag("ci", "3.12") == f"ci-{PYTHON_TAG}"
    assert get_os_deps_image_python_tag("ci", "3.12", PYTHON_DIGEST) == f"ci-{PYTHON_TAG}-{'12' * 6}"


@mock.patch.dict(HARDENED_OS_DEPS_IMAGE_DIGESTS, PINS, clear=True)
def test_pinned_os_deps_image():
    assert get_pinned_os_deps_image("ci", "3.12") == _pinned("ci")
    assert get_pinned_os_deps_image("ci", "3.13") is None


@mock.patch.dict(HARDENED_OS_DEPS_IMAGE_DIGESTS, PINS, clear=True)
def test_ci_image_starts_from_the_pinned_os_deps_image():
    assert _build_args(BuildCiParams(python="3.12"))["CI_OS_DEPS_IMAGE"] == _pinned("ci")


@pytest.mark.parametrize(
    "params",
    [
        pytest.param({"python": "3.13"}, id="not-pinned"),
        pytest.param({"python": "3.12", "use_pinned_os_deps_images": False}, id="disabled"),
        pytest.param({"python": "3.12", "python_image": "my-registry/python:x"}, id="custom-base-image"),
        pytest.param({"python": "3.12", "debian_version": "bookworm"}, id="other-debian-version"),
        pytest.param({"python": "3.12", "additional_dev_apt_deps": "jq"}, id="additional-dev-apt-deps"),
        pytest.param({"python": "3.12", "dev_apt_command": "echo"}, id="dev-apt-command"),
    ],
)
@mock.patch.dict(HARDENED_OS_DEPS_IMAGE_DIGESTS, PINS, clear=True)
def test_ci_image_installs_os_dependencies_itself(params):
    assert "CI_OS_DEPS_IMAGE" not in _build_args(BuildCiParams(**params))


@pytest.mark.parametrize(
    ("params", "expected"),
    [
        pytest.param(
            {},
            {"DEV_OS_DEPS_IMAGE": _pinned("dev"), "RUNTIME_OS_DEPS_IMAGE": _pinned("runtime")},
            id="both",
        ),
        pytest.param({"image_flavor": "legacy"}, {}, id="legacy-flavor"),
        pytest.param({"python_image": "my-registry/python:x"}, {}, id="custom-base-image"),
        pytest.param({"debian_version": "bookworm"}, {}, id="other-debian-version"),
        pytest.param(
            {"additional_dev_apt_deps": "jq"},
            {"RUNTIME_OS_DEPS_IMAGE": _pinned("runtime")},
            id="custom-dev-apt-deps",
        ),
        pytest.param(
            {"additional_runtime_apt_deps": "jq"},
            {"DEV_OS_DEPS_IMAGE": _pinned("dev")},
            id="custom-runtime-apt-deps",
        ),
        pytest.param(
            {"runtime_apt_command": "echo"},
            {"DEV_OS_DEPS_IMAGE": _pinned("dev")},
            id="custom-runtime-apt-command",
        ),
    ],
)
@mock.patch.dict(HARDENED_OS_DEPS_IMAGE_DIGESTS, PINS, clear=True)
def test_prod_image_starts_from_the_pinned_os_deps_images(params, expected):
    build_args = _build_args(BuildProdParams(python="3.12", **params))

    assert {name: build_args[name] for name in build_args if name.endswith("_OS_DEPS_IMAGE")} == expected


@pytest.mark.parametrize("image_type", OS_DEPS_IMAGE_TYPES, ids=lambda image_type: image_type.name)
def test_os_deps_image_stage_ends_with_its_installation(image_type):
    dockerfile = (AIRFLOW_ROOT_PATH / image_type.dockerfile).read_text()

    stage = extract_dockerfile_stage(dockerfile, image_type.target)

    assert stage.startswith(f"FROM ${{{image_type.build_arg}}} as {image_type.target}\n")
    assert stage.rstrip().endswith(f"RUN bash /scripts/docker/install_os_dependencies.sh {image_type.name}")
    assert f'ARG {image_type.build_arg}="${{BASE_IMAGE}}"' in dockerfile


def test_extract_dockerfile_stage_fails_for_a_missing_stage():
    with pytest.raises(ValueError, match="no-such-stage"):
        extract_dockerfile_stage("FROM scratch as main\n", "no-such-stage")


@pytest.mark.parametrize(
    ("image_type", "expected"),
    [
        pytest.param("ci", {}, id="ci"),
        pytest.param("dev", {"AIRFLOW_IMAGE_FLAVOR": "hardened"}, id="dev"),
        pytest.param("runtime", {"AIRFLOW_IMAGE_FLAVOR": "hardened"}, id="runtime"),
    ],
)
def test_os_deps_image_build_args_match_the_image_builds(image_type, expected):
    build_args = get_os_deps_image_build_args(IMAGE_TYPES[image_type], "3.12", PYTHON_DIGEST)

    assert build_args == {
        "BASE_IMAGE": f"{MIRROR}:{PYTHON_TAG}@{PYTHON_DIGEST}",
        "AIRFLOW_PYTHON_VERSION": ALL_PYTHON_VERSION_TO_PATCHLEVEL_VERSION["3.12"],
        **expected,
    }


@pytest.mark.parametrize(
    "change",
    [
        pytest.param("script", id="script"),
        pytest.param("stage", id="stage"),
        pytest.param("build-arg", id="build-arg"),
    ],
)
def test_os_deps_image_inputs_hash_changes_with_its_inputs(change, tmp_path):
    (tmp_path / "Dockerfile.ci").write_text(
        "FROM ${CI_OS_DEPS_IMAGE} as ci-os-deps\nRUN true\nFROM x as main\n"
    )
    script = tmp_path / "install_os_dependencies.sh"
    script.write_text("echo install\n")
    build_args = {"BASE_IMAGE": "base@sha256:1", "AIRFLOW_PYTHON_VERSION": "3.12.1"}
    with (
        mock.patch(f"{PATCH_PREFIX}.AIRFLOW_ROOT_PATH", tmp_path),
        mock.patch(f"{PATCH_PREFIX}.INSTALL_OS_DEPENDENCIES_SCRIPT_PATH", script),
    ):
        before = compute_os_deps_image_inputs_hash(IMAGE_TYPES["ci"], build_args)
        assert before == compute_os_deps_image_inputs_hash(
            IMAGE_TYPES["ci"], {**build_args, "BASE_IMAGE": "other-base@sha256:2"}
        )
        if change == "script":
            script.write_text("echo install more\n")
        elif change == "stage":
            (tmp_path / "Dockerfile.ci").write_text(
                "FROM ${CI_OS_DEPS_IMAGE} as ci-os-deps\nRUN false\nFROM x as main\n"
            )
        else:
            build_args["AIRFLOW_PYTHON_VERSION"] = "3.12.2"
        assert compute_os_deps_image_inputs_hash(IMAGE_TYPES["ci"], build_args) != before


def test_os_deps_image_builds_are_tagged_with_their_inputs(registry):
    builds = get_os_deps_image_builds("3.12", ("ci",))

    assert len(builds) == 1
    assert builds[0].latest_tag == f"ci-{PYTHON_TAG}"
    digest_prefix = f"ci-{PYTHON_TAG}-{'12' * 6}-"
    assert builds[0].content_tag.startswith(digest_prefix)
    assert len(builds[0].content_tag) == len(digest_prefix) + 12


def test_os_deps_image_builds_fail_without_a_mirrored_base_image():
    with mock.patch(f"{PATCH_PREFIX}.run_command", autospec=True, side_effect=FakeRegistry()):
        with pytest.raises(SystemExit):
            get_os_deps_image_builds("3.12", ())


def test_build_os_deps_images_builds_and_pushes_the_platform_image(registry):
    result = CliRunner().invoke(
        build_os_deps_images,
        ["--python", "3.12", "--image-type", "runtime", "--platform", "linux/arm64"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    (build,) = get_os_deps_image_builds("3.12", ("runtime",))
    (command,) = registry.builds
    assert command[command.index("--target") + 1] == "runtime-os-deps"
    assert command[command.index("--file") + 1] == "Dockerfile"
    assert command[command.index("--platform") + 1] == "linux/arm64"
    assert command[command.index("--tag") + 1] == f"{REPOSITORY}:{build.content_tag}-arm64"
    assert "--push" in command
    assert "--no-cache" in command


@pytest.mark.parametrize("existing", ["merged", "platform"])
def test_build_os_deps_images_skips_images_already_built(registry, existing):
    (build,) = get_os_deps_image_builds("3.12", ("ci",))
    suffix = "" if existing == "merged" else "-amd64"
    registry.digests[f"{REPOSITORY}:{build.content_tag}{suffix}"] = OS_DEPS_DIGEST

    result = CliRunner().invoke(
        build_os_deps_images,
        ["--python", "3.12", "--image-type", "ci", "--platform", "linux/amd64"],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert registry.builds == []


def test_publish_os_deps_images_merges_the_platforms_and_moves_the_tags(registry, tmp_path):
    (build,) = get_os_deps_image_builds("3.12", ("ci",))
    platform_images = [
        f"{REPOSITORY}:{build.content_tag}-{platform.split('/')[-1]}" for platform in OS_DEPS_IMAGE_PLATFORMS
    ]
    for platform_image in platform_images:
        registry.digests[platform_image] = "sha256:" + "0f" * 32
    github_output = tmp_path / "github_output"

    result = CliRunner().invoke(
        publish_os_deps_images,
        ["--python", "3.12", "--image-type", "ci"],
        env={"GITHUB_OUTPUT": str(github_output)},
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert registry.copies == [
        (platform_images, [f"{REPOSITORY}:{build.content_tag}"]),
        (
            [f"{REPOSITORY}:{build.content_tag}@{OS_DEPS_DIGEST}"],
            [f"{REPOSITORY}:ci-{PYTHON_TAG}"],
        ),
    ]
    assert github_output.read_text() == "moved-os-deps-images=true\n"


def test_publish_os_deps_images_does_not_move_tags_already_there(registry, tmp_path):
    (build,) = get_os_deps_image_builds("3.12", ("ci",))
    registry.digests[f"{REPOSITORY}:{build.content_tag}"] = OS_DEPS_DIGEST
    registry.digests[f"{REPOSITORY}:{build.latest_tag}"] = OS_DEPS_DIGEST
    github_output = tmp_path / "github_output"

    result = CliRunner().invoke(
        publish_os_deps_images,
        ["--python", "3.12", "--image-type", "ci"],
        env={"GITHUB_OUTPUT": str(github_output)},
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert registry.copies == []
    assert github_output.read_text() == "moved-os-deps-images=false\n"


def test_publish_os_deps_images_fails_for_a_platform_not_built(registry):
    (build,) = get_os_deps_image_builds("3.12", ("ci",))
    registry.digests[f"{REPOSITORY}:{build.content_tag}-amd64"] = OS_DEPS_DIGEST

    result = CliRunner().invoke(
        publish_os_deps_images,
        ["--python", "3.12", "--image-type", "ci"],
        env={"GITHUB_OUTPUT": None},
        catch_exceptions=False,
    )

    assert result.exit_code == 1
    assert registry.copies == []


@pytest.mark.parametrize(
    ("pins", "expected"),
    [
        pytest.param(
            PINS,
            {"DEV_OS_DEPS_IMAGE": _pinned("dev"), "RUNTIME_OS_DEPS_IMAGE": _pinned("runtime")},
            id="pinned",
        ),
        pytest.param({}, {}, id="not-pinned"),
    ],
)
@mock.patch(f"{PATCH_PREFIX}.perform_environment_checks", autospec=True)
@mock.patch(f"{PATCH_PREFIX}.check_remote_ghcr_io_commands", autospec=True)
@mock.patch(f"{PATCH_PREFIX}.check_airflow_cache_builder_configured", autospec=True)
@mock.patch(f"{PATCH_PREFIX}.run_command", autospec=True)
def test_release_prod_images_start_from_the_pinned_os_deps_images(
    mock_run_command, mock_builder, mock_ghcr, mock_environment, pins, expected, tmp_path
):
    with mock.patch.dict(HARDENED_OS_DEPS_IMAGE_DIGESTS, pins, clear=True):
        result = CliRunner().invoke(
            release_prod_images,
            [
                "--airflow-version",
                "3.2.0",
                "--python",
                "3.12",
                "--platform",
                "linux/amd64",
                "--metadata-folder",
                str(tmp_path),
                "--skip-latest",
            ],
            catch_exceptions=False,
        )

    assert result.exit_code == 0
    command = mock_run_command.call_args.args[0]
    build_args = dict(command[i + 1].split("=", 1) for i, arg in enumerate(command) if arg == "--build-arg")
    assert {name: build_args[name] for name in build_args if name.endswith("_OS_DEPS_IMAGE")} == expected
