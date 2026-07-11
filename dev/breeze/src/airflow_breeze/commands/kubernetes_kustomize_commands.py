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
"""``breeze k8s`` commands for kustomize overlays under ``chart/kustomize-overlays/``.

Split out of ``kubernetes_commands.py`` to keep that module focused on the
core cluster/test lifecycle. The commands here still register on the shared
``kubernetes_group`` (imported below), so they remain ``breeze k8s ...``
subcommands.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from typing import Any, Literal

import click
import yaml

from airflow_breeze.commands.common_options import (
    option_dry_run,
    option_python,
    option_verbose,
)
from airflow_breeze.commands.kubernetes_commands import (
    _docker_pull_with_429_retry,
    kubernetes_group,
    option_executor,
    option_kubernetes_version,
)
from airflow_breeze.utils.console import console_print
from airflow_breeze.utils.kubernetes_utils import (
    CHART_PATH,
    KUBECTL_BIN_PATH,
    get_k8s_env,
    get_kind_cluster_name,
    get_kubeconfig_file,
    make_sure_kubernetes_tools_are_installed,
    run_command_with_k8s_env,
)
from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH
from airflow_breeze.utils.run_utils import run_command

# Allow-list of third-party container images that kustomize overlays under
# chart/kustomize-overlays/ may declare. `breeze k8s smoke-test-overlay`
# auto-discovers every `image:` in the rendered manifest and `docker pull`s it
# into kind; this list bounds *what* it may pull so an overlay cannot make CI
# pull and run an arbitrary, unreviewed image. It is a deliberately-reviewed
# gate: introducing an overlay image means editing both the overlay manifest
# (owned by `/chart/` in .github/CODEOWNERS) and this list (owned by `/dev/`),
# so a maintainer must approve before CI will pull it. Keep entries pinned to
# the exact `image:` string the overlay declares.
ALLOWED_OVERLAY_IMAGES: frozenset[str] = frozenset(
    {
        "gcavalcante8808/krb5-server:latest",  # kerberos overlay KDC + client test pod
        "alpine/k8s:1.31.0",  # kerberos overlay keytab-bootstrap job
    }
)

# `breeze k8s smoke-test-overlay` — functional smoke test for a single
# kustomize overlay under chart/kustomize-overlays/.
#
# Counterpart to the structural `build_kustomize_overlays` prek hook:
# the prek hook validates that an overlay builds and that its STATUS.yaml
# parses, while this command applies the overlay to a running kind
# cluster, waits for every resource declared in the STATUS.yaml `verify:`
# block, and optionally runs a per-overlay pytest module for behavioural
# checks. An overlay's STATUS may only advance to `tested` once this
# command exits 0.
# ---------------------------------------------------------------------------

KUSTOMIZE_OVERLAYS_PATH = CHART_PATH / "kustomize-overlays"
# Behavioural overlay tests live under chart/tests/overlay_tests/.
# They are NOT discovered by `breeze testing helm-tests --test-type all`
# because `overlay_tests` is in chart/pyproject.toml's norecursedirs
# — only this command (which invokes pytest by explicit path) sees them.
OVERLAY_TESTS_DIR = CHART_PATH / "tests" / "overlay_tests"


def _substitute_overlay_placeholders(rendered: str, release_name: str, namespace: str) -> str:
    return rendered.replace("RELEASE-NAME", release_name).replace("NAMESPACE", namespace)


def _resolve_verify_resource_name(name: str, release_name: str) -> str:
    """Auto-prepend ``<release_name>-`` to a verify-block resource name.

    STATUS.yaml's ``verify:`` entries name resources by their **suffix
    only** (e.g. ``kerberos-kdc``) - the runner prepends the release name
    so the same overlay can be installed under any release. A legacy
    ``RELEASE-NAME-foo`` form is still tolerated so older overlays keep
    working after the schema change: the literal ``RELEASE-NAME-`` prefix
    is stripped before the auto-prepend, leaving the suffix to be
    re-prefixed with the actual release name.
    """
    if name.startswith("RELEASE-NAME-"):
        name = name[len("RELEASE-NAME-") :]
    return f"{release_name}-{name}"


def _load_overlay_verify_block(overlay_dir: Path) -> dict[str, Any]:
    status_path = overlay_dir / "STATUS.yaml"
    if not status_path.exists():
        console_print(f"[error]Overlay {overlay_dir.name} is missing STATUS.yaml")
        sys.exit(1)
    status_doc = yaml.safe_load(status_path.read_text()) or {}
    verify = status_doc.get("verify")
    if not verify:
        console_print(
            f"[error]Overlay {overlay_dir.name} has no `verify:` block in STATUS.yaml; "
            "add one before running the smoke test (see chart/kustomize-overlays/CONTRIBUTING.rst)."
        )
        sys.exit(1)
    return verify


# Container `state.waiting.reason` values that indicate the pod is not going
# to recover without human intervention. Treated as an immediate
# verify-block failure rather than waiting for the configured timeout.
_TERMINAL_POD_WAITING_REASONS: frozenset[str] = frozenset(
    {
        "ImagePullBackOff",
        "ErrImagePull",
        "InvalidImageName",
        "ImageInspectError",
        "CrashLoopBackOff",
        "CreateContainerConfigError",
        "CreateContainerError",
    }
)


def _has_terminal_pod_failure(
    kind: str,
    name: str,
    namespace: str,
    env: dict[str, str],
    kubectl: str,
) -> tuple[bool, str]:
    """Return (failed, reason) for pods backing this verify resource.

    Inspects waiting reasons on every regular and init container of every
    pod selected by the resource's controller. The selector lookup mirrors
    what the controller itself uses: ``spec.selector.matchLabels`` for
    Deployment / StatefulSet / DaemonSet, the auto-applied
    ``job-name=<name>`` label for Job. Resources without backing pods
    (Service, Secret, ConfigMap, CRDs, …) are always treated as not-failed
    here; their progress is observed via the success condition alone.
    """
    if kind not in ("Deployment", "StatefulSet", "DaemonSet", "Job"):
        return False, ""
    if kind == "Job":
        selector = f"job-name={name}"
    else:
        sel = run_command(
            [
                kubectl,
                "get",
                kind.lower(),
                name,
                "-n",
                namespace,
                "-o",
                "go-template={{range $k,$v := .spec.selector.matchLabels}}{{$k}}={{$v}},{{end}}",
            ],
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )
        if sel.returncode != 0 or not sel.stdout.strip():
            return False, ""
        selector = sel.stdout.strip().rstrip(",")
    pods = run_command(
        [
            kubectl,
            "get",
            "pods",
            "-n",
            namespace,
            "-l",
            selector,
            "-o",
            (
                "jsonpath={range .items[*]}"
                "{range .status.containerStatuses[*]}{.state.waiting.reason}|{end}"
                "{range .status.initContainerStatuses[*]}{.state.waiting.reason}|{end}"
                "{end}"
            ),
        ],
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    if pods.returncode != 0:
        return False, ""
    for reason in (r for r in pods.stdout.split("|") if r):
        if reason in _TERMINAL_POD_WAITING_REASONS:
            return True, reason
    return False, ""


def _wait_for_verify_resource(
    resource: dict[str, Any],
    namespace: str,
    timeout_seconds: int,
    env: dict[str, str],
) -> int:
    """Poll a verify-block resource to success or terminal failure.

    Each iteration runs a one-second kubectl check for the success
    condition and, in the same cycle, inspects backing pods (if any) for
    terminal waiting reasons (ImagePullBackOff/CrashLoopBackOff/…). The
    moment a terminal reason appears the loop aborts with a non-zero
    return; otherwise it sleeps and retries up to the configured deadline.

    Branch matrix:
      * ``ready: true`` -> rollout status (Deployment/StatefulSet/DaemonSet)
      * ``complete: true`` -> wait for condition=complete (Job)
      * neither -> wait for the resource to be created (handles Secrets /
        ConfigMaps / CRD children that an overlay's own Job or controller
        materialises asynchronously; returns immediately for synchronously
        applied resources)
    """
    kind = resource["kind"]
    name = resource["name"]
    kubectl = str(KUBECTL_BIN_PATH)
    ns_args = ["-n", namespace]
    target = f"{kind.lower()}/{name}"
    if resource.get("ready") and kind in ("Deployment", "StatefulSet", "DaemonSet"):
        check_cmd = [kubectl, "rollout", "status", target, *ns_args, "--timeout=1s"]
        readiness = "ready"
    elif resource.get("complete") and kind == "Job":
        check_cmd = [kubectl, "wait", "--for=condition=complete", target, *ns_args, "--timeout=1s"]
        readiness = "complete"
    else:
        check_cmd = [kubectl, "wait", "--for=create", target, *ns_args, "--timeout=1s"]
        readiness = "created"
    console_print(f"[info]verify: waiting for {kind}/{name} to be {readiness} (timeout={timeout_seconds}s)")
    deadline = time.monotonic() + timeout_seconds
    poll_interval = 5
    last_err = ""
    while time.monotonic() < deadline:
        success = run_command(check_cmd, env=env, check=False, capture_output=True, text=True)
        if success.returncode == 0:
            console_print(f"[success]verify: {kind}/{name} is {readiness}")
            return 0
        last_err = (success.stderr or "").strip()
        failed, reason = _has_terminal_pod_failure(kind, name, namespace, env, kubectl)
        if failed:
            console_print(
                f"[error]verify failed early for {kind}/{name}: backing pod in {reason}; "
                "not waiting out the full timeout."
            )
            run_command([kubectl, "describe", kind.lower(), name, *ns_args], env=env, check=False)
            run_command([kubectl, "get", "pods", *ns_args, "-o", "wide"], env=env, check=False)
            return 1
        time.sleep(poll_interval)
    console_print(
        f"[error]verify timed out for {kind}/{name} after {timeout_seconds}s. Last error: {last_err}"
    )
    run_command([kubectl, "describe", kind.lower(), name, *ns_args], env=env, check=False)
    return 1


def _render_overlay(
    overlay_dir: Path,
    release_name: str,
    namespace: str,
    env: dict[str, str],
) -> str | None:
    kubectl = str(KUBECTL_BIN_PATH)
    render = run_command(
        [kubectl, "kustomize", str(overlay_dir)],
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    if render.returncode != 0:
        console_print(f"[error]kubectl kustomize failed:\n{render.stderr}")
        return None
    return _substitute_overlay_placeholders(render.stdout, release_name, namespace)


def _discover_overlay_images(manifest: str) -> list[str]:
    """Extract every container image referenced by the rendered manifest.

    Walks every loaded YAML doc and collects every ``image:`` string value,
    regardless of nesting depth, so it picks up containers, initContainers,
    sidecars under any pod-spec-bearing kind (Deployment, StatefulSet,
    DaemonSet, Job, CronJob, Pod) without needing per-kind logic.
    """
    images: set[str] = set()

    def _walk(node: Any) -> None:
        if isinstance(node, dict):
            for k, v in node.items():
                if k == "image" and isinstance(v, str):
                    images.add(v)
                else:
                    _walk(v)
        elif isinstance(node, list):
            for item in node:
                _walk(item)

    for doc in yaml.safe_load_all(manifest):
        if doc:
            _walk(doc)
    return sorted(images)


def _find_disallowed_overlay_images(images: list[str]) -> list[str]:
    """Return the overlay images that are not on the ``ALLOWED_OVERLAY_IMAGES`` allow-list.

    Bounds what ``smoke-test-overlay`` may ``docker pull`` so an overlay cannot make
    CI pull and run an arbitrary, unreviewed image. See ``ALLOWED_OVERLAY_IMAGES``.
    """
    return [image for image in images if image not in ALLOWED_OVERLAY_IMAGES]


def _preload_overlay_images(
    manifest: str,
    python: str,
    kubernetes_version: str,
) -> int:
    """Pre-pull every image the overlay references and ``kind load`` it.

    Same pattern as ``_preload_test_images_to_kind`` but driven by what the
    overlay actually declares, so it stays in sync as overlays evolve and
    works for any overlay without a per-overlay images list. With
    imagePullPolicy=IfNotPresent set on the overlay's pods (the convention),
    kubelet never reaches out to a registry once these are loaded — so the
    smoke test does not flake on Docker Hub rate limits or registry outages.
    """
    images = _discover_overlay_images(manifest)
    if not images:
        return 0
    disallowed = _find_disallowed_overlay_images(images)
    if disallowed:
        console_print(
            "[error]Overlay references image(s) not on the allow-list: "
            f"{disallowed}.\nAdd them to ALLOWED_OVERLAY_IMAGES in "
            "dev/breeze/src/airflow_breeze/commands/kubernetes_kustomize_commands.py "
            "(a maintainer-reviewed change) before the smoke test will pull them."
        )
        return 1
    cluster_name = get_kind_cluster_name(python=python, kubernetes_version=kubernetes_version)
    console_print(
        f"[info]Preloading {len(images)} overlay image(s) into kind cluster {cluster_name}: {images}"
    )
    for image in images:
        pull_rc = _docker_pull_with_429_retry(image, output=None)
        if pull_rc != 0:
            console_print(f"[error]docker pull {image} failed")
            return pull_rc
        kind_load = run_command_with_k8s_env(
            ["kind", "load", "docker-image", "--name", cluster_name, image],
            python=python,
            kubernetes_version=kubernetes_version,
            check=False,
        )
        if kind_load.returncode != 0:
            console_print(f"[error]kind load docker-image {image} into {cluster_name} failed")
            return kind_load.returncode
    return 0


def _apply_or_delete_overlay(
    action: Literal["apply", "delete"],
    manifest: str,
    namespace: str,
    env: dict[str, str],
) -> int:
    kubectl = str(KUBECTL_BIN_PATH)
    extra: list[str] = ["--ignore-not-found=true"] if action == "delete" else []
    result = run_command(
        [kubectl, action, "-n", namespace, *extra, "-f", "-"],
        env=env,
        check=False,
        input=manifest,
        text=True,
    )
    return result.returncode


class _SequenceIndentingDumper(yaml.SafeDumper):
    """yaml.SafeDumper variant that indents sequence items under their key.

    PyYAML's default safe_dump output emits ``resources:\\n- kind: …`` which
    yamllint (with ``indent-sequences: true``, the repo's default) rejects
    with "expected 4 but found 2". Overriding ``increase_indent`` to pass
    ``indentless=False`` produces ``resources:\\n  - kind: …`` instead, which
    matches the rest of the YAML in the repo and keeps the yamllint hook
    green on the auto-promoted STATUS.yaml.
    """

    def increase_indent(self, flow: bool = False, indentless: bool = False) -> None:
        return super().increase_indent(flow, False)


def _promote_overlay_status(overlay_dir: Path) -> int:
    """Rewrite STATUS.yaml in-place to ``status: tested``.

    Preserves everything above the YAML document separator ``---``.
    Re-emits the document body with status fields refreshed and the
    existing ``verify:`` block carried over.

    Idempotent: if the overlay is already ``tested``, ``chart-version``
    and ``last-verified`` are refreshed to current values. ``deprecated``
    overlays are refused.
    """
    import datetime

    status_path = overlay_dir / "STATUS.yaml"
    original = status_path.read_text()
    sep_idx = original.find("\n---")
    if sep_idx >= 0:
        header = original[:sep_idx] + "\n---\n"
        body = original[sep_idx + len("\n---") :]
    else:
        header = ""
        body = original
    doc = yaml.safe_load(body) or {}
    if doc.get("status") == "deprecated":
        console_print(
            f"[error]Refusing to promote {status_path.relative_to(AIRFLOW_ROOT_PATH)}: "
            "status is `deprecated`. Remove the deprecation first."
        )
        return 1
    chart_meta = yaml.safe_load((CHART_PATH / "Chart.yaml").read_text())
    promoted: dict[str, Any] = {
        "status": "tested",
        "chart-version": str(chart_meta["version"]),
        "last-verified": datetime.date.today().isoformat(),
    }
    verify = doc.get("verify")
    if verify:
        promoted["verify"] = verify
    rendered = yaml.dump(
        promoted,
        Dumper=_SequenceIndentingDumper,
        sort_keys=False,
        default_flow_style=False,
    )
    status_path.write_text(header + rendered)
    console_print(
        f"[success]Promoted {status_path.relative_to(AIRFLOW_ROOT_PATH)}: "
        f"status=tested chart-version={promoted['chart-version']} "
        f"last-verified={promoted['last-verified']}"
    )
    console_print(f"[info]Review with `git diff {status_path.relative_to(AIRFLOW_ROOT_PATH)}` and commit.")
    return 0


def _run_overlay_pytest(
    overlay_name: str,
    namespace: str,
    release_name: str,
    python: str,
    kubernetes_version: str,
    executor: str,
) -> int:
    test_file = OVERLAY_TESTS_DIR / f"test_{overlay_name}.py"
    if not test_file.exists():
        console_print(
            f"[info]No behavioural test module at {test_file.relative_to(AIRFLOW_ROOT_PATH)} — "
            "verify-block checks are the only assertions for this overlay."
        )
        return 0
    env = get_k8s_env(python=python, kubernetes_version=kubernetes_version, executor=executor)
    env["OVERLAY_UNDER_TEST"] = overlay_name
    env["OVERLAY_NAMESPACE"] = namespace
    env["OVERLAY_RELEASE_NAME"] = release_name
    pytest_cmd = ["uv", "run", "pytest", str(test_file.relative_to(CHART_PATH)), "-xvs"]
    console_print(f"[info]Running behavioural tests: {' '.join(pytest_cmd)}")
    result = run_command(pytest_cmd, env=env, check=False, cwd=CHART_PATH.as_posix())
    return result.returncode


def _is_ci() -> bool:
    """Detect GitHub Actions / generic CI."""
    return os.environ.get("CI", "").lower() == "true"


def _smoke_test_overlay_impl(
    overlay_name: str,
    overlay_dir: Path,
    verify: dict[str, Any],
    python: str,
    kubernetes_version: str,
    executor: str,
    release_name: str,
    namespace: str,
    skip_cleanup: bool,
    no_pytest: bool,
) -> int:
    """Run the smoke test and return a single exit code.

    Kept return-based (not ``sys.exit``-based) so the click wrapper can
    decide whether to swallow the failure (CI + not-tested) or surface
    it as a non-zero process exit.
    """
    env = get_k8s_env(python=python, kubernetes_version=kubernetes_version, executor=executor)
    console_print(f"\n[info]Rendering overlay {overlay_name}...")
    manifest = _render_overlay(overlay_dir, release_name, namespace, env)
    if manifest is None:
        return 1
    console_print("\n[info]Preloading overlay images into kind cluster...")
    if _preload_overlay_images(manifest, python, kubernetes_version) != 0:
        console_print("[error]Image preload failed.")
        return 1
    console_print(f"\n[info]Applying overlay {overlay_name} to namespace {namespace}...")
    if _apply_or_delete_overlay("apply", manifest, namespace, env) != 0:
        console_print("[error]Overlay apply failed.")
        return 1
    try:
        console_print("\n[info]Walking STATUS.yaml verify block...")
        timeout = int(verify.get("timeout_seconds", 300))
        for resource in verify["resources"]:
            substituted = {**resource, "name": _resolve_verify_resource_name(resource["name"], release_name)}
            if _wait_for_verify_resource(substituted, namespace, timeout, env) != 0:
                console_print("[error]verify block failed.")
                return 1
        console_print("\n[success]verify block passed.")
        if not no_pytest:
            rc = _run_overlay_pytest(
                overlay_name=overlay_name,
                namespace=namespace,
                release_name=release_name,
                python=python,
                kubernetes_version=kubernetes_version,
                executor=executor,
            )
            if rc != 0:
                console_print("[error]Per-overlay pytest module failed.")
                return rc
    finally:
        if skip_cleanup:
            console_print("[warning]--skip-cleanup set; overlay left in place.")
        else:
            console_print(f"\n[info]Cleaning up overlay {overlay_name}...")
            _apply_or_delete_overlay("delete", manifest, namespace, env)
    return 0


@kubernetes_group.command(
    name="smoke-test-overlay",
    help="Apply a kustomize overlay to the current KinD cluster, wait for its STATUS.yaml "
    "`verify:` resources, and run the optional per-overlay pytest module.",
)
@click.argument("overlay_name", type=str)
@option_python
@option_kubernetes_version
@option_executor
@click.option(
    "--release-name",
    default="airflow",
    show_default=True,
    help="Substitute for the RELEASE-NAME placeholder in the overlay.",
)
@click.option(
    "--namespace",
    default="airflow",
    show_default=True,
    help="Namespace to apply into and substitute for the NAMESPACE placeholder.",
)
@click.option(
    "--skip-cleanup",
    is_flag=True,
    help="Leave the overlay applied after the run (useful for debugging).",
)
@click.option(
    "--no-pytest",
    is_flag=True,
    help="Skip the per-overlay pytest module even if it exists.",
)
@click.option(
    "--promote-status",
    is_flag=True,
    help=(
        "If the run is green (verify block + per-overlay pytest both pass), rewrite the "
        "overlay's STATUS.yaml in place to `status: tested` with chart-version from "
        "chart/Chart.yaml and today's date as last-verified. Opt-in because it modifies "
        "a checked-in file; review with `git diff` and commit. Refused when CI=true."
    ),
)
@option_verbose
@option_dry_run
def smoke_test_overlay(
    overlay_name: str,
    python: str,
    kubernetes_version: str,
    executor: str,
    release_name: str,
    namespace: str,
    skip_cleanup: bool,
    no_pytest: bool,
    promote_status: bool,
):
    in_ci = _is_ci()
    if promote_status and in_ci:
        console_print(
            "[error]Refusing --promote-status when CI=true is set. "
            "STATUS.yaml is a checked-in file: it must be flipped locally by a developer "
            "(the deliberate human claim 'I verified this against my cluster') and committed. "
            "CI re-runs the smoke test to verify the existing STATUS, not to mutate it."
        )
        sys.exit(1)
    make_sure_kubernetes_tools_are_installed()
    overlay_dir = KUSTOMIZE_OVERLAYS_PATH / overlay_name
    if not (overlay_dir / "kustomization.yaml").is_file():
        console_print(
            f"[error]No overlay at {overlay_dir.relative_to(AIRFLOW_ROOT_PATH)} "
            f"(expected a kustomization.yaml there)."
        )
        sys.exit(1)
    # Two-step pre-flight. The kubeconfig file persists across docker
    # restarts even though the kind cluster is gone, so checking only
    # the file leads to the next step (`kind load docker-image`) hanging
    # silently while it tries to reach a cluster that no longer exists.
    # `kind get clusters` returns the current list of live clusters and
    # is the actual source of truth.
    kubeconfig = get_kubeconfig_file(python=python, kubernetes_version=kubernetes_version)
    cluster_name = get_kind_cluster_name(python=python, kubernetes_version=kubernetes_version)
    bootstrap_hint = (
        f"[error]Run first:\n"
        f"  breeze k8s deploy-cluster --python {python} --kubernetes-version {kubernetes_version}"
        f" --rebuild-base-image  # --rebuild-base-image only needed first run\n"
        f"  breeze k8s deploy-airflow --python {python} --kubernetes-version {kubernetes_version}"
    )
    if not kubeconfig.exists():
        console_print(
            f"[error]No kubeconfig at {kubeconfig} for python={python}, "
            f"kubernetes={kubernetes_version} — cluster was never created.\n"
        )
        console_print(bootstrap_hint)
        sys.exit(1)
    kind_list = run_command(
        ["kind", "get", "clusters"],
        env=get_k8s_env(python=python, kubernetes_version=kubernetes_version),
        check=False,
        capture_output=True,
        text=True,
    )
    live_clusters = (kind_list.stdout or "").splitlines() if kind_list.returncode == 0 else []
    if cluster_name not in (c.strip() for c in live_clusters):
        console_print(
            f"[error]Kind cluster {cluster_name!r} not running (kubeconfig at {kubeconfig} "
            "is left over from a previous run — docker restart, manual cluster delete, "
            "or kind binary upgrade typically causes this).\n"
        )
        console_print(bootstrap_hint)
        sys.exit(1)
    verify = _load_overlay_verify_block(overlay_dir)
    status_doc = yaml.safe_load((overlay_dir / "STATUS.yaml").read_text()) or {}
    overlay_status = status_doc.get("status", "not-tested")
    soft_fail_in_ci = in_ci and overlay_status == "not-tested"
    if soft_fail_in_ci:
        console_print(
            f"[info]CI mode + status={overlay_status}: any smoke-test failure will be reported "
            "but exit 0 (PoC overlays do not gate CI). Flip STATUS to `tested` to make failures "
            "gate CI."
        )

    exit_code = _smoke_test_overlay_impl(
        overlay_name=overlay_name,
        overlay_dir=overlay_dir,
        verify=verify,
        python=python,
        kubernetes_version=kubernetes_version,
        executor=executor,
        release_name=release_name,
        namespace=namespace,
        skip_cleanup=skip_cleanup,
        no_pytest=no_pytest,
    )

    if exit_code == 0:
        console_print(f"\n[success]Smoke test for overlay {overlay_name} passed.")
        if promote_status:
            promote_rc = _promote_overlay_status(overlay_dir)
            if promote_rc != 0:
                sys.exit(promote_rc)
        else:
            console_print(
                f"[info]To flip {overlay_dir.relative_to(AIRFLOW_ROOT_PATH)}/STATUS.yaml to "
                "`status: tested` automatically on a green run, re-run with --promote-status "
                "(opt-in because it edits a checked-in file; refused when CI=true is set)."
            )
        sys.exit(0)

    # Reached only on a non-zero exit (the success branch above always exits).
    if soft_fail_in_ci:
        console_print(
            f"[warning]Smoke test for overlay {overlay_name} failed (exit={exit_code}), "
            f"but status is `{overlay_status}` and CI=true is set, so the CI job will not "
            "be gated by this failure. Read the warnings above and either fix the overlay or "
            "leave it as `not-tested` until the next iteration."
        )
        sys.exit(0)
    sys.exit(exit_code)
