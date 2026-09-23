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

"""Shared fixtures and helpers for kustomize-overlay smoke tests.

Run from `breeze k8s smoke-test-overlay <name>` which sets the
``OVERLAY_UNDER_TEST`` / ``OVERLAY_NAMESPACE`` / ``OVERLAY_RELEASE_NAME``
env vars and ensures kubectl is on PATH against the right kind cluster.
The helpers below are deliberately small and synchronous so a new
overlay's behavioural test can be written without reinventing them.

Conventions for new per-overlay tests:

* Inherit from nothing - use the fixtures here and module-level
  functions. There is no need for ``BaseK8STest`` because overlay tests
  do not need the Airflow REST API.
* Use the ``kubectl`` helper for one-shot commands; use the
  ``run_throwaway_pod`` fixture when you need to exec something inside
  the cluster.
* Reuse images already declared by the overlay (so the smoke-test
  runner's auto-preload covers them). See
  ``chart/kustomize-overlays/CONTRIBUTING.rst`` for the full rules.
"""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Iterator
from typing import Any

import pytest

from overlay_tests._helpers import kubectl


@pytest.fixture(scope="session")
def overlay_namespace() -> str:
    """Namespace the overlay-under-test was applied into.

    Set by ``breeze k8s smoke-test-overlay``; defaults to ``airflow`` so
    the module is also runnable against a hand-rolled cluster.
    """
    return os.environ.get("OVERLAY_NAMESPACE", "airflow")


@pytest.fixture(scope="session")
def overlay_release_name() -> str:
    """Helm release name the overlay-under-test was applied with.

    Set by ``breeze k8s smoke-test-overlay``; defaults to ``airflow``.
    """
    return os.environ.get("OVERLAY_RELEASE_NAME", "airflow")


@pytest.fixture
def run_throwaway_pod() -> Iterator[Any]:
    """Spawn a one-shot pod, wait for it to ``Succeed``, return ``(rc, logs)``.

    Use this when a behavioural assertion needs something to run inside
    the cluster (a CLI tool against an in-cluster service, a smoke probe
    against a Secret/ConfigMap the overlay produced, etc.).

    Each call constructs a Pod from the caller's image, command, args,
    and optional ``overrides`` (a partial Pod spec merged into the
    container; useful for volumeMounts/volumes/env). The fixture:

    * sets ``restartPolicy: Never`` and ``imagePullPolicy: IfNotPresent``
      so the smoke-test runner's image preload is honoured,
    * waits up to ``timeout`` for the pod to reach phase=Succeeded,
    * returns ``(0, logs)`` on success or
      ``(non-zero, "phase=… stderr …\\n--- logs ---\\n…")`` on failure,
    * deletes the pod at teardown regardless of the test outcome.

    Reuse an image already declared by the overlay so it is preloaded
    into the kind nodes - otherwise the pod will sit in ImagePullBackOff.
    """
    spawned: list[tuple[str, str]] = []

    def _spawn(
        *,
        image: str,
        namespace: str,
        command: list[str],
        args: list[str],
        overrides: dict[str, Any] | None = None,
        timeout: str = "150s",
    ) -> tuple[int, str]:
        pod_name = f"overlay-pod-{uuid.uuid4().hex[:8]}"
        container: dict[str, Any] = {
            "name": "client",
            "image": image,
            "imagePullPolicy": "IfNotPresent",
            "command": command,
            "args": args,
        }
        if overrides and "containers" in overrides.get("spec", {}):
            # Caller supplied container fragments (typically volumeMounts);
            # shallow-merge into the synthesised container.
            container.update(overrides["spec"]["containers"][0])
        pod_spec: dict[str, Any] = {
            "spec": {"restartPolicy": "Never", "containers": [container]},
        }
        if overrides and "volumes" in overrides.get("spec", {}):
            pod_spec["spec"]["volumes"] = overrides["spec"]["volumes"]
        spawned.append((pod_name, namespace))
        run = kubectl(
            "run",
            pod_name,
            "-n",
            namespace,
            f"--image={image}",
            "--restart=Never",
            "--attach=false",
            f"--overrides={json.dumps(pod_spec)}",
        )
        if run.returncode != 0:
            return run.returncode, f"kubectl run failed: {run.stderr}"
        wait = kubectl(
            "wait",
            "--for=jsonpath={.status.phase}=Succeeded",
            f"pod/{pod_name}",
            "-n",
            namespace,
            f"--timeout={timeout}",
        )
        logs = kubectl("logs", pod_name, "-n", namespace).stdout
        if wait.returncode != 0:
            phase = kubectl("get", "pod", pod_name, "-n", namespace, "-o", "jsonpath={.status.phase}").stdout
            return wait.returncode, (
                f"pod {pod_name} did not Succeed (phase={phase}): {wait.stderr.strip()}\n--- logs ---\n{logs}"
            )
        return 0, logs

    yield _spawn

    for pod_name, namespace in spawned:
        kubectl("delete", "pod", pod_name, "-n", namespace, "--ignore-not-found=true", "--wait=false")
