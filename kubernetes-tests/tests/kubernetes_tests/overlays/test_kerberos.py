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

"""Behavioural smoke test for chart/kustomize-overlays/kerberos.

Run via:

    breeze k8s smoke-test-overlay kerberos

The runner sets ``OVERLAY_UNDER_TEST=kerberos``, ``OVERLAY_NAMESPACE``,
and ``OVERLAY_RELEASE_NAME`` in the environment. The declarative checks
in the overlay's STATUS.yaml ``verify:`` block already cover "every
resource came up"; this module adds the behavioural assertion that the
keytab the bootstrap Job materialised actually works against the
in-cluster KDC.
"""

from __future__ import annotations

import base64
import json
import os
import subprocess
import uuid

import pytest

NAMESPACE = os.environ.get("OVERLAY_NAMESPACE", "airflow")
RELEASE_NAME = os.environ.get("OVERLAY_RELEASE_NAME", "airflow")
SECRET_NAME = f"{RELEASE_NAME}-kerberos-keytab"
PRINCIPAL = f"airflow/airflow.{NAMESPACE}.svc.cluster.local@EXAMPLE.COM"
KDC_HOST = f"{RELEASE_NAME}-kerberos-kdc.{NAMESPACE}.svc.cluster.local"


def _kubectl(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["kubectl", *args], check=False, capture_output=True, text=True)


def test_keytab_secret_is_non_empty():
    """The bootstrap Job must have populated the keytab Secret."""
    result = _kubectl("get", "secret", SECRET_NAME, "-n", NAMESPACE, "-o", "json")
    assert result.returncode == 0, f"Secret {SECRET_NAME} missing: {result.stderr}"
    secret = json.loads(result.stdout)
    keytab_b64 = secret.get("data", {}).get("airflow.keytab")
    assert keytab_b64, "Secret has no airflow.keytab key"
    keytab_bytes = base64.b64decode(keytab_b64)
    assert len(keytab_bytes) > 0, "Keytab is empty"
    # MIT keytab files start with the keytab version word 0x0502 or 0x0501.
    assert keytab_bytes[:1] == b"\x05", (
        f"Keytab does not look like an MIT keytab (first byte={keytab_bytes[0]:#x})"
    )


@pytest.mark.timeout(180)
def test_kinit_against_in_cluster_kdc():
    """A throwaway client pod must be able to kinit using the keytab."""
    pod_name = f"kerberos-client-{uuid.uuid4().hex[:8]}"
    # Use the official Debian image and install krb5-user at runtime — keeps
    # the test self-contained without baking a custom client image.
    overrides = {
        "spec": {
            "restartPolicy": "Never",
            "containers": [
                {
                    "name": "client",
                    "image": "debian:bookworm-slim",
                    "command": ["/bin/bash", "-ceu"],
                    "args": [
                        "apt-get update -qq >/dev/null && "
                        "DEBIAN_FRONTEND=noninteractive apt-get install -yqq krb5-user >/dev/null && "
                        f"kinit -kt /keytab/airflow.keytab {PRINCIPAL} && "
                        "klist"
                    ],
                    "volumeMounts": [
                        {"name": "krb5-conf", "mountPath": "/etc/krb5.conf", "subPath": "krb5.conf"},
                        {"name": "keytab", "mountPath": "/keytab", "readOnly": True},
                    ],
                }
            ],
            "volumes": [
                {"name": "krb5-conf", "configMap": {"name": f"{RELEASE_NAME}-krb5-conf"}},
                {"name": "keytab", "secret": {"secretName": SECRET_NAME}},
            ],
        }
    }
    run_result = _kubectl(
        "run",
        pod_name,
        "-n",
        NAMESPACE,
        "--image=debian:bookworm-slim",
        "--restart=Never",
        "--attach=false",
        f"--overrides={json.dumps(overrides)}",
    )
    assert run_result.returncode == 0, f"kubectl run failed: {run_result.stderr}"
    try:
        wait_result = _kubectl(
            "wait",
            "--for=jsonpath={.status.phase}=Succeeded",
            f"pod/{pod_name}",
            "-n",
            NAMESPACE,
            "--timeout=150s",
        )
        logs = _kubectl("logs", pod_name, "-n", NAMESPACE).stdout
        assert wait_result.returncode == 0, (
            f"Client pod did not Succeed within timeout: {wait_result.stderr}\n--- logs ---\n{logs}"
        )
        assert "Default principal:" in logs, f"klist output missing principal line:\n{logs}"
        assert PRINCIPAL in logs, f"Expected principal {PRINCIPAL!r} not in klist output:\n{logs}"
    finally:
        _kubectl("delete", "pod", pod_name, "-n", NAMESPACE, "--ignore-not-found=true", "--wait=false")
