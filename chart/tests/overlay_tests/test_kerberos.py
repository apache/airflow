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

The declarative ``verify:`` block in the overlay's STATUS.yaml already
covers "every resource came up". This module adds the behavioural
assertion that the keytab the bootstrap Job materialised actually
authenticates against the in-cluster KDC. It is also a reference for
how a per-overlay test is structured: see ``conftest.py`` next to this
file for the reusable fixtures and helpers.
"""

from __future__ import annotations

import base64

from overlay_tests._helpers import get_secret_data


def test_keytab_secret_is_non_empty(overlay_namespace, overlay_release_name):
    """The bootstrap Job must have populated the keytab Secret with real bytes."""
    secret_name = f"{overlay_release_name}-kerberos-keytab"
    data = get_secret_data(secret_name, overlay_namespace)
    keytab_b64 = data.get("airflow.keytab")
    assert keytab_b64, f"Secret {secret_name} has no airflow.keytab key"
    keytab_bytes = base64.b64decode(keytab_b64)
    assert len(keytab_bytes) > 0, "Keytab is empty"
    # MIT keytab files start with the keytab version word 0x0502 or 0x0501.
    assert keytab_bytes[:1] == b"\x05", (
        f"Keytab does not look like an MIT keytab (first byte={keytab_bytes[0]:#x})"
    )


def test_kinit_against_in_cluster_kdc(overlay_namespace, overlay_release_name, run_throwaway_pod):
    """A throwaway client pod must be able to kinit using the keytab.

    Reuses the same krb5-server image as the KDC pod so the smoke test
    runner's generic image-preload step already loaded it into the kind
    cluster - the test does not introduce a third image dependency. The
    image is Alpine-based and ships busybox sh (not bash) - hence
    ``/bin/sh -c`` rather than ``/bin/bash``.
    """
    secret_name = f"{overlay_release_name}-kerberos-keytab"
    configmap_name = f"{overlay_release_name}-krb5-conf"
    principal = f"airflow/airflow.{overlay_namespace}.svc.cluster.local@EXAMPLE.COM"
    # The KDC readiness probe covers krb5kdc TCP bind, and krb5.conf
    # forces TCP via udp_preference_limit=1 - but a short retry around
    # kinit is still defensive against cold-start ticket-cache races
    # the first time a fresh KDC sees an AS-REQ.
    kinit_with_retry = (
        f"for i in 1 2 3 4 5; do "
        f"kinit -kt /keytab/airflow.keytab {principal} && break; "
        f"echo 'kinit attempt '$i' failed, sleeping 2s' >&2; sleep 2; "
        f"done && klist"
    )
    rc, output = run_throwaway_pod(
        image="gcavalcante8808/krb5-server:latest",
        namespace=overlay_namespace,
        command=["/bin/sh", "-c"],
        args=[kinit_with_retry],
        overrides={
            "spec": {
                "containers": [
                    {
                        "volumeMounts": [
                            {"name": "krb5-conf", "mountPath": "/etc/krb5.conf", "subPath": "krb5.conf"},
                            {"name": "keytab", "mountPath": "/keytab", "readOnly": True},
                        ],
                    }
                ],
                "volumes": [
                    {"name": "krb5-conf", "configMap": {"name": configmap_name}},
                    {"name": "keytab", "secret": {"secretName": secret_name}},
                ],
            }
        },
    )
    assert rc == 0, output
    assert "Default principal:" in output, f"klist output missing principal line:\n{output}"
    assert principal in output, f"Expected principal {principal!r} not in klist output:\n{output}"
