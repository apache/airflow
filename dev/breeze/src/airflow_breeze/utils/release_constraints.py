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
"""Handing the constraints of a release over to the workflow that resolves them.

A release used to tag whatever the ``constraints-X-Y`` branch happened to hold, so the constraints
shipped with a version were a resolution made by the last CI build rather than one made for that
version. They are resolved per release now, by the ``release-constraints`` workflow - on runners
rather than on the release manager's machine, which would otherwise need a CI image for every
supported Python before it could cut a release.

The workflow derives the stage from the version it is given, so nothing here has to pass a
separate switch that could disagree with it.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from airflow_breeze.utils.confirm import confirm_action
from airflow_breeze.utils.console import console_print
from airflow_breeze.utils.gh_workflow_utils import trigger_workflow_and_monitor

RELEASE_CONSTRAINTS_WORKFLOW = "release-constraints.yml"
APACHE_AIRFLOW_REPO = "apache/airflow"
PYPI_JSON_API_URL = "https://pypi.org/pypi/{distribution}/json"
PYPI_LOOKUP_PARALLELISM = 16


def _find_newest_published_version(distribution: str) -> str | None:
    """Newest non-yanked version of ``distribution`` on PyPI, candidates included."""
    import requests
    from packaging.version import InvalidVersion, Version

    response = requests.get(PYPI_JSON_API_URL.format(distribution=distribution), timeout=60)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    newest: Version | None = None
    for version, files in response.json().get("releases", {}).items():
        try:
            parsed = Version(version)
        except InvalidVersion:
            continue
        if not any(not pypi_file.get("yanked") for pypi_file in files):
            continue
        if newest is None or parsed > newest:
            newest = parsed
    return str(newest) if newest else None


def find_pending_provider_candidates() -> list[tuple[str, str]]:
    """Providers whose newest PyPI version is a candidate - what the final constraints run refuses."""
    from packaging.version import Version

    from airflow_breeze.utils.provider_dependencies import get_provider_dependencies

    distributions = [
        f"apache-airflow-providers-{provider_id.replace('.', '-')}"
        for provider_id, metadata in sorted(get_provider_dependencies().items())
        if metadata["state"] == "ready"
    ]
    with ThreadPoolExecutor(max_workers=PYPI_LOOKUP_PARALLELISM) as executor:
        newest_versions = list(executor.map(_find_newest_published_version, distributions))
    return [
        (distribution, version)
        for distribution, version in zip(distributions, newest_versions)
        if version is not None and Version(version).is_prerelease
    ]


def publish_constraints(
    *,
    version: str,
    ref: str,
    workflow_branch: str = "main",
    fail_on_pending_provider_candidates: bool | None = None,
) -> None:
    """Resolve, publish and tag the constraints belonging to ``version``.

    ``version`` alone decides what happens: a candidate (``3.3.1rc1``) pins the providers at the
    versions PyPI holds - the wave being voted on is there only as rc versions - and lands on a
    branch of its own, leaving the branch every other build reads where it was. Re-running it for
    the same candidate replaces that branch and its tag, so a wave can be re-cut. A final
    (``3.3.1``) resolves against the published releases and commits onto ``constraints-X-Y``, which
    is what makes the released constraints the baseline everything downstream reads.

    ``fail_on_pending_provider_candidates`` overrides the workflow's own derivation. Left ``None``
    the workflow derives it from the version (a final fails when a provider's newest PyPI version is
    a pending rc); ``False`` pins the currently-released providers and proceeds, for a final that
    deliberately ships past an overlapping, independent provider wave.
    """
    from packaging.version import Version

    parsed = Version(version)
    if not parsed.is_prerelease:
        stage = "final"
    elif parsed.pre and parsed.pre[0] == "b":
        stage = "beta"
    else:
        stage = "candidate"
    if not confirm_action(
        f"Trigger the release-constraints workflow for the {stage} {version} (resolved from {ref})?"
    ):
        return
    console_print(f"[info]Resolving constraints for {version} from {ref} - this runs on CI runners.")
    # The workflow input name is hyphenated, so it cannot be a keyword argument; "" lets the
    # workflow derive the guard from the version, which is the default when no override is given.
    overrides = {
        "fail-on-pending-provider-candidates": ""
        if fail_on_pending_provider_candidates is None
        else str(fail_on_pending_provider_candidates).lower()
    }
    trigger_workflow_and_monitor(
        workflow_name=RELEASE_CONSTRAINTS_WORKFLOW,
        repo=APACHE_AIRFLOW_REPO,
        branch=workflow_branch,
        version=version,
        ref=ref,
        **overrides,
    )
    console_print(f"[success]Constraints for {version} published and tagged 'constraints-{version}'")
