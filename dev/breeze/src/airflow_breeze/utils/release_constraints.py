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

from airflow_breeze.utils.confirm import confirm_action
from airflow_breeze.utils.console import console_print
from airflow_breeze.utils.gh_workflow_utils import trigger_workflow_and_monitor

RELEASE_CONSTRAINTS_WORKFLOW = "release-constraints.yml"
APACHE_AIRFLOW_REPO = "apache/airflow"


def publish_constraints(*, version: str, ref: str, workflow_branch: str = "main") -> None:
    """Resolve, publish and tag the constraints belonging to ``version``.

    ``version`` alone decides what happens: a candidate (``3.1.3rc1``) pins the providers at the
    versions PyPI holds - the wave being voted on is there only as rc versions - and lands on a
    branch of its own, leaving the branch every other build reads where it was. Re-running it for
    the same candidate replaces that branch and its tag, so a wave can be re-cut. A final
    (``3.1.3``) resolves against the published releases and commits onto ``constraints-X-Y``, which
    is what makes the released constraints the baseline everything downstream reads.
    """
    stage = "candidate" if "rc" in version else "final"
    if not confirm_action(
        f"Trigger the release-constraints workflow for the {stage} {version} (resolved from {ref})?"
    ):
        return
    console_print(f"[info]Resolving constraints for {version} from {ref} - this runs on CI runners.")
    trigger_workflow_and_monitor(
        workflow_name=RELEASE_CONSTRAINTS_WORKFLOW,
        repo=APACHE_AIRFLOW_REPO,
        branch=workflow_branch,
        version=version,
        ref=ref,
    )
    console_print(f"[success]Constraints for {version} published and tagged 'constraints-{version}'")
