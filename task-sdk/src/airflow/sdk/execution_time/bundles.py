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
Materialize a task instance's Dag bundle on the worker.

Kept apart from :mod:`airflow.sdk.execution_time.task_runner` so the supervisor
side — which needs a bundle on disk before it can launch a language-SDK
subprocess — does not have to import the task runner and everything it pulls in.
"""

from __future__ import annotations

import os
from getpass import getuser
from typing import TYPE_CHECKING

from airflow.dag_processing.bundles.base import unpack_bundle_version  # noqa: SDK002
from airflow.dag_processing.bundles.manager import DagBundlesManager  # noqa: SDK002
from airflow.sdk.api.datamodels._generated import BundleInfo
from airflow.sdk.exceptions import AirflowException
from airflow.sdk.execution_time.tracing import detail_span

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger

    from airflow.dag_processing.bundles.base import BaseDagBundle  # noqa: SDK002

__all__ = ["initialize_pinned_bundle", "initialize_ti_bundle", "verify_bundle_access"]


def initialize_pinned_bundle(target: BundleInfo, logger: FilteringBoundLogger) -> BaseDagBundle:
    """
    Materialize *target* at a concrete version, so the tree handed to the caller is lockable.

    A bundle resolved without a version points at the bundle's shared, mutable
    checkout: another task refreshing the same bundle resets it underneath a caller
    still reading it, and ``BundleVersionLock`` cannot protect it because a
    version-less lock is a no-op. Re-resolving at the version current now yields a
    private ``versions/<version>`` tree that the lock does cover.

    Bundles that do not track versions have nothing to pin and keep their single path.
    """
    bundle = initialize_ti_bundle(target)
    if bundle.version is not None:
        return bundle
    version, version_data = unpack_bundle_version(bundle.get_current_version(), bundle)
    if version is None:
        return bundle
    logger.debug("Pinning Dag bundle to its current version", bundle=target.name, version=version)
    return initialize_ti_bundle(BundleInfo(name=target.name, version=version, version_data=version_data))


def initialize_ti_bundle(bundle_info: BundleInfo) -> BaseDagBundle:
    """
    Resolve, initialize, and access-check the Dag bundle for a task instance.

    Shared by :func:`~airflow.sdk.execution_time.task_runner.parse` (Python task
    path) and the subprocess coordinators (language-SDK path), which both need a
    task instance's bundle materialized on disk before use. Returns the
    initialized bundle so callers can read ``bundle.path``.
    """
    bundle_instance = DagBundlesManager().get_bundle(
        name=bundle_info.name,
        version=bundle_info.version,
        version_data=bundle_info.version_data,
    )
    bundle_instance.initialize()
    verify_bundle_access(bundle_instance)
    return bundle_instance


@detail_span("verify_bundle_access")
def verify_bundle_access(bundle_instance: BaseDagBundle) -> None:
    """
    Verify bundle is accessible by the current user.

    This is called after user impersonation (if any) to ensure the bundle
    is actually accessible. Uses os.access() which works with any permission
    scheme (standard Unix permissions, ACLs, SELinux, etc.).

    :param bundle_instance: The bundle instance to check
    :raises AirflowException: if bundle is not accessible
    """
    bundle_path = bundle_instance.path

    if not bundle_path.exists():
        # Already handled by initialize() with a warning
        return

    # Check read permission (and execute for directories to list contents)
    access_mode = os.R_OK
    if bundle_path.is_dir():
        access_mode |= os.X_OK

    if not os.access(bundle_path, access_mode):
        raise AirflowException(
            f"Bundle '{bundle_instance.name}' path '{bundle_path}' is not accessible "
            f"by user '{getuser()}'. When using run_as_user, ensure bundle directories "
            f"are readable by the impersonated user. "
            f"See: https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-bundles.html"
        )
