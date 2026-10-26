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
"""Dag bundle subcommands."""

from __future__ import annotations

from airflow.cli.simple_table import AirflowConsole
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.dag_processing.bundles.manifest import BundleManifestError
from airflow.dag_processing.bundles.manifest_local import (
    ManifestLocalDagBundle,
    publish_manifest_local_dag_bundle,
)
from airflow.utils.cli import suppress_logs_and_warning


# Not wrapped in action_cli: its audit-log hook writes to the metadata database, which a
# deployment pipeline publishing from CI usually cannot reach.
@suppress_logs_and_warning
def dag_bundle_publish_local(args) -> None:
    """Publish a local source directory as a new release of a manifest-backed local Dag bundle."""
    try:
        # Bundle classes raise TypeError for invalid kwargs; get_bundle raises ValueError for
        # an unconfigured name.
        bundle = DagBundlesManager().get_bundle(args.bundle_name)
    except (TypeError, ValueError) as e:
        raise SystemExit(f"Cannot load Dag bundle {args.bundle_name!r}: {e}")
    if not isinstance(bundle, ManifestLocalDagBundle):
        raise SystemExit(
            f"Dag bundle {args.bundle_name!r} is a {type(bundle).__name__}, not a ManifestLocalDagBundle. "
            "Only manifest-backed local Dag bundles can be published with this command."
        )

    try:
        result = publish_manifest_local_dag_bundle(
            bundle=bundle,
            source_path=args.source_path,
            expected_current_version=args.expected_current_version,
        )
    # ValueError: the source, published root and cache paths overlap.
    except (BundleManifestError, OSError, ValueError) as e:
        raise SystemExit(f"Failed to publish Dag bundle {args.bundle_name!r}: {e}")

    AirflowConsole().print_as(
        data=[
            {
                "bundle_name": result.bundle_name,
                "version": result.version,
                "version_path": result.version_path,
                "manifest_ref_path": result.manifest_ref_path,
                "manifest_sha256": result.manifest_sha256,
                "file_count": result.file_count,
                "total_size": result.total_size,
                "created_snapshot": result.created_snapshot,
            }
        ],
        output=args.output,
    )
