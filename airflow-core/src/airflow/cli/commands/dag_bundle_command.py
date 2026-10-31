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

from contextlib import contextmanager

from airflow._shared.module_loading import import_string
from airflow.cli.simple_table import AirflowConsole
from airflow.dag_processing.bundles.artifact_store import BundleManifestRefDurabilityError
from airflow.dag_processing.bundles.manager import get_configured_bundle_config
from airflow.dag_processing.bundles.manifest import BundleManifestError, is_valid_bundle_version
from airflow.dag_processing.bundles.manifest_local import (
    ManifestLocalDagBundle,
    publish_manifest_local_dag_bundle,
)
from airflow.exceptions import AirflowConfigException
from airflow.utils.cli import suppress_logs_and_warning


@contextmanager
def _exit_on_error(args, message: str, *error_types: type[Exception]):
    """Turn an expected failure into a one-line error, or keep its traceback with ``--verbose``."""
    try:
        yield
    except error_types as e:
        if args.verbose:
            raise
        raise SystemExit(f"{message}: {e}")


# Not wrapped in action_cli: its audit-log hook writes to the metadata database, which a
# deployment pipeline publishing from CI usually cannot reach.
@suppress_logs_and_warning
def dag_bundle_publish_local(args) -> None:
    """Publish a local source directory as a new release of a manifest-backed local Dag bundle."""
    expected_current_version = args.expected_current_version
    if expected_current_version is not None and not is_valid_bundle_version(expected_current_version):
        raise SystemExit(
            f"--expected-current-version {expected_current_version!r} is not a bundle version; "
            "expected 'sha256-' followed by 64 lowercase hex digits."
        )

    load_error = f"Cannot load Dag bundle {args.bundle_name!r}"
    # Import only the named bundle's class: the publishing host need not have the packages of
    # the other configured bundles installed.
    with _exit_on_error(args, load_error, AirflowConfigException, ImportError, ValueError):
        bundle_config = get_configured_bundle_config(args.bundle_name)
        bundle_class = import_string(bundle_config.classpath)
    # Checked before construction, which can fail for other bundle types, e.g. on a missing connection.
    if not (isinstance(bundle_class, type) and issubclass(bundle_class, ManifestLocalDagBundle)):
        raise SystemExit(
            f"Dag bundle {args.bundle_name!r} is configured with {bundle_config.classpath}, not a "
            "ManifestLocalDagBundle. Only manifest-backed local Dag bundles can be published with "
            "this command."
        )
    # Bundle classes raise TypeError for invalid kwargs.
    with _exit_on_error(args, load_error, TypeError):
        bundle = bundle_class(name=args.bundle_name, version=None, **bundle_config.kwargs)

    try:
        result = publish_manifest_local_dag_bundle(
            bundle=bundle,
            source_path=args.source_path,
            expected_current_version=expected_current_version,
        )
    except BundleManifestRefDurabilityError as e:
        if args.verbose:
            raise
        raise SystemExit(f"Cannot confirm publication of Dag bundle {args.bundle_name!r}: {e}") from None
    except (BundleManifestError, OSError, ValueError) as e:
        if args.verbose:
            raise
        raise SystemExit(f"Failed to publish Dag bundle {args.bundle_name!r}: {e}") from None

    # No colour: FORCE_COLOR, often set in CI, would otherwise put ANSI codes into JSON output.
    AirflowConsole(color_system=None).print_as(
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
