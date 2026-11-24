#
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

from datetime import datetime
from typing import TYPE_CHECKING, Any

import sqlalchemy as sa
from sqlalchemy import Integer, select, tuple_
from sqlalchemy.orm import Mapped, mapped_column

from airflow._shared.timezones import timezone
from airflow.models.base import Base, StringID
from airflow.utils.sqlalchemy import UtcDateTime

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sqlalchemy.orm import Session


class DagBundleManifest(Base):
    """
    Manifest of one published version of a manifest-backed Dag bundle.

    Rows are immutable. The scheduler ships the manifest to workers in ``BundleInfo.version_data``.
    """

    __tablename__ = "dag_bundle_manifest"

    bundle_name: Mapped[str] = mapped_column(StringID(length=250), primary_key=True)
    bundle_version: Mapped[str] = mapped_column(StringID(length=200), primary_key=True)
    manifest: Mapped[dict[str, Any]] = mapped_column(sa.JSON(), nullable=False)
    created_at: Mapped[datetime] = mapped_column(UtcDateTime, default=timezone.utcnow, nullable=False)


class DagBundleManifestRef(Base):
    """
    Release reference of a manifest-backed Dag bundle, which names its current version.

    ``revision`` makes replacing the payload a compare-and-swap. ``version`` repeats the payload's
    version so cleanup can match it without reading JSON.
    """

    __tablename__ = "dag_bundle_manifest_ref"

    bundle_name: Mapped[str] = mapped_column(StringID(length=250), primary_key=True)
    version: Mapped[str | None] = mapped_column(StringID(length=200), nullable=True)
    payload: Mapped[dict[str, Any]] = mapped_column(sa.JSON(), nullable=False)
    revision: Mapped[int] = mapped_column(Integer, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        UtcDateTime, default=timezone.utcnow, onupdate=timezone.utcnow, nullable=False
    )


def get_bundle_manifests(keys: Iterable[tuple[str, str]], *, session: Session) -> dict[tuple[str, str], dict]:
    """Return the stored manifest for each ``(bundle_name, bundle_version)`` that has one."""
    unique_keys = set(keys)
    if not unique_keys:
        return {}
    rows = session.execute(
        select(
            DagBundleManifest.bundle_name, DagBundleManifest.bundle_version, DagBundleManifest.manifest
        ).where(tuple_(DagBundleManifest.bundle_name, DagBundleManifest.bundle_version).in_(unique_keys))
    )
    return {(bundle_name, bundle_version): manifest for bundle_name, bundle_version, manifest in rows}
