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
"""Sphinx extension to generate Airflow database ERD diagrams at doc build time.

Detects which package is being built via the ``AIRFLOW_PACKAGE_NAME`` environment
variable and generates the appropriate ERD as Mermaid ER-diagram markup, consumed
by the ``.. mermaid::`` directive from ``sphinxcontrib-mermaid``:

* ``apache-airflow`` — core Airflow models only
* ``apache-airflow-providers-fab`` — FAB auth-manager models only
* ``apache-airflow-providers-edge3`` — Edge3 provider models only
"""

from __future__ import annotations

import os
from pathlib import Path

from sphinx.util import logging

log = logging.getLogger(__name__)

PLACEHOLDER_MERMAID = """\
erDiagram
  ERD_NOT_GENERATED {
    string reason "eralchemy is not available"
  }
"""


def _write_placeholder(mmd_path: str) -> None:
    """Write placeholder Mermaid markup so the doc build does not break with missing content."""
    Path(mmd_path).write_text(PLACEHOLDER_MERMAID)


def _collect_core_metadata():
    """Collect SQLAlchemy MetaData for core Airflow models."""
    from sqlalchemy import MetaData

    from airflow.models import import_all_models
    from airflow.models.base import Base

    import_all_models()

    metadata = MetaData()
    for table in Base.metadata.tables.values():
        table.to_metadata(metadata)
    log.info("Collected %d core model tables", len(metadata.tables))
    return metadata


def _collect_fab_metadata():
    """Collect SQLAlchemy MetaData for FAB provider models."""
    from flask_appbuilder import Model as FABModel
    from sqlalchemy import MetaData

    import airflow.providers.fab.auth_manager.models  # noqa: F401

    metadata = MetaData()
    for table in FABModel.metadata.tables.values():
        table.to_metadata(metadata)
    log.info("Collected %d FAB provider tables", len(metadata.tables))
    return metadata


def _collect_edge3_metadata():
    """Collect SQLAlchemy MetaData for Edge3 provider models."""
    from sqlalchemy import MetaData

    import airflow.providers.edge3.models.edge_job
    import airflow.providers.edge3.models.edge_logs
    import airflow.providers.edge3.models.edge_worker  # noqa: F401
    from airflow.providers.edge3.models.edge_base import edge_metadata

    metadata = MetaData()
    for table in edge_metadata.tables.values():
        table.to_metadata(metadata)
    log.info("Collected %d Edge3 provider tables", len(metadata.tables))
    return metadata


# Map package names to their metadata collector and output filename.
_PACKAGE_ERD_CONFIG: dict[str, tuple] = {
    "apache-airflow": (_collect_core_metadata, "airflow_erd.mmd"),
    "apache-airflow-providers-fab": (_collect_fab_metadata, "fab_erd.mmd"),
    "apache-airflow-providers-edge3": (_collect_edge3_metadata, "edge3_erd.mmd"),
}


def builder_inited(app):
    """Generate the ERD diagram Mermaid markup from SQLAlchemy metadata during doc build."""
    package_name = os.environ.get("AIRFLOW_PACKAGE_NAME", "")
    config = _PACKAGE_ERD_CONFIG.get(package_name)
    if config is None:
        return

    collector, filename = config

    src_dir = app.srcdir
    img_dir = os.path.join(src_dir, "img")
    mmd_path = os.path.join(img_dir, filename)

    os.makedirs(img_dir, exist_ok=True)

    try:
        from eralchemy.main import _intermediary_to_mermaid_er, all_to_intermediary, filter_resources
    except ImportError:
        log.warning("eralchemy is not installed, skipping ERD diagram generation")
        _write_placeholder(mmd_path)
        return

    log.info("Generating ERD diagram for %s at %s", package_name, mmd_path)

    try:
        metadata = collector()
    except ImportError:
        log.warning("Could not import models for %s, skipping ERD generation", package_name)
        _write_placeholder(mmd_path)
        return

    # Bypass eralchemy's `render_er`/`intermediary_to_mermaid_er`: those wrap the markup in an
    # HTML comment plus a `mermaid.ink` image link, meant for GitHub-flavored markdown READMEs.
    # The `.. mermaid::` Sphinx directive needs the raw `erDiagram ...` markup instead.
    tables, relationships = all_to_intermediary(metadata)
    tables, relationships = filter_resources(
        tables, relationships, exclude_tables=["sqlite_sequence"], sort_mode="alphabetical"
    )
    markup = _intermediary_to_mermaid_er(tables, relationships)
    Path(mmd_path).write_text(markup)

    log.info("ERD diagram generated successfully")


def setup(app):
    app.connect("builder-inited", builder_inited)
    return {"parallel_read_safe": True, "parallel_write_safe": True}
