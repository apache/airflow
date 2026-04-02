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
variable and generates the appropriate ERD:

* ``apache-airflow`` — core Airflow models only
* ``apache-airflow-providers-fab`` — FAB auth-manager models only
* ``apache-airflow-providers-edge3`` — Edge3 provider models only
"""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

from sphinx.util import logging

log = logging.getLogger(__name__)

PLACEHOLDER_SVG = """\
<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="800" height="100">
  <rect width="800" height="100" fill="#fff3cd" stroke="#ffc107" stroke-width="2" rx="8"/>
  <text x="400" y="40" text-anchor="middle" font-family="sans-serif" font-size="16" fill="#856404">
    ERD diagram not generated: eralchemy or graphviz system package is not available.
  </text>
  <text x="400" y="70" text-anchor="middle" font-family="sans-serif" font-size="14" fill="#856404">
    Install graphviz (e.g. "brew install graphviz") and eralchemy, then rebuild the docs.
  </text>
</svg>
"""


def _write_placeholder(svg_path: str) -> None:
    """Write a placeholder SVG so the doc build does not break with a missing image."""
    Path(svg_path).write_text(PLACEHOLDER_SVG)


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
    "apache-airflow": (_collect_core_metadata, "airflow_erd.svg"),
    "apache-airflow-providers-fab": (_collect_fab_metadata, "fab_erd.svg"),
    "apache-airflow-providers-edge3": (_collect_edge3_metadata, "edge3_erd.svg"),
}


def builder_inited(app):
    """Generate the ERD diagram SVG from SQLAlchemy metadata during doc build."""
    package_name = os.environ.get("AIRFLOW_PACKAGE_NAME", "")
    config = _PACKAGE_ERD_CONFIG.get(package_name)
    if config is None:
        return

    collector, filename = config

    src_dir = app.srcdir
    img_dir = os.path.join(src_dir, "img")
    svg_path = os.path.join(img_dir, filename)

    os.makedirs(img_dir, exist_ok=True)

    try:
        from eralchemy import render_er
    except ImportError:
        log.warning("eralchemy is not installed, skipping ERD diagram generation")
        _write_placeholder(svg_path)
        return

    # eralchemy needs either pygraphviz or graphviz Python package, and both need
    # the graphviz system package (the ``dot`` binary) to render SVG output.
    if not shutil.which("dot"):
        hint = (
            "On macOS, install it with: brew install graphviz"
            if sys.platform == "darwin"
            else "Install graphviz via your system package manager."
        )
        log.warning(
            "graphviz system package is not installed — the 'dot' command is not on PATH. "
            "Skipping ERD diagram generation. %s",
            hint,
        )
        _write_placeholder(svg_path)
        return

    log.info("Generating ERD diagram for %s at %s", package_name, svg_path)

    try:
        metadata = collector()
    except ImportError:
        log.warning("Could not import models for %s, skipping ERD generation", package_name)
        _write_placeholder(svg_path)
        return

    render_er(
        metadata,
        svg_path,
        exclude_tables=["sqlite_sequence"],
    )

    if not os.path.exists(svg_path):
        log.warning("ERD diagram was not generated (eralchemy/graphviz error), writing placeholder")
        _write_placeholder(svg_path)
        return

    log.info("ERD diagram generated successfully")


def setup(app):
    app.connect("builder-inited", builder_inited)
    return {"parallel_read_safe": True, "parallel_write_safe": True}
