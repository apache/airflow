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

"""Pagefind index builder and static file handler."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from pagefind.index import IndexConfig, PagefindIndex
from sphinx.util.fileutil import copy_asset

if TYPE_CHECKING:
    from sphinx.application import Sphinx

logger = logging.getLogger(__name__)


async def build_pagefind_index(app: Sphinx) -> dict[str, int]:
    """Build Pagefind search index using Python API."""
    output_dir = Path(app.builder.outdir)
    pagefind_dir = output_dir / "_pagefind"

    config = IndexConfig(
        root_selector=app.config.pagefind_root_selector,
        exclude_selectors=app.config.pagefind_exclude_selectors,
        output_path=str(pagefind_dir),
        verbose=app.config.pagefind_verbose,
        force_language=app.config.language or "en",
        keep_index_url=False,
    )

    logger.info("Building Pagefind search index...")

    async with PagefindIndex(config=config) as index:
        result = await index.add_directory(path=str(output_dir), glob=app.config.pagefind_glob)

        page_count = result.get("page_count", 0)
        logger.info("Pagefind indexed %s pages", page_count)

        if app.config.pagefind_custom_records:
            for record in app.config.pagefind_custom_records:
                try:
                    await index.add_custom_record(**record)
                except Exception as e:
                    logger.warning("Failed to add custom record: %s", e)

        return result


def build_index_finished(app: Sphinx, exception: Exception | None) -> None:
    """Build Pagefind index after HTML build completes."""
    if exception:
        logger.info("Skipping Pagefind indexing due to build errors")
        return

    if app.builder.format != "html":
        return

    if not app.config.pagefind_enabled:
        logger.info("Pagefind indexing disabled (pagefind_enabled=False)")
        return

    try:
        result = asyncio.run(build_pagefind_index(app))
        if result.get("page_count", 0) > 0:
            logger.info("✓ Pagefind index created with %s pages", result["page_count"])
    except Exception:
        logger.exception("Failed to build Pagefind index.")


def copy_static_files(app: Sphinx, exception: Exception | None) -> None:
    """Copy CSS and JS files to _static directory."""
    if exception or app.builder.format != "html":
        return

    static_dir = Path(app.builder.outdir) / "_static"
    extension_static = Path(__file__).parent / "static"

    css_src = extension_static / "css" / "pagefind.css"
    css_dest = static_dir / "css"
    css_dest.mkdir(parents=True, exist_ok=True)
    if css_src.exists():
        copy_asset(str(css_src), str(css_dest))

    js_src = extension_static / "js" / "search.js"
    js_dest = static_dir / "js"
    js_dest.mkdir(parents=True, exist_ok=True)
    if js_src.exists():
        copy_asset(str(js_src), str(js_dest))
