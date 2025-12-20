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
import re
from pathlib import Path
from typing import TYPE_CHECKING

from pagefind.index import IndexConfig, PagefindIndex
from sphinx.util.fileutil import copy_asset

if TYPE_CHECKING:
    from sphinx.application import Sphinx

logger = logging.getLogger(__name__)


def add_content_weights_lightweight(
    output_dir: Path, glob_pattern: str, exclude_patterns: list[str] | None = None
) -> int:
    """Add data-pagefind-weight attributes using simple regex replacement.

    :param output_dir: Output directory
    :param glob_pattern: Glob pattern
    :param exclude_patterns: Exclude patterns
    :return: Number of files processed
    """
    files_processed = 0
    exclude_patterns = exclude_patterns or []

    # Regex patterns to match opening tags without existing weight attribute
    # Use maximum valid weights (0.0-10.0 range, quadratic scale)
    # https://pagefind.app/docs/weighting/
    # Weight of 10.0 = ~100x impact, 7.0 = ~49x impact (default h1), 5.0 = ~25x impact
    patterns = [
        (re.compile(r"<title(?![^>]*data-pagefind-weight)"), '<title data-pagefind-weight="10.0"'),
        (re.compile(r"<h1(?![^>]*data-pagefind-weight)"), '<h1 data-pagefind-weight="9.0"'),
    ]

    for html_file in output_dir.glob(glob_pattern):
        if not html_file.is_file():
            continue

        # Check if file matches any exclude pattern (using simple prefix matching)
        relative_path = html_file.relative_to(output_dir)
        relative_str = str(relative_path)
        if any(relative_str.startswith(pattern.rstrip("/*")) for pattern in exclude_patterns):
            continue

        try:
            content = html_file.read_text(encoding="utf-8")
            modified_content = content

            for pattern, replacement in patterns:
                modified_content = pattern.sub(replacement, modified_content)

            if modified_content != content:
                html_file.write_text(modified_content, encoding="utf-8")
                files_processed += 1

        except Exception as e:
            logger.warning("Failed to add weights to %s: %s", html_file, e)

    return files_processed


async def build_pagefind_index(app: Sphinx) -> dict[str, int]:
    """Build Pagefind search index using Python API."""
    output_dir = Path(app.builder.outdir)
    pagefind_dir = output_dir / "_pagefind"

    # Add content weighting if enabled
    if getattr(app.config, "pagefind_content_weighting", True):
        logger.info("Adding content weights to HTML files...")
        exclude_patterns = getattr(app.config, "pagefind_exclude_patterns", [])
        files_processed = add_content_weights_lightweight(
            output_dir, app.config.pagefind_glob, exclude_patterns
        )
        logger.info("Added content weights to %s files", files_processed)

    config = IndexConfig(
        root_selector=app.config.pagefind_root_selector,
        exclude_selectors=app.config.pagefind_exclude_selectors,
        output_path=str(pagefind_dir),
        verbose=app.config.pagefind_verbose,
        force_language=app.config.language or "en",
        keep_index_url=False,
        write_playground=getattr(app.config, "pagefind_enable_playground", False),
    )

    logger.info("Building Pagefind search index...")

    exclude_patterns = getattr(app.config, "pagefind_exclude_patterns", [])

    if exclude_patterns:
        # Need to index files individually to apply exclusion patterns
        logger.info("Indexing with exclusion patterns: %s", exclude_patterns)
        indexed = 0
        skipped = 0

        async with PagefindIndex(config=config) as index:
            for html_file in output_dir.glob(app.config.pagefind_glob):
                if not html_file.is_file():
                    continue

                relative_path = html_file.relative_to(output_dir)
                relative_str = str(relative_path)

                # Check if path matches any exclude pattern (prefix matching)
                if any(relative_str.startswith(pattern.rstrip("/*")) for pattern in exclude_patterns):
                    skipped += 1
                    continue

                try:
                    content = html_file.read_text(encoding="utf-8")
                    await index.add_html_file(
                        content=content,
                        source_path=str(html_file),
                        url=str(relative_path),
                    )
                    indexed += 1
                except Exception as e:
                    logger.warning("Failed to index %s: %s", relative_path, e)

            logger.info("Pagefind indexed %s pages (excluded %s)", indexed, skipped)

            if app.config.pagefind_custom_records:
                for record in app.config.pagefind_custom_records:
                    try:
                        await index.add_custom_record(**record)
                    except Exception as e:
                        logger.warning("Failed to add custom record: %s", e)

            return {"page_count": indexed}
    else:
        # No exclusions - use fast directory indexing
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

            return {"page_count": page_count}


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
        page_count = result.get("page_count", 0)

        if page_count == 0:
            raise RuntimeError("Pagefind indexing failed: no pages were indexed")

        logger.info("✓ Pagefind index created with %s pages", page_count)
    except Exception as e:
        logger.exception("Failed to build Pagefind index")
        raise RuntimeError(f"Pagefind indexing failed: {e}") from e


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
