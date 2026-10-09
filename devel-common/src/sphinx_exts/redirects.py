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
"""Based on: https://github.com/sphinx-contrib/redirects"""

from __future__ import annotations

import os

from sphinx.builders import html as builders
from sphinx.util import logging

TEMPLATE = '<html><head><meta http-equiv="refresh" content="0; url={}"/></head></html>'

log = logging.getLogger(__name__)


def iter_redirects(app):
    """Yield source and resolved destination paths for redirects."""
    redirect_file_path = os.path.join(app.srcdir, app.config.redirects_file)
    if not os.path.exists(redirect_file_path):
        log.info("Could not find the redirect file: %s", redirect_file_path)
        return

    in_suffix = next(iter(app.config.source_suffix.keys()))

    with open(redirect_file_path) as redirects:
        for line in redirects:
            if not line.strip() or line.startswith("#"):
                continue

            from_path, _, to_path = line.rstrip().partition(" ")

            log.debug("Redirecting '%s' to '%s'", from_path, to_path)

            from_path = from_path.replace(in_suffix, ".html")
            to_path = to_path.replace(in_suffix, ".html")

            # Preserve the existing handling of provider redirect paths.
            depth = len(from_path.split(os.path.sep)) - 1
            if (
                from_path.startswith("_api/airflow/")
                and "_api/airflow/providers" not in from_path
                and "providers" in to_path
            ):
                to_path_prefix = f"..{os.path.sep}" * depth
            elif "providers" in to_path:
                to_path_prefix = f"..{os.path.sep}" * (depth + 1)
            else:
                to_path_prefix = f"..{os.path.sep}" * depth

            to_path = to_path_prefix + to_path

            log.debug("Resolved redirect '%s' to '%s'", from_path, to_path)
            yield from_path, to_path


def generate_redirects(app):
    """Generate HTML redirect files."""
    if not isinstance(app.builder, builders.StandaloneHTMLBuilder):
        return

    for from_path, to_path in iter_redirects(app):
        redirected_filename = os.path.join(app.builder.outdir, from_path)
        os.makedirs(os.path.dirname(redirected_filename), exist_ok=True)

        with open(redirected_filename, "w") as f:
            f.write(TEMPLATE.format(to_path))


def setup(app):
    """Setup plugin."""
    app.add_config_value("redirects_file", "redirects", "env")
    app.connect("builder-inited", generate_redirects)
    return {"version": "builtin", "parallel_read_safe": True, "parallel_write_safe": True}
