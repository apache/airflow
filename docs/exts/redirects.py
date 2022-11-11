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


def generate_redirects(app):
    """Generate redirects files."""
    redirect_file_path = os.path.join(app.srcdir, app.config.redirects_file)
    if not os.path.exists(redirect_file_path):
        log.info("Could not found the redirect file: %s", redirect_file_path)
        return

    in_suffix = next(iter(app.config.source_suffix.keys()))

    if not isinstance(app.builder, builders.StandaloneHTMLBuilder):
        return

    with open(redirect_file_path) as redirects:
        for line in redirects.readlines():
            # Skip empty line
            if not line.strip():
                continue

            # Skip comments
            if line.startswith("#"):
                continue

            from_path, _, to_path = line.rstrip().partition(" ")

            log.debug("Redirecting '%s' to '%s'", from_path, to_path)

            from_path = from_path.replace(in_suffix, ".html")
            to_path = to_path.replace(in_suffix, ".html")

            to_path_prefix = f"..{os.path.sep}" * (len(from_path.split(os.path.sep)) - 1)
            to_path = to_path_prefix + to_path

            log.debug("Resolved redirect '%s' to '%s'", from_path, to_path)

            redirected_filename = os.path.join(app.builder.outdir, from_path)
            redirected_directory = os.path.dirname(redirected_filename)

            os.makedirs(redirected_directory, exist_ok=True)

            with open(redirected_filename, "w") as f:
                f.write(TEMPLATE.format(to_path))


def setup(app):
    """Setup plugin"""
    app.add_config_value("redirects_file", "redirects", "env")
    app.connect("builder-inited", generate_redirects)
