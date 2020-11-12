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
import hashlib
import json
import os
import sys
import tempfile
from distutils.file_util import copy_file
from functools import lru_cache
from typing import Dict

import requests
from sphinx.builders import html as builders
from sphinx.util import logging

log = logging.getLogger(__name__)


def _gethash(string: str):
    hash_object = hashlib.sha256(string.encode())
    return hash_object.hexdigest()


def _user_cache_dir(appname=None):
    """Return full path to the user-specific cache dir for this application"""
    if sys.platform == "win32":
        # Windows has a complex procedure to download the App Dir directory because this directory can be
        # changed in window registry, so i use temporary directory for cache
        path = os.path.join(tempfile.gettempdir(), appname)
    elif sys.platform == 'darwin':
        path = os.path.expanduser('~/Library/Caches')
    else:
        path = os.getenv('XDG_CACHE_HOME', os.path.expanduser('~/.cache'))
    path = os.path.join(path, appname)
    return path


@lru_cache(maxsize=None)
def fetch_and_cache(script_url: str, output_filename: str):
    """Fetch URL to local cache and returns path."""
    cache_key = _gethash(script_url)
    cache_dir = _user_cache_dir("redoc-doc")
    cache_metadata_filepath = os.path.join(cache_dir, "cache-metadata.json")
    cache_filepath = os.path.join(cache_dir, f"{cache_key}-{output_filename}")
    # Create cache directory
    os.makedirs(cache_dir, exist_ok=True)
    # Load cache metadata
    cache_metadata: Dict[str, str] = {}
    if os.path.exists(cache_metadata_filepath):
        try:
            with open(cache_metadata_filepath) as cache_file:
                cache_metadata = json.load(cache_file)
        except json.JSONDecodeError:
            os.remove(cache_metadata_filepath)
    etag = cache_metadata.get(cache_key)

    # If we have a file and etag, check the fast path
    if os.path.exists(cache_filepath) and etag:
        res = requests.get(script_url, headers={"If-None-Match": etag})
        if res.status_code == 304:
            return cache_filepath

    # Slow patch
    res = requests.get(script_url)
    res.raise_for_status()

    with open(cache_filepath, "wb") as output_file:
        output_file.write(res.content)

    # Save cache metadata, if needed
    etag = res.headers.get('etag', None)
    if etag:
        cache_metadata[cache_key] = etag
        with open(cache_metadata_filepath, 'w') as cache_file:
            json.dump(cache_metadata, cache_file)

    return cache_filepath


def builder_inited(app):
    """Sphinx "builder-inited" event handler."""
    if not isinstance(app.builder, builders.StandaloneHTMLBuilder):
        log.warning(
            F"The plugin is support only 'html' builder, but you are using '{type(app.builder)}'. Skipping..."
        )
        return
    script_url = app.config.redoc_script_url
    output_filename = "script.js"

    fetch_and_cache(script_url, output_filename)


def build_finished(app, exception):
    """Sphinx "build-finished" event handler."""
    if exception or not isinstance(app.builder, builders.StandaloneHTMLBuilder):
        return
    script_url = app.config.redoc_script_url
    output_filename = "script.js"

    cache_filepath = fetch_and_cache(script_url, output_filename)
    copy_file(cache_filepath, os.path.join(app.builder.outdir, '_static', "redoc.js"))


def setup(app):
    """Setup plugin"""
    app.add_config_value("redoc_script_url", None, "env")
    app.connect("builder-inited", builder_inited)
    app.connect("build-finished", build_finished)
