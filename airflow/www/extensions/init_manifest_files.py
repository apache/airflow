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

import json
import os

from flask import url_for


def configure_manifest_files(app):
    """
    Load the manifest file and register the `url_for_asset_` template tag.

    :param app:
    """
    manifest = {}

    def parse_manifest_json():
        try:
            manifest_file = os.path.join(
                os.path.dirname(__file__), os.pardir, "static/dist/manifest.json"
            )
            with open(manifest_file) as file:
                manifest.update(json.load(file))

                for source, target in manifest.copy().items():
                    manifest[source] = os.path.join("dist", target)
        except Exception:
            print(
                "Please make sure to build the frontend in static/ directory and restart the server"
            )

    def get_asset_url(filename):
        if app.debug:
            parse_manifest_json()
        return url_for("static", filename=manifest.get(filename, filename))

    parse_manifest_json()

    @app.context_processor
    def get_url_for_asset():
        """
        Template tag to return the asset URL.

        WebPack renders the assets after minification and modification under the
        static/dist folder. This template tag reads the asset name in
        ``manifest.json`` and returns the appropriate file.
        """
        return {"url_for_asset": get_asset_url}
