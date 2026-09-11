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
from unittest import mock

from flask import Flask

from airflow.providers.fab.www.extensions.init_manifest_files import configure_manifest_files

MANIFEST = {"app.js": "app.0123456789abcdef.js", "app.css": "app.fedcba9876543210.css"}


def _url_for_asset(flask_app):
    """Pull the ``url_for_asset`` template tag off the registered context processor."""
    return flask_app.template_context_processors[None][-1]()["url_for_asset"]


class TestConfigureManifestFiles:
    def test_a_manifest_entry_is_served_from_the_dist_directory(self):
        flask_app = Flask(__name__)

        with mock.patch("builtins.open", mock.mock_open(read_data=json.dumps(MANIFEST))):
            configure_manifest_files(flask_app)

        with flask_app.test_request_context():
            url_for_asset = _url_for_asset(flask_app)

            # Webpack writes the hashed files under static/dist, so the manifest target
            # has to be prefixed before it is handed to ``url_for``.
            assert url_for_asset("app.js") == "/static/" + os.path.join("dist", "app.0123456789abcdef.js")
            assert url_for_asset("app.css") == "/static/" + os.path.join("dist", "app.fedcba9876543210.css")

    def test_an_unknown_asset_falls_back_to_its_own_name(self):
        flask_app = Flask(__name__)

        with mock.patch("builtins.open", mock.mock_open(read_data=json.dumps(MANIFEST))):
            configure_manifest_files(flask_app)

        with flask_app.test_request_context():
            assert _url_for_asset(flask_app)("not-in-manifest.js") == "/static/not-in-manifest.js"

    def test_a_missing_manifest_leaves_every_asset_unhashed(self):
        flask_app = Flask(__name__)

        # A checkout with no built frontend must not stop the app from starting.
        with mock.patch("builtins.open", side_effect=FileNotFoundError):
            configure_manifest_files(flask_app)

        with flask_app.test_request_context():
            assert _url_for_asset(flask_app)("app.js") == "/static/app.js"

    def test_the_manifest_is_re_read_on_every_lookup_in_debug_mode(self):
        flask_app = Flask(__name__)
        flask_app.debug = True

        with mock.patch("builtins.open", mock.mock_open(read_data=json.dumps(MANIFEST))) as mocked_open:
            configure_manifest_files(flask_app)
            reads_after_setup = mocked_open.call_count

            with flask_app.test_request_context():
                _url_for_asset(flask_app)("app.js")

            assert mocked_open.call_count > reads_after_setup

    def test_the_manifest_is_read_once_when_not_in_debug_mode(self):
        flask_app = Flask(__name__)

        with mock.patch("builtins.open", mock.mock_open(read_data=json.dumps(MANIFEST))) as mocked_open:
            configure_manifest_files(flask_app)
            reads_after_setup = mocked_open.call_count

            with flask_app.test_request_context():
                _url_for_asset(flask_app)("app.js")

            assert mocked_open.call_count == reads_after_setup
