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

import os

from flask import Blueprint


def init_react_ui(app):
    dev_mode = os.environ.get("DEV_MODE", False) == "true"

    bp = Blueprint(
        "ui",
        __name__,
        # The dev mode index file points to the vite dev server instead of static build files
        static_folder="../../ui/dev" if dev_mode else "../../ui/dist",
        static_url_path="/ui",
    )

    @bp.route("/ui", defaults={"page": ""})
    @bp.route("/ui/<page>")
    def index(page):
        return bp.send_static_file("index.html")

    app.register_blueprint(bp)
