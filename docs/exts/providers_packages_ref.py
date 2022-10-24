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

from provider_yaml_utils import load_package_data
from sphinx.application import Sphinx


def _on_config_inited(app, config):
    del app
    jinja_context = getattr(config, "jinja_contexts", None) or {}

    jinja_context["providers_ctx"] = {"providers": load_package_data()}

    config.jinja_contexts = jinja_context


def setup(app: Sphinx):
    """Setup plugin"""
    app.setup_extension("sphinx_jinja")
    app.connect("config-inited", _on_config_inited)
    app.add_crossref_type(
        directivename="provider",
        rolename="provider",
    )
    return {"parallel_read_safe": True, "parallel_write_safe": True}
