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

"""
Bugs in sphinx-autoapi using metaclasses prevent us from upgrading to 1.3
which has implicit namespace support. Until that time, we make it look
like a real package for building docs
"""
import os

from sphinx.application import Sphinx

ROOT_PROJECT_DIR = os.path.abspath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir)
)

PROVIDER_INIT_FILE = os.path.join(ROOT_PROJECT_DIR, "airflow", "providers", "__init__.py")


def _create_init_py(app, config):
    del app
    del config
    with open(PROVIDER_INIT_FILE, "wt"):
        pass


def _delete_init_py(app, exception):
    del app
    del exception
    if os.path.exists(PROVIDER_INIT_FILE):
        os.remove(PROVIDER_INIT_FILE)


def setup(app: Sphinx):
    """
    Sets the plugin up and returns configuration of the plugin.

    :param app: application.
    :return json description of the configuration that is needed by the plugin.
    """
    app.connect("config-inited", _create_init_py)
    app.connect("build-finished", _delete_init_py)

    return {"version": "builtin", "parallel_read_safe": True, "parallel_write_safe": True}
