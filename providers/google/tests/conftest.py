# providers/google/tests/conftest.py

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

import importlib.metadata

import werkzeug

pytest_plugins = "tests_common.pytest_plugin"

# Flask 2.2.x test client reads werkzeug.__version__ which Werkzeug 3.x removed.
# Connexion 2.x used to pin Werkzeug<3, but connexion is now removed from FAB provider.
if not hasattr(werkzeug, "__version__"):
    werkzeug.__version__ = importlib.metadata.version("werkzeug")
