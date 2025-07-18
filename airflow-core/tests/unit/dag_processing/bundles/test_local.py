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

from pathlib import Path

from airflow.dag_processing.bundles.local import LocalDagBundle

from tests_common.test_utils.config import conf_vars


class TestLocalDagBundle:
    def test_path(self):
        bundle = LocalDagBundle(name="test", path="/hello")
        assert bundle.path == Path("/hello")

    @conf_vars({("core", "dags_folder"): "/tmp/somewhere/dags"})
    def test_path_default(self):
        bundle = LocalDagBundle(name="test", refresh_interval=300)
        assert bundle.path == Path("/tmp/somewhere/dags")

    def test_none_for_version(self):
        assert LocalDagBundle.supports_versioning is False

        bundle = LocalDagBundle(name="test", path="/hello")

        assert bundle.get_current_version() is None
