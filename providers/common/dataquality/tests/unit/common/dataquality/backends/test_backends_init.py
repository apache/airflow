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

import subprocess
import sys


def test_get_backend_from_config_does_not_eagerly_import_object_storage():
    """
    ``ObjectStorageResultsBackend`` pulls in ``ObjectStoragePath`` (fsspec/upath), which
    ``get_backend_from_config()`` shouldn't pay for at import time when no ``results_path`` is
    configured -- it's only needed once a backend is actually built. Runs in a subprocess for a
    clean ``sys.modules`` unaffected by other tests importing the submodule directly.
    """
    script = (
        "import sys\n"
        "from airflow.providers.common.dataquality.backends import get_backend_from_config\n"
        "assert 'airflow.providers.common.dataquality.backends.object_storage' not in sys.modules\n"
        "assert get_backend_from_config() is None\n"
        "assert 'airflow.providers.common.dataquality.backends.object_storage' not in sys.modules\n"
    )
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr
