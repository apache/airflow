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

import sys
from pathlib import Path

from cov_runner import run_tests

sys.path.insert(0, str(Path(__file__).parent.resolve()))

source_files = ["airflow/www"]

restapi_files = ["tests/www"]

files_not_fully_covered = [
    "airflow/www/api/experimental/endpoints.py",
    "airflow/www/app.py",
    "airflow/www/auth.py",
    "airflow/www/decorators.py",
    "airflow/www/extensions/init_appbuilder.py",
    "airflow/www/extensions/init_auth_manager.py",
    "airflow/www/extensions/init_dagbag.py",
    "airflow/www/extensions/init_jinja_globals.py",
    "airflow/www/extensions/init_manifest_files.py",
    "airflow/www/extensions/init_security.py",
    "airflow/www/extensions/init_session.py",
    "airflow/www/extensions/init_views.py",
    "airflow/www/fab_security/manager.py",
    "airflow/www/fab_security/sqla/manager.py",
    "airflow/www/forms.py",
    "airflow/www/gunicorn_config.py",
    "airflow/www/security_manager.py",
    "airflow/www/session.py",
    "airflow/www/utils.py",
    "airflow/www/views.py",
    "airflow/www/widgets.py",
]

if __name__ == "__main__":
    args = ["-qq"] + restapi_files
    run_tests(args, source_files, files_not_fully_covered)
