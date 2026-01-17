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

from airflow import settings
from airflow.providers.edge3.models.db import EdgeDBManager
from airflow.utils.providers_configuration_loader import providers_configuration_loaded


@providers_configuration_loaded
def resetdb(args):
    """Reset the Edge metadata database."""
    print(f"DB: {settings.engine.url!r}")
    if not (args.yes or input("This will drop Edge tables. Proceed? (y/n)").upper() == "Y"):
        raise SystemExit("Cancelled")
    EdgeDBManager(settings.Session()).resetdb(skip_init=args.skip_init)


@providers_configuration_loaded
def migratedb(args):
    """Migrate the Edge metadata database."""
    print(f"DB: {settings.engine.url!r}")
    EdgeDBManager(settings.Session()).initdb()
    print("Edge database migration complete.")
