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

from providers.common.compat.src.airflow.providers.common.compat.version_compat import AIRFLOW_V_3_0_PLUS

# This module is probably only needed for the combination of fab provider 1.5 and airflow 2.x
# It was used during the small window of time when fab provider was based off of main
# after main was version 3 dev and so it was sorta straddling the two versions.
# At that time datasets were renamed assets, but fab provider was still trying to be
# compatible with Airflow 2.x.
# Probably we should not have put this stuff in here but hey.

if AIRFLOW_V_3_0_PLUS:
    # it is not expected that this block would ever be reached
    # because only fab provider 2+ is compatible with airflow 3+
    # and fab provider 2+ should use the permissions module in the fab provider
    # but we throw it in here just in case.
    RESOURCE_ASSET = "Datasets"
else:
    RESOURCE_ASSET = "Assets"
RESOURCE_ASSET_ALIAS = "Asset Aliases"
RESOURCE_BACKFILL = "Backfills"
RESOURCE_DAG_VERSION = "DAG Versions"


__all__ = ["RESOURCE_ASSET", "RESOURCE_ASSET_ALIAS", "RESOURCE_BACKFILL", "RESOURCE_DAG_VERSION"]
