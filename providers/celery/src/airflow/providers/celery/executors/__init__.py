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

import packaging.version

from airflow import __version__ as airflow_version
from airflow.exceptions import AirflowOptionalProviderFeatureException

base_version = packaging.version.parse(airflow_version).base_version

if packaging.version.parse(base_version) < packaging.version.parse("2.7.0"):
    raise AirflowOptionalProviderFeatureException(
        "Celery Executor from Celery Provider should only be used with Airflow 2.7.0+.\n"
        f"This is Airflow {airflow_version} and Celery and CeleryKubernetesExecutor are "
        f"available in the 'airflow.executors' package. You should not use "
        f"the provider's executors in this version of Airflow."
    )
