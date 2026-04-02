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

from airflow.providers.apache.hive.macros.hive import closest_ds_partition, max_partition
from airflow.providers.common.compat.sdk import AirflowPlugin


class HivePlugin(AirflowPlugin):
    """Hive plugin - delivering macros used by users that use the provider."""

    name = "hive"
    macros = [max_partition, closest_ds_partition]
