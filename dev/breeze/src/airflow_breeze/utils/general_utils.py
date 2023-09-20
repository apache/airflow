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

providers_prefix = "apache-airflow-providers-"


def get_provider_name_from_short_hand(short_form_providers: tuple[str]) -> list[str]:
    providers = []
    for short_form_provider in short_form_providers:
        if short_form_provider == "providers-index":
            providers.append("apache-airflow-providers")
            continue

        short_form_provider.split(".")
        parts = "-".join(short_form_provider.split("."))
        providers.append(providers_prefix + parts)
    return providers
