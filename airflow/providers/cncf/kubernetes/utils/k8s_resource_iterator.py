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

from typing import Callable, Iterator

from kubernetes.utils import FailToCreateError

from airflow.providers.cncf.kubernetes.utils.delete_from import FailToDeleteError


def k8s_resource_iterator(callback: Callable[[dict], None], resources: Iterator) -> None:
    failures: list = []
    for data in resources:
        if data is not None:
            if "List" in data["kind"]:
                kind = data["kind"].replace("List", "")
                for yml_doc in data["items"]:
                    if kind != "":
                        yml_doc["apiVersion"] = data["apiVersion"]
                        yml_doc["kind"] = kind
                    try:
                        callback(yml_doc)
                    except (FailToCreateError, FailToDeleteError) as failure:
                        failures.extend(failure.api_exceptions)
            else:
                try:
                    callback(data)
                except (FailToCreateError, FailToDeleteError) as failure:
                    failures.extend(failure.api_exceptions)
    if failures:
        raise FailToCreateError(failures)
