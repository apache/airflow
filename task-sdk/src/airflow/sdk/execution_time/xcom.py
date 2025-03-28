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

from airflow.configuration import conf
from airflow.sdk.bases.xcom import BaseXCom


def resolve_xcom_backend():
    """
    Resolve custom XCom class.

    :returns: returns the custom XCom class if configured.
    """
    clazz = conf.getimport("core", "xcom_backend", fallback="airflow.sdk.bases.xcom.BaseXCom")
    if not clazz:
        return BaseXCom
    if not issubclass(clazz, BaseXCom):
        raise TypeError(
            f"Your custom XCom class `{clazz.__name__}` is not a subclass of `{BaseXCom.__name__}`."
        )
    return clazz


XCom = resolve_xcom_backend()
