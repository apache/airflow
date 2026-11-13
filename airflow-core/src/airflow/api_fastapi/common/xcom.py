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

from fastapi import HTTPException, status

from airflow.models.dynamic_region import LOOP_XCOM_PREFIX
from airflow.models.xcom import EXTERNAL_CLEAR_TARGET_XCOM_KEY


def reject_reserved_xcom_key(key: str) -> None:
    """Reject an XCom key that only Airflow writes, from what the Execution API validated."""
    if key.startswith(LOOP_XCOM_PREFIX):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Reserved loop XCom key")
    if key == EXTERNAL_CLEAR_TARGET_XCOM_KEY:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Reserved XCom key")
