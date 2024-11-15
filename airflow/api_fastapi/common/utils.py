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

from datetime import datetime

from fastapi import HTTPException, status
from pendulum.parsing import ParserError

from airflow.utils import timezone


def format_datetime(value: str) -> datetime:
    """
    Format datetime objects.

    If it can't be parsed, it returns an HTTP 400 exception.
    """
    try:
        return timezone.parse(value)
    except (ParserError, TypeError) as err:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail=f"Incorrect datetime argument: {err}")
