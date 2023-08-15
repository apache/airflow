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

from airflow.exceptions import AirflowException, AirflowSkipException


def raise_failed_or_skiping_exception(
    *, soft_fail: bool = False, failed_message: str, skipping_message: str = ""
) -> None:
    """Raise AirflowSkipException if self.soft_fail is set to True. Otherwise raise AirflowException."""
    if soft_fail:
        raise AirflowSkipException(skipping_message or failed_message)
    raise AirflowException(failed_message)
