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

import warnings

from airflow.exceptions import AirflowProviderDeprecationWarning

DEFERRABLE_OVERRIDES_WAIT_WARNING = (
    "deferrable=True currently takes precedence over wait_for_completion=False, so this task still "
    "waits for completion in the triggerer. In the next major release of the Amazon provider, the task "
    "will not wait when wait_for_completion is False. deferrable may come from the [operators] "
    "default_deferrable option. To keep waiting, set wait_for_completion=True. To stop waiting now, "
    "set deferrable=False."
)


def should_defer(*, deferrable: bool, wait_for_completion: bool) -> bool:
    """
    Decide whether an operator that accepts both ``deferrable`` and ``wait_for_completion`` defers.

    Deferring still takes precedence over ``wait_for_completion=False``, with a deprecation warning.
    In the next major release this returns ``deferrable and wait_for_completion``.
    """
    if deferrable and not wait_for_completion:
        warnings.warn(DEFERRABLE_OVERRIDES_WAIT_WARNING, AirflowProviderDeprecationWarning, stacklevel=2)
    return deferrable
