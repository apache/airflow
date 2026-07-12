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

from airflow.providers.islo.exceptions import IsloError, IsloUnfencedLaunchError


def test_unfenced_launch_error_preserves_reconciliation_context() -> None:
    launch_error = TimeoutError("response lost")
    delete_error = OSError("delete failed")
    error = IsloUnfencedLaunchError("sandbox", launch_error, delete_error)

    assert isinstance(error, IsloError)
    assert error.sandbox_name == "sandbox"
    assert error.launch_error is launch_error
    assert error.delete_error is delete_error
