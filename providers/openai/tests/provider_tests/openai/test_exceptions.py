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

from unittest.mock import Mock

import pytest

from airflow.providers.openai.exceptions import OpenAIBatchJobException, OpenAIBatchTimeout


@pytest.mark.parametrize(
    "exception_class",
    [
        OpenAIBatchTimeout,
        OpenAIBatchJobException,
    ],
)
def test_wait_for_batch_raise_exception(exception_class):
    mock_hook_instance = Mock()
    mock_hook_instance.wait_for_batch.side_effect = exception_class
    hook = mock_hook_instance
    with pytest.raises(exception_class):
        hook.wait_for_batch(batch_id="batch_id")
