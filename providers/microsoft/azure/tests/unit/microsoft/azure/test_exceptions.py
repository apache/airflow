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

import pytest

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.microsoft.azure.exceptions import (
    AzureBatchAutoScaleFormulaMissingError,
    AzureBatchJobPreparationTaskMissingError,
    AzureBatchLatestImageSpecIncompleteError,
    AzureBatchPoolSizingConflictError,
    AzureBatchPoolSizingMissingError,
    AzureBatchRequiredParametersMissingError,
    AzureBatchVmImageSpecIncompleteError,
    AzureBatchVmPublisherMissingError,
)

ALL_EXCEPTIONS = [
    AzureBatchAutoScaleFormulaMissingError,
    AzureBatchJobPreparationTaskMissingError,
    AzureBatchLatestImageSpecIncompleteError,
    AzureBatchPoolSizingConflictError,
    AzureBatchPoolSizingMissingError,
    AzureBatchRequiredParametersMissingError,
    AzureBatchVmImageSpecIncompleteError,
    AzureBatchVmPublisherMissingError,
]


@pytest.mark.parametrize("exception_class", ALL_EXCEPTIONS)
def test_inherits_from_airflow_exception(exception_class):
    assert issubclass(exception_class, AirflowException)


@pytest.mark.parametrize("exception_class", ALL_EXCEPTIONS)
def test_can_be_raised_with_message(exception_class):
    with pytest.raises(exception_class, match="boom"):
        raise exception_class("boom")


@pytest.mark.parametrize("exception_class", ALL_EXCEPTIONS)
def test_caught_as_airflow_exception(exception_class):
    with pytest.raises(AirflowException):
        raise exception_class("boom")
