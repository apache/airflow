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

from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.exceptions import (
    KubernetesApiError,
    KubernetesApiPermissionError,
    PodMutationHookException,
    PodReconciliationError,
)


class TestKubernetesExceptions:
    @pytest.mark.parametrize(
        "exc_class",
        [
            PodMutationHookException,
            PodReconciliationError,
            KubernetesApiError,
            KubernetesApiPermissionError,
        ],
    )
    def test_inherits_from_airflow_exception(self, exc_class: type[AirflowException]):
        assert issubclass(exc_class, AirflowException)
        assert issubclass(exc_class, Exception)

    @pytest.mark.parametrize(
        "exc_class, message",
        [
            (PodMutationHookException, "Failed to mutate pod spec during execution"),
            (PodReconciliationError, "Unable to reconcile conflicting pod configurations"),
            (KubernetesApiError, "404 Not Found connecting to Kubernetes API"),
            (KubernetesApiPermissionError, "Forbidden: Service account lacks pods/create RBAC"),
        ],
    )
    def test_exception_instantiation_and_message(
        self, exc_class: type[AirflowException], message: str
    ):
        exc = exc_class(message)
        assert str(exc) == message
        assert exc.args == (message,)

    @pytest.mark.parametrize(
        "exc_class",
        [
            PodMutationHookException,
            PodReconciliationError,
            KubernetesApiError,
            KubernetesApiPermissionError,
        ],
    )
    def test_catching_as_airflow_exception(self, exc_class: type[AirflowException]):
        msg = f"Testing catch of {exc_class.__name__}"
        with pytest.raises(AirflowException) as exc_info:
            raise exc_class(msg)

        assert isinstance(exc_info.value, exc_class)
        assert str(exc_info.value) == msg
