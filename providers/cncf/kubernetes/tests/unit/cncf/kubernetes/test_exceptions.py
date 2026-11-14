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


class TestPodMutationHookException:
    def test_is_airflow_exception(self):
        assert issubclass(PodMutationHookException, AirflowException)

    def test_can_be_raised_and_caught_as_airflow_exception(self):
        with pytest.raises(AirflowException):
            raise PodMutationHookException("hook failed")

    def test_message_is_preserved(self):
        exc = PodMutationHookException("something went wrong in the hook")
        assert "something went wrong in the hook" in str(exc)

    def test_can_be_raised_with_no_message(self):
        with pytest.raises(PodMutationHookException):
            raise PodMutationHookException


class TestPodReconciliationError:
    def test_is_airflow_exception(self):
        assert issubclass(PodReconciliationError, AirflowException)

    def test_can_be_raised_and_caught_as_airflow_exception(self):
        with pytest.raises(AirflowException):
            raise PodReconciliationError("reconciliation failed")

    def test_message_is_preserved(self):
        exc = PodReconciliationError("failed to merge pod configs")
        assert "failed to merge pod configs" in str(exc)

    def test_can_be_raised_with_no_message(self):
        with pytest.raises(PodReconciliationError):
            raise PodReconciliationError


class TestKubernetesApiError:
    def test_is_airflow_exception(self):
        assert issubclass(KubernetesApiError, AirflowException)

    def test_can_be_raised_and_caught_as_airflow_exception(self):
        with pytest.raises(AirflowException):
            raise KubernetesApiError("api call failed")

    def test_message_is_preserved(self):
        exc = KubernetesApiError("could not reach k8s api")
        assert "could not reach k8s api" in str(exc)

    def test_can_be_raised_with_no_message(self):
        with pytest.raises(KubernetesApiError):
            raise KubernetesApiError


class TestKubernetesApiPermissionError:
    def test_is_airflow_exception(self):
        assert issubclass(KubernetesApiPermissionError, AirflowException)

    def test_can_be_raised_and_caught_as_airflow_exception(self):
        with pytest.raises(AirflowException):
            raise KubernetesApiPermissionError("permission denied")

    def test_message_is_preserved(self):
        exc = KubernetesApiPermissionError("forbidden: insufficient permissions")
        assert "forbidden: insufficient permissions" in str(exc)

    def test_can_be_raised_with_no_message(self):
        with pytest.raises(KubernetesApiPermissionError):
            raise KubernetesApiPermissionError


class TestExceptionHierarchy:
    @pytest.mark.parametrize(
        "exc_class",
        [
            PodMutationHookException,
            PodReconciliationError,
            KubernetesApiError,
            KubernetesApiPermissionError,
        ],
    )
    def test_all_exceptions_are_distinct_types(self, exc_class):
        """Each exception is its own class, not an alias."""
        others = {
            PodMutationHookException,
            PodReconciliationError,
            KubernetesApiError,
            KubernetesApiPermissionError,
        } - {exc_class}
        for other in others:
            assert exc_class is not other

    @pytest.mark.parametrize(
        "exc_class",
        [
            PodMutationHookException,
            PodReconciliationError,
            KubernetesApiError,
            KubernetesApiPermissionError,
        ],
    )
    def test_catching_as_specific_type_works(self, exc_class):
        with pytest.raises(exc_class):
            raise exc_class("test message")
