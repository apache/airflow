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

import sys


class TestExceptions:
    def setup_method(self):
        self.old_modules = dict(sys.modules)

    def teardown_method(self):
        # Remove any new modules imported during the test run. This lets us
        # import the same source files for more than one test.
        for mod in [m for m in sys.modules if m not in self.old_modules]:
            del sys.modules[mod]

    def test_pod_mutation_hook_exceptions_compatibility(
        self,
    ):
        from airflow.exceptions import (
            PodMutationHookException as CoreMutationHookException,
        )
        from airflow.providers.cncf.kubernetes.exceptions import (
            PodMutationHookException as ProviderMutationHookException,
        )
        from airflow.providers.cncf.kubernetes.pod_generator import (
            PodMutationHookException as ProviderGeneratorMutationHookException,
        )

        assert ProviderMutationHookException == CoreMutationHookException
        assert ProviderMutationHookException == ProviderGeneratorMutationHookException

    def test_pod_reconciliation_error_exceptions_compatibility(
        self,
    ):
        from airflow.exceptions import (
            PodReconciliationError as CoreReconciliationError,
        )
        from airflow.providers.cncf.kubernetes.exceptions import (
            PodReconciliationError as ProviderReconciliationError,
        )
        from airflow.providers.cncf.kubernetes.pod_generator import (
            PodReconciliationError as ProviderGeneratorReconciliationError,
        )

        assert ProviderReconciliationError == CoreReconciliationError
        assert ProviderReconciliationError == ProviderGeneratorReconciliationError
