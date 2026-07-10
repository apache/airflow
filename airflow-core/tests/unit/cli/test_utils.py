#
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

from airflow.cli.utils import deprecated_for_airflowctl


class TestDeprecatedForAirflowctl:
    def test_records_replacement_without_emitting_a_user_warning(self):
        @deprecated_for_airflowctl("airflowctl dags trigger")
        def command(args):
            return "result"

        # Calling the command emits nothing to users (any warning would become an error here).
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            result = command(args=None)

        assert result == "result"
        # The replacement is recorded for maintainers, not shown to users.
        assert command._migrated_to_airflowctl == "airflowctl dags trigger"

    def test_passes_through_args_and_leaves_function_untouched(self):
        @deprecated_for_airflowctl("airflowctl pools create")
        def command(a, b, *, c):
            """Original docstring."""
            return (a, b, c)

        assert command(1, 2, c=3) == (1, 2, 3)
        # The decorator returns the original function untouched apart from the metadata it records.
        assert command.__name__ == "command"
        assert command.__doc__ == "Original docstring."
        assert command._migrated_to_airflowctl == "airflowctl pools create"
