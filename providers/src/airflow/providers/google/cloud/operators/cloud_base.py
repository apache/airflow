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
"""This module contains a Google API base operator."""

from __future__ import annotations

from google.api_core.gapic_v1.method import DEFAULT

from airflow.models import BaseOperator


class GoogleCloudBaseOperator(BaseOperator):
    """Abstract base class for operators using Google API client libraries."""

    def __deepcopy__(self, memo):
        """
        Update the memo to fix the non-copyable global constant.

        This constant can be specified in operator parameters as a retry configuration to indicate a default.
        See https://github.com/apache/airflow/issues/28751 for details.
        """
        memo[id(DEFAULT)] = DEFAULT
        return super().__deepcopy__(memo)
