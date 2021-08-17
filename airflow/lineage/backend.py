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
"""Sends lineage metadata to a backend"""
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from airflow.models.baseoperator import BaseOperator


class LineageBackend:
    """Sends lineage metadata to a backend"""

    def send_lineage(
        self,
        operator: 'BaseOperator',
        inlets: Optional[list] = None,
        outlets: Optional[list] = None,
        context: Optional[dict] = None,
    ):
        """
        Sends lineage metadata to a backend

        :param operator: the operator executing a transformation on the inlets and outlets
        :type operator: airflow.models.baseoperator.BaseOperator
        :param inlets: the inlets to this operator
        :type inlets: list
        :param outlets: the outlets from this operator
        :type outlets: list
        :param context: the current context of the task instance
        :type context: dict
        """
        raise NotImplementedError()
