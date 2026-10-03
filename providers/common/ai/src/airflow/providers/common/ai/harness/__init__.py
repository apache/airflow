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
"""
Run a vendor's own agent loop in-process, with Airflow giving it tools, credentials and a result.

Unlike :mod:`airflow.providers.common.ai.tools`, where the Dag author's own agent
code drives the run, a harness backend owns the whole run:
:class:`~airflow.providers.common.ai.operators.harness.HarnessOperator` calls
:meth:`~airflow.providers.common.ai.harness.base.HarnessBackend.run` once and gets
back a final result.

.. note::

    Experimental: this interface can change or be removed in a minor release of this
    provider.
    See :ref:`howto/stability`.
"""

from __future__ import annotations
