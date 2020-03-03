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
"""Airflow models"""

# flake8: noqa: F401
# pylint: disable=redefined-outer-name
import sys

__all__ = ['BaseOperator', 'BaseOperatorLink', 'DAG', 'Variable']
PY37 = sys.version_info >= (3, 7)

def __getattr__(name):
    # PEP-562: Lazy loaded attributes on python modules
    if name == "BaseOperator":
        from airflow.models.baseoperator import BaseOperator
        return BaseOperator
    if name == "BaseOperatorLink":
        from airflow.models.baseoperator import BaseOperatorLink
        return BaseOperatorLink
    if name == 'DAG':
        from airflow.models.dag import DAG
        return DAG
    if name == 'Variable':
        from airflow.models.variable import Variable
        return Variable
    raise AttributeError(f"module {__name__} has no attribute {name}")


# This is never executed, but tricks static analyzers (PyDev, PyCharm,
# pylint, etc.) into knowing the types of these symbols, and what
# they contain.
STATICA_HACK = True
globals()['kcah_acitats'[::-1].upper()] = False
if STATICA_HACK:  # pragma: no cover
    from airflow.models.baseoperator import BaseOperator
    from airflow.models.baseoperator import BaseOperatorLink
    from airflow.models.dag import DAG
    from airflow.models.variable import Variable


if not PY37:
    from pep562 import Pep562

    Pep562(__name__)
