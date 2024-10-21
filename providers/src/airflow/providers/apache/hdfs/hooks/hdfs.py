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

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

_EXCEPTION_MESSAGE = """The old HDFS Hooks have been removed in 4.0.0 version of the apache.hdfs provider.
Please convert your DAGs to use the WebHdfsHook or downgrade the provider to below 4.*
if you want to continue using it.
If you want to use earlier provider you can downgrade to latest released 3.* version
using `pip install apache-airflow-providers-apache-hdfs==3.2.1` (no constraints)
"""


class HDFSHookException(AirflowException):
    """
    This Exception has been removed and is not functional.

    Please convert your DAGs to use the WebHdfsHook or downgrade the provider
    to below 4.* if you want to continue using it. If you want to use earlier
    provider you can downgrade to latest released 3.* version using
    `pip install apache-airflow-providers-apache-hdfs==3.2.1` (no constraints).
    """

    def __init__(self, *args, **kwargs):
        raise RuntimeError(_EXCEPTION_MESSAGE)


class HDFSHook(BaseHook):
    """
    This Hook has been removed and is not functional.

    Please convert your DAGs to use the WebHdfsHook or downgrade the provider
    to below 4.*. if you want to continue using it. If you want to use earlier
    provider you can downgrade to latest released 3.* version using
    `pip install apache-airflow-providers-apache-hdfs==3.2.1` (no constraints).
    """

    def __init__(self, *args, **kwargs):
        raise RuntimeError(_EXCEPTION_MESSAGE)
