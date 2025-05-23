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
# fmt: off
"""
Operators.

:sphinx-autoapi-skip:
"""

from __future__ import annotations

from airflow.utils.deprecation_tools import add_deprecated_classes

__deprecated_classes = {
    "python":{
        "PythonOperator": "airflow.providers.standard.operators.python.PythonOperator",
        "BranchPythonOperator": "airflow.providers.standard.operators.python.BranchPythonOperator",
        "ShortCircuitOperator": "airflow.providers.standard.operators.python.ShortCircuitOperator",
        "PythonVirtualenvOperator": "airflow.providers.standard.operators.python.PythonVirtualenvOperator",
        "ExternalPythonOperator": "airflow.providers.standard.operators.python.ExternalPythonOperator",
        "BranchExternalPythonOperator": "airflow.providers.standard.operators.python.BranchExternalPythonOperator",
        "BranchPythonVirtualenvOperator": "airflow.providers.standard.operators.python.BranchPythonVirtualenvOperator",
        "get_current_context": "airflow.sdk.get_current_context",
    },
    "bash":{
        "BashOperator": "airflow.providers.standard.operators.bash.BashOperator",
    },
    "datetime":{
        "BranchDateTimeOperator": "airflow.providers.standard.operators.datetime.BranchDateTimeOperator",
    },
    "generic_transfer": {
        "GenericTransfer": "airflow.providers.common.sql.operators.generic_transfer.GenericTransfer",
    },
    "weekday": {
        "BranchDayOfWeekOperator": "airflow.providers.standard.operators.weekday.BranchDayOfWeekOperator",
    },
    "trigger_dagrun": {
        "TriggerDagRunOperator": "airflow.providers.standard.operators.trigger_dagrun.TriggerDagRunOperator",
    },
    "latest_only": {
        "LatestOnlyOperator": "airflow.providers.standard.operators.latest_only.LatestOnlyOperator",
    },
    "empty": {
        "EmptyOperator": "airflow.providers.standard.operators.empty.EmptyOperator",
    },
    "email": {
        "EmailOperator": "airflow.providers.smtp.operators.smtp.EmailOperator",
    },
    "smooth": {
        "SmoothOperator": "airflow.providers.standard.operators.smooth.SmoothOperator",
    },
    "branch":{
        "BranchMixIn": "airflow.providers.standard.operators.branch.BranchMixIn",
        "BaseBranchOperator": "airflow.providers.standard.operators.branch.BaseBranchOperator",
    }

}
add_deprecated_classes(__deprecated_classes, __name__)
