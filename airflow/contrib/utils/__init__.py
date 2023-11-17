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
"""This package is deprecated. Please use :mod:`airflow.utils`."""
from __future__ import annotations

import warnings

from airflow.exceptions import RemovedInAirflow3Warning
from airflow.utils.deprecation_tools import add_deprecated_classes

warnings.warn(
    "This module is deprecated. Please use `airflow.utils`.",
    RemovedInAirflow3Warning,
    stacklevel=2
)

__deprecated_classes = {
    "gcp_field_sanitizer": {
        "GcpBodyFieldSanitizer": "airflow.providers.google.cloud.utils.field_sanitizer.GcpBodyFieldSanitizer",
        "GcpFieldSanitizerException": (
            "airflow.providers.google.cloud.utils.field_sanitizer.GcpFieldSanitizerException"
        ),
    },
    "gcp_field_validator": {
        "GcpBodyFieldValidator": "airflow.providers.google.cloud.utils.field_validator.GcpBodyFieldValidator",
        "GcpFieldValidationException": (
            "airflow.providers.google.cloud.utils.field_validator.GcpFieldValidationException"
        ),
        "GcpValidationSpecificationException": (
            "airflow.providers.google.cloud.utils.field_validator.GcpValidationSpecificationException"
        ),
    },
    "mlengine_operator_utils": {
        "create_evaluate_ops": (
            "airflow.providers.google.cloud.utils.mlengine_operator_utils.create_evaluate_ops"
        ),
    },
    "mlengine_prediction_summary": {
        "JsonCoder": "airflow.providers.google.cloud.utils.mlengine_prediction_summary.JsonCoder",
        "MakeSummary": "airflow.providers.google.cloud.utils.mlengine_prediction_summary.MakeSummary",
    },
    "sendgrid": {
        "import_string": "airflow.utils.module_loading.import_string",
    },
    "weekday": {
        "WeekDay": "airflow.utils.weekday.WeekDay",
    },
}

add_deprecated_classes(__deprecated_classes, __name__)
