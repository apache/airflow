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
"""Single source of truth for provider module types.

All extraction scripts and frontend data files derive their type
definitions from this module.  To add a new module type, add an
entry to ``MODULE_TYPES`` (and optionally ``BASE_CLASS_IMPORTS``),
then run ``generate_types_json.py`` to propagate to the frontend.
"""

from __future__ import annotations

MODULE_TYPES: dict[str, dict] = {
    "operator": {
        "yaml_key": "operators",
        "level": "module",  # "module" = python-modules list
        "suffixes": ["Operator", "Command"],
        "label": "Operators",
        "icon": "O",
    },
    "hook": {
        "yaml_key": "hooks",
        "level": "module",
        "suffixes": ["Hook"],
        "label": "Hooks",
        "icon": "H",
    },
    "sensor": {
        "yaml_key": "sensors",
        "level": "module",
        "suffixes": ["Sensor"],
        "label": "Sensors",
        "icon": "S",
    },
    "trigger": {
        "yaml_key": "triggers",
        "level": "module",
        "suffixes": ["Trigger"],
        "label": "Triggers",
        "icon": "T",
    },
    "transfer": {
        "yaml_key": "transfers",
        "level": "flat",  # "flat" = count only (len of list)
        "suffixes": ["Operator", "Transfer"],
        "label": "Transfers",
        "icon": "X",
    },
    "bundle": {
        "yaml_key": "bundles",
        "level": "module",
        "suffixes": ["Bundle"],
        "label": "Bundles",
        "icon": "B",
    },
    "notifier": {
        "yaml_key": "notifications",
        "level": "flat",
        "suffixes": [],
        "label": "Notifiers",
        "icon": "N",
    },
    "secret": {
        "yaml_key": "secrets-backends",
        "level": "flat",
        "suffixes": [],
        "label": "Secrets Backend",
        "icon": "K",
    },
    "logging": {
        "yaml_key": "logging",
        "level": "flat",
        "suffixes": [],
        "label": "Log Handler",
        "icon": "L",
    },
    "executor": {
        "yaml_key": "executors",
        "level": "flat",
        "suffixes": [],
        "label": "Executors",
        "icon": "E",
    },
    "decorator": {
        "yaml_key": "task-decorators",
        "level": "flat",
        "suffixes": [],
        "label": "Decorators",
        "icon": "@",
    },
}

# Runtime base class imports for issubclass checks (extract_parameters.py).
# Ordered so more-specific types are checked first (sensor before operator,
# since BaseSensorOperator inherits BaseOperator).
BASE_CLASS_IMPORTS: list[tuple[str, str]] = [
    ("sensor", "airflow.sdk.bases.sensor.BaseSensorOperator"),
    ("trigger", "airflow.triggers.base.BaseTrigger"),
    ("hook", "airflow.sdk.bases.hook.BaseHook"),
    ("bundle", "airflow.dag_processing.bundles.base.BaseDagBundle"),
    ("operator", "airflow.sdk.bases.operator.BaseOperator"),
]

# Derived lookups used by extraction scripts.
# Maps yaml section key -> type id for module-level sections.
MODULE_LEVEL_SECTIONS: dict[str, str] = {
    info["yaml_key"]: type_id for type_id, info in MODULE_TYPES.items() if info["level"] == "module"
}

# Maps yaml section key -> type id for flat-count sections.
FLAT_LEVEL_SECTIONS: dict[str, str] = {
    info["yaml_key"]: type_id for type_id, info in MODULE_TYPES.items() if info["level"] == "flat"
}

# Maps type id -> list of class name suffixes for AST matching.
TYPE_SUFFIXES: dict[str, list[str]] = {type_id: info["suffixes"] for type_id, info in MODULE_TYPES.items()}

# Class-level sections used by extract_parameters.py (subset of flat that
# list full class paths rather than simple entries).
CLASS_LEVEL_SECTIONS: dict[str, str] = {
    "notifications": "notifier",
    "secrets-backends": "secret",
    "logging": "logging",
    "executors": "executor",
}

# All type ids, ordered consistently.
ALL_TYPE_IDS: list[str] = list(MODULE_TYPES.keys())
