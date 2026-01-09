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

from contextlib import ExitStack, contextmanager
from unittest import mock

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_2_PLUS

PLUGINS_MANAGER_NULLABLE_ATTRIBUTES_V3_0 = [
    "plugins",
    "macros_modules",
    "admin_views",
    "flask_blueprints",
    "fastapi_apps",
    "fastapi_root_middlewares",
    "external_views",
    "react_apps",
    "menu_links",
    "flask_appbuilder_views",
    "flask_appbuilder_menu_links",
    "global_operator_extra_links",
    "operator_extra_links",
    "registered_operator_link_classes",
    "timetable_classes",
    "hook_lineage_reader_classes",
]


PLUGINS_MANAGER_NULLABLE_ATTRIBUTES_V2_10 = [
    "plugins",
    "macros_modules",
    "admin_views",
    "flask_blueprints",
    "menu_links",
    "flask_appbuilder_views",
    "flask_appbuilder_menu_links",
    "global_operator_extra_links",
    "operator_extra_links",
    "registered_operator_link_classes",
    "timetable_classes",
    "hook_lineage_reader_classes",
]


@contextmanager
def mock_plugin_manager(plugins=None, **kwargs):
    """
    Protects the initial state and sets the default state for the airflow.plugins module.

    You can also overwrite variables by passing a keyword argument.

    airflow.plugins_manager uses many global variables. To avoid side effects, this decorator performs
    the following operations:

    1. saves variables state,
    2. set variables to default value,
    3. executes context code,
    4. restores the state of variables to the state from point 1.

    Use this context if you want your test to not have side effects in airflow.plugins_manager, and
    other tests do not affect the results of this test.
    """
    illegal_arguments = set(kwargs.keys()) - set(PLUGINS_MANAGER_NULLABLE_ATTRIBUTES_V3_0) - {"import_errors"}
    if illegal_arguments:
        raise TypeError(
            f"TypeError: mock_plugin_manager got an unexpected keyword arguments: {illegal_arguments}"
        )
    # Handle plugins specially
    with ExitStack() as exit_stack:
        if AIRFLOW_V_3_2_PLUS:
            # Always start the block with an non-initialized plugins, so ensure_plugins_loaded runs.
            from airflow import plugins_manager

            plugins_manager._get_plugins.cache_clear()
            plugins_manager._get_ui_plugins.cache_clear()
            plugins_manager.get_flask_plugins.cache_clear()
            plugins_manager.get_fastapi_plugins.cache_clear()
            plugins_manager._get_extra_operators_links_plugins.cache_clear()
            plugins_manager.get_timetables_plugins.cache_clear()
            plugins_manager.get_hook_lineage_readers_plugins.cache_clear()
            plugins_manager.integrate_macros_plugins.cache_clear()
            plugins_manager.get_priority_weight_strategy_plugins.cache_clear()

            if plugins is not None or "import_errors" in kwargs:
                exit_stack.enter_context(
                    mock.patch(
                        "airflow.plugins_manager._get_plugins",
                        return_value=(
                            plugins or [],
                            kwargs.get("import_errors", {}),
                        ),
                    )
                )
            elif kwargs:
                raise NotImplementedError(
                    "mock_plugin_manager does not support patching other attributes in Airflow 3.2+"
                )
        else:

            def mock_loaded_plugins():
                exit_stack.enter_context(mock.patch("airflow.plugins_manager.plugins", plugins or []))

            exit_stack.enter_context(
                mock.patch(
                    "airflow.plugins_manager.load_plugins_from_plugin_directory",
                    side_effect=mock_loaded_plugins,
                )
            )
            exit_stack.enter_context(
                mock.patch("airflow.plugins_manager.load_providers_plugins", side_effect=mock_loaded_plugins)
            )
            exit_stack.enter_context(
                mock.patch("airflow.plugins_manager.load_entrypoint_plugins", side_effect=mock_loaded_plugins)
            )

            if AIRFLOW_V_3_0_PLUS:
                ATTR_TO_PATCH = PLUGINS_MANAGER_NULLABLE_ATTRIBUTES_V3_0
            else:
                ATTR_TO_PATCH = PLUGINS_MANAGER_NULLABLE_ATTRIBUTES_V2_10

            for attr in ATTR_TO_PATCH:
                exit_stack.enter_context(mock.patch(f"airflow.plugins_manager.{attr}", kwargs.get(attr)))

            # Always start the block with an non-initialized plugins, so ensure_plugins_loaded runs.
            exit_stack.enter_context(mock.patch("airflow.plugins_manager.plugins", None))
            exit_stack.enter_context(
                mock.patch("airflow.plugins_manager.import_errors", kwargs.get("import_errors", {}))
            )

        yield
