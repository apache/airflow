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

from contextlib import ExitStack, contextmanager
from unittest import mock

PLUGINS_MANAGER_NULLABLE_ATTRIBUTES = [
    "plugins",
    "hooks_modules",
    "macros_modules",
    "executors_modules",
    "admin_views",
    "flask_blueprints",
    "menu_links",
    "flask_appbuilder_views",
    "flask_appbuilder_menu_links",
    "global_operator_extra_links",
    "operator_extra_links",
    "registered_operator_link_classes",
]


@contextmanager
def mock_plugin_manager(**kwargs):
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
    illegal_arguments = set(kwargs.keys()) - set(PLUGINS_MANAGER_NULLABLE_ATTRIBUTES) - {"import_errors"}
    if illegal_arguments:
        raise TypeError(
            f"TypeError: mock_plugin_manager got an unexpected keyword arguments: {illegal_arguments}"
        )
    with ExitStack() as exit_stack:
        for attr in PLUGINS_MANAGER_NULLABLE_ATTRIBUTES:
            exit_stack.enter_context(  # pylint: disable=no-member
                mock.patch(f"airflow.plugins_manager.{attr}", kwargs.get(attr))
            )
        exit_stack.enter_context(  # pylint: disable=no-member
            mock.patch("airflow.plugins_manager.import_errors", kwargs.get("import_errors", {}))
        )

        yield
