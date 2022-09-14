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

from airflow_breeze.utils import recording  # isort:skip # noqa

try:
    # We handle ImportError so that click autocomplete works
    import rich_click as click
except ImportError as e:
    if "No module named 'rich_click'" in e.msg:
        # just ignore the import error when rich_click is missing
        import click  # type: ignore[no-redef]
    else:
        raise
else:
    from airflow_breeze.commands.ci_commands_config import CI_COMMANDS, CI_PARAMETERS
    from airflow_breeze.commands.ci_image_commands_config import (
        CI_IMAGE_TOOLS_COMMANDS,
        CI_IMAGE_TOOLS_PARAMETERS,
    )
    from airflow_breeze.commands.developer_commands_config import DEVELOPER_COMMANDS, DEVELOPER_PARAMETERS
    from airflow_breeze.commands.kubernetes_commands_config import (
        KUBERNETES_CLUSTER_COMMANDS,
        KUBERNETES_INSPECTION_COMMANDS,
        KUBERNETES_PARAMETERS,
        KUBERNETES_TESTING_COMMANDS,
    )
    from airflow_breeze.commands.production_image_commands_config import (
        PRODUCTION_IMAGE_TOOLS_COMMANDS,
        PRODUCTION_IMAGE_TOOLS_PARAMETERS,
    )
    from airflow_breeze.commands.release_management_commands_config import (
        RELEASE_MANAGEMENT_COMMANDS,
        RELEASE_MANAGEMENT_PARAMETERS,
    )
    from airflow_breeze.commands.setup_commands_config import SETUP_COMMANDS, SETUP_PARAMETERS
    from airflow_breeze.commands.testing_commands_config import TESTING_COMMANDS, TESTING_PARAMETERS

    click.rich_click.SHOW_METAVARS_COLUMN = False
    click.rich_click.SHOW_ARGUMENTS = False
    click.rich_click.APPEND_METAVARS_HELP = True
    click.rich_click.OPTIONS_PANEL_TITLE = "Common options"
    click.rich_click.STYLE_ERRORS_SUGGESTION = "bright_blue italic"
    click.rich_click.ERRORS_SUGGESTION = "\nTry running the '--help' flag for more information.\n"
    click.rich_click.ERRORS_EPILOGUE = (
        "\nTo find out more, visit [info]https://github.com/apache/airflow/blob/main/BREEZE.rst[/]\n"
    )
    click.rich_click.OPTION_GROUPS = {
        **DEVELOPER_PARAMETERS,
        **KUBERNETES_PARAMETERS,
        **TESTING_PARAMETERS,
        **SETUP_PARAMETERS,
        **CI_IMAGE_TOOLS_PARAMETERS,
        **PRODUCTION_IMAGE_TOOLS_PARAMETERS,
        **CI_PARAMETERS,
        **RELEASE_MANAGEMENT_PARAMETERS,
    }
    click.rich_click.COMMAND_GROUPS = {
        "breeze": [
            DEVELOPER_COMMANDS,
            {
                "name": "Advanced command groups",
                "commands": ["testing", "ci-image", "k8s", "prod-image", "setup", "release-management", "ci"],
            },
        ],
        "breeze testing": [TESTING_COMMANDS],
        "breeze k8s": [
            KUBERNETES_CLUSTER_COMMANDS,
            KUBERNETES_INSPECTION_COMMANDS,
            KUBERNETES_TESTING_COMMANDS,
        ],
        "breeze ci-image": [CI_IMAGE_TOOLS_COMMANDS],
        "breeze prod-image": [PRODUCTION_IMAGE_TOOLS_COMMANDS],
        "setup": [SETUP_COMMANDS],
        "release-management": [RELEASE_MANAGEMENT_COMMANDS],
        "breeze ci": [CI_COMMANDS],
    }
