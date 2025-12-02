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

from airflow_breeze.commands.sbom_commands_config import SBOM_COMMANDS, SBOM_PARAMETERS
from airflow_breeze.commands.ui_commands_config import UI_COMMANDS, UI_PARAMETERS
from airflow_breeze.commands.workflow_commands_config import WORKFLOW_RUN_COMMANDS, WORKFLOW_RUN_PARAMETERS

from airflow_breeze.utils import recording  # isort:skip  # noqa: F401

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
        KUBERNETES_DEVELOPMENT_COMMANDS,
        KUBERNETES_INSPECTION_COMMANDS,
        KUBERNETES_PARAMETERS,
        KUBERNETES_TESTING_COMMANDS,
    )
    from airflow_breeze.commands.production_image_commands_config import (
        PRODUCTION_IMAGE_TOOLS_COMMANDS,
        PRODUCTION_IMAGE_TOOLS_PARAMETERS,
    )
    from airflow_breeze.commands.release_management_commands_config import (
        RELEASE_AIRFLOW_COMMANDS,
        RELEASE_AIRFLOW_CTL_COMMANDS,
        RELEASE_AIRFLOW_TASK_SDK_COMMANDS,
        RELEASE_HELM_COMMANDS,
        RELEASE_MANAGEMENT_PARAMETERS,
        RELEASE_OTHER_COMMANDS,
        RELEASE_PROVIDERS_COMMANDS,
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
        "\nTo find out more, visit [info]https://github.com/apache/airflow/"
        "blob/main/dev/breeze/doc/01_installation.rst[/]\n"
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
        **SBOM_PARAMETERS,
        **WORKFLOW_RUN_PARAMETERS,
        **UI_PARAMETERS,
    }
    click.rich_click.COMMAND_GROUPS = {
        "breeze": [
            DEVELOPER_COMMANDS,
            {
                "name": "Testing commands",
                "commands": ["testing", "k8s"],
            },
            {
                "name": "Image commands",
                "commands": ["ci-image", "prod-image"],
            },
            {
                "name": "Release management commands",
                "commands": ["release-management", "sbom", "workflow-run"],
            },
            {
                "name": "CI commands",
                "commands": ["ci"],
            },
            {
                "name": "UI commands",
                "commands": ["ui"],
            },
            {
                "name": "Setup commands",
                "commands": ["setup"],
            },
        ],
        "breeze testing": TESTING_COMMANDS,
        "breeze k8s": [
            KUBERNETES_CLUSTER_COMMANDS,
            KUBERNETES_DEVELOPMENT_COMMANDS,
            KUBERNETES_INSPECTION_COMMANDS,
            KUBERNETES_TESTING_COMMANDS,
        ],
        "breeze ci-image": [CI_IMAGE_TOOLS_COMMANDS],
        "breeze prod-image": [PRODUCTION_IMAGE_TOOLS_COMMANDS],
        "breeze setup": [SETUP_COMMANDS],
        "breeze release-management": [
            RELEASE_AIRFLOW_COMMANDS,
            RELEASE_HELM_COMMANDS,
            RELEASE_PROVIDERS_COMMANDS,
            RELEASE_AIRFLOW_TASK_SDK_COMMANDS,
            RELEASE_AIRFLOW_CTL_COMMANDS,
            RELEASE_OTHER_COMMANDS,
        ],
        "breeze sbom": [SBOM_COMMANDS],
        "breeze ci": [CI_COMMANDS],
        "breeze workflow-run": [WORKFLOW_RUN_COMMANDS],
        "breeze ui": [UI_COMMANDS],
    }
