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
from airflow_breeze.utils import recording  # isort:skip # noqa

try:
    # We handle ImportError so that click autocomplete works
    import rich_click as click

    from airflow_breeze.commands.ci_commands import CI_COMMANDS, CI_PARAMETERS
    from airflow_breeze.commands.ci_image_commands import CI_IMAGE_TOOLS_COMMANDS, CI_IMAGE_TOOLS_PARAMETERS
    from airflow_breeze.commands.configuration_and_maintenance_commands import (
        CONFIGURATION_AND_MAINTENANCE_COMMANDS,
        CONFIGURATION_AND_MAINTENANCE_PARAMETERS,
    )
    from airflow_breeze.commands.developer_commands import DEVELOPER_COMMANDS, DEVELOPER_PARAMETERS
    from airflow_breeze.commands.production_image_commands import (
        PRODUCTION_IMAGE_TOOLS_COMMANDS,
        PRODUCTION_IMAGE_TOOLS_PARAMETERS,
    )
    from airflow_breeze.commands.release_management_commands import (
        RELEASE_MANAGEMENT_COMMANDS,
        RELEASE_MANAGEMENT_PARAMETERS,
    )
    from airflow_breeze.commands.testing_commands import TESTING_COMMANDS, TESTING_PARAMETERS

    click.rich_click.SHOW_METAVARS_COLUMN = False
    click.rich_click.SHOW_ARGUMENTS = False
    click.rich_click.APPEND_METAVARS_HELP = True
    click.rich_click.STYLE_ERRORS_SUGGESTION = "bright_blue italic"
    click.rich_click.ERRORS_SUGGESTION = "\nTry running the '--help' flag for more information.\n"
    click.rich_click.ERRORS_EPILOGUE = (
        "\nTo find out more, visit [info]https://github.com/apache/airflow/blob/main/BREEZE.rst[/]\n"
    )
    click.rich_click.OPTION_GROUPS = {
        **DEVELOPER_PARAMETERS,
        **TESTING_PARAMETERS,
        **CONFIGURATION_AND_MAINTENANCE_PARAMETERS,
        **CI_IMAGE_TOOLS_PARAMETERS,
        **PRODUCTION_IMAGE_TOOLS_PARAMETERS,
        **CI_PARAMETERS,
        **RELEASE_MANAGEMENT_PARAMETERS,
    }
    click.rich_click.COMMAND_GROUPS = {
        "breeze": [
            DEVELOPER_COMMANDS,
            TESTING_COMMANDS,
            CONFIGURATION_AND_MAINTENANCE_COMMANDS,
            CI_IMAGE_TOOLS_COMMANDS,
            PRODUCTION_IMAGE_TOOLS_COMMANDS,
            CI_COMMANDS,
            RELEASE_MANAGEMENT_COMMANDS,
        ]
    }
except ImportError as e:
    if "No module named 'rich_click'" in e.msg:
        # just ignore the import error when rich_click is missing
        import click  # type: ignore[no-redef]
    else:
        raise
