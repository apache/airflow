 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Command Line Interface
----------------------

.. important::
   The Airflow Core version must be ``3.2.0`` or newer to be able to use provider-level CLI commands.


If your provider include :doc:`/core-extensions/auth-managers` or :doc:`/core-extensions/executors`, you should also implement the provider-level CLI commands
to improve the Airflow CLI response speed and avoid loading heavy dependencies when those commands are not needed.

Even if your auth manager or executor do not implement the ``get_cli_commands`` interface (:meth:`airflow.api_fastapi.auth.managers.base_auth_manager.BaseAuthManager.get_cli_commands` or :meth:`airflow.executors.base_executor.BaseExecutor.get_cli_commands`), you should still implement provider-level CLI commands that return empty list to avoid loading auth manager or executor code for every CLI command, which will also resolve the following warning:

.. code-block:: console

    Please define the 'cli' section in the provider.yaml for custom auth managers to avoid this warning.
    For community providers, please update to the version that support '.cli.definition.get_cli_commands' function.
    For more details, see https://airflow.apache.org/docs/apache-airflow-providers/core-extensions/cli-commands.html

    Please define the 'cli' section in the provider.yaml for custom executors to avoid this warning.
    For community providers, please update to the version that support '.cli.definition.get_cli_commands' function.
    For more details, see https://airflow.apache.org/docs/apache-airflow-providers/core-extensions/cli-commands.html


Implementing provider-level CLI Commands
========================================

To implement provider-level CLI commands, follow these steps:

1. Define all your CLI commands in ``airflow.providers.<your_provider_name>.cli.definition`` module. Additionally, you **should avoid defining heavy dependencies in this module** to reduce the Airflow CLI startup time. Please use ``airflow.cli.cli_config.lazy_load_command`` utility to lazily load the actual callable to run.

.. code-block:: python

    from airflow.cli.cli_config import (
        ActionCommand,
        Arg,
        GroupCommand,
        lazy_load_command,
    )


    @staticmethod
    def get_my_cli_commands() -> list[GroupCommand]:
        executor_sub_commands = [
            ActionCommand(
                name="executor_subcommand_name",
                help="Description of what this specific command does",
                func=lazy_load_command("path.to.python.function.for.command"),
                args=Arg(
                    "--my-arg",
                    help="Description of my arg",
                    action="store_true",
                ),
            ),
        ]
        auth_manager_sub_commands = [
            ActionCommand(
                name="auth_manager_subcommand_name",
                help="Description of what this specific command does",
                func=lazy_load_command("path.to.python.function.for.command"),
                args=(),
            ),
        ]
        custom_sub_commands = [
            ActionCommand(
                name="custom_subcommand_name",
                help="Description of what this specific command does",
                func=lazy_load_command("path.to.python.function.for.command"),
                args=(),
            ),
        ]

        return [
            GroupCommand(
                name="my_cool_executor",
                help="Description of what this group of commands do",
                subcommands=executor_sub_commands,
            ),
            GroupCommand(
                name="my_cool_auth_manager",
                help="Description of what this group of commands do",
                subcommands=auth_manager_sub_commands,
            ),
            GroupCommand(
                name="my_cool_custom_commands",
                help="Description of what this group of commands do",
                subcommands=custom_sub_commands,
            ),
        ]

2. Update ``cli`` section of your provider's ``provider.yaml`` file to point to the function that
   returns the list of CLI commands. For example:


.. code-block:: yaml

    cli:
    - airflow.providers.<your_provider_name>.cli.definition.get_my_cli_commands

3. Update ``get_provider_info.py`` file of your provider to include the CLI commands in the
   returned dictionary. For example:

.. code-block:: python

    def get_provider_info() -> dict[str, list[str]]:
        return {
            # ...
            "cli": ["airflow.providers.<your_provider_name>.cli.definition.get_my_cli_commands"],
            # ...
        }

You can read more about ``provider.yaml`` and ``get_provider_info.py`` in :doc:`/howto/create-custom-providers`.

Community-Managed Provider CLI Commands
=======================================

This is a summary of all Apache Airflow Community provided implementations of CLI commands
exposed via community-managed providers.

.. note::
    For example, if you are using :doc:`KubernetesExecutor <apache-airflow-providers-cncf-kubernetes:kubernetes_executor>` and you encounter the ``Please define the 'cli' section in the provider.yaml for custom executors to avoid this warning.`` warning during CLI usage, ensure that you have updated to a version of the provider that includes the necessary CLI command definitions as described below.

Those provided by the community-managed providers:

.. airflow-cli-commands::
   :tags: None
   :header-separator: "
