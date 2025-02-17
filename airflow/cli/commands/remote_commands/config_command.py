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
"""Config sub-commands."""

from __future__ import annotations

from dataclasses import dataclass
from io import StringIO
from typing import NamedTuple

import pygments
from pygments.lexers.configs import IniLexer

from airflow.cli.simple_table import AirflowConsole
from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.utils.cli import should_use_colors
from airflow.utils.code_utils import get_terminal_formatter
from airflow.utils.providers_configuration_loader import providers_configuration_loaded


@providers_configuration_loaded
def show_config(args):
    """Show current application configuration."""
    with StringIO() as output:
        conf.write(
            output,
            section=args.section,
            include_examples=args.include_examples or args.defaults,
            include_descriptions=args.include_descriptions or args.defaults,
            include_sources=args.include_sources and not args.defaults,
            include_env_vars=args.include_env_vars or args.defaults,
            include_providers=not args.exclude_providers,
            comment_out_everything=args.comment_out_everything or args.defaults,
            only_defaults=args.defaults,
        )
        code = output.getvalue()
    if should_use_colors(args):
        code = pygments.highlight(code=code, formatter=get_terminal_formatter(), lexer=IniLexer())
    print(code)


@providers_configuration_loaded
def get_value(args):
    """Get one value from configuration."""
    # while this will make get_value quite a bit slower we must initialize configuration
    # for providers because we do not know what sections and options will be available after
    # providers are initialized. Theoretically Providers might add new sections and options
    # but also override defaults for existing options, so without loading all providers we
    # cannot be sure what is the final value of the option.
    try:
        value = conf.get(args.section, args.option)
        print(value)
    except AirflowConfigException:
        pass


class ConfigParameter(NamedTuple):
    """Represents a configuration parameter."""

    section: str
    option: str


@dataclass
class ConfigChange:
    """
    Class representing the configuration changes in Airflow 3.0.

    :param config: The configuration parameter being changed.
    :param suggestion: A suggestion for replacing or handling the removed configuration.
    :param renamed_to: The new section and option if the configuration is renamed.
    :param was_deprecated: If the config is removed, whether the old config was deprecated.
    """

    config: ConfigParameter
    suggestion: str = ""
    renamed_to: ConfigParameter | None = None
    was_deprecated: bool = True

    @property
    def message(self) -> str:
        """Generate a message for this configuration change."""
        if self.renamed_to:
            if self.config.section != self.renamed_to.section:
                return (
                    f"`{self.config.option}` configuration parameter moved from `{self.config.section}` section to "
                    f"`{self.renamed_to.section}` section as `{self.renamed_to.option}`."
                )
            return (
                f"`{self.config.option}` configuration parameter renamed to `{self.renamed_to.option}` "
                f"in the `{self.config.section}` section."
            )
        return (
            f"Removed{' deprecated' if self.was_deprecated else ''} `{self.config.option}` configuration parameter "
            f"from `{self.config.section}` section. "
            f"{self.suggestion}"
        )


CONFIGS_CHANGES = [
    # admin
    ConfigChange(
        config=ConfigParameter("admin", "hide_sensitive_variable_fields"),
        renamed_to=ConfigParameter("core", "hide_sensitive_var_conn_fields"),
    ),
    ConfigChange(
        config=ConfigParameter("admin", "sensitive_variable_fields"),
        renamed_to=ConfigParameter("core", "sensitive_var_conn_names"),
    ),
    # core
    ConfigChange(
        config=ConfigParameter("core", "check_slas"),
        suggestion="The SLA feature is removed in Airflow 3.0, to be replaced with Airflow Alerts in future",
    ),
    ConfigChange(
        config=ConfigParameter("core", "strict_dataset_uri_validation"),
        suggestion="Dataset URI with a defined scheme will now always be validated strictly, "
        "raising a hard error on validation failure.",
    ),
    ConfigChange(
        config=ConfigParameter("core", "dataset_manager_class"),
        renamed_to=ConfigParameter("core", "asset_manager_class"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "dataset_manager_kwargs"),
        renamed_to=ConfigParameter("core", "asset_manager_kwargs"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "worker_precheck"),
        renamed_to=ConfigParameter("celery", "worker_precheck"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "non_pooled_task_slot_count"),
        renamed_to=ConfigParameter("core", "default_pool_task_slot_count"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "dag_concurrency"),
        renamed_to=ConfigParameter("core", "max_active_tasks_per_dag"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "sql_alchemy_conn"),
        renamed_to=ConfigParameter("database", "sql_alchemy_conn"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "sql_engine_encoding"),
        renamed_to=ConfigParameter("database", "sql_engine_encoding"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "sql_engine_collation_for_ids"),
        renamed_to=ConfigParameter("database", "sql_engine_collation_for_ids"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "sql_alchemy_pool_enabled"),
        renamed_to=ConfigParameter("database", "sql_alchemy_pool_enabled"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "sql_alchemy_pool_size"),
        renamed_to=ConfigParameter("database", "sql_alchemy_pool_size"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "sql_alchemy_max_overflow"),
        renamed_to=ConfigParameter("database", "sql_alchemy_max_overflow"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "sql_alchemy_pool_recycle"),
        renamed_to=ConfigParameter("database", "sql_alchemy_pool_recycle"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "sql_alchemy_pool_pre_ping"),
        renamed_to=ConfigParameter("database", "sql_alchemy_pool_pre_ping"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "sql_alchemy_schema"),
        renamed_to=ConfigParameter("database", "sql_alchemy_schema"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "sql_alchemy_connect_args"),
        renamed_to=ConfigParameter("database", "sql_alchemy_connect_args"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "load_default_connections"),
        renamed_to=ConfigParameter("database", "load_default_connections"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "max_db_retries"),
        renamed_to=ConfigParameter("database", "max_db_retries"),
    ),
    ConfigChange(config=ConfigParameter("core", "task_runner")),
    ConfigChange(config=ConfigParameter("core", "enable_xcom_pickling")),
    ConfigChange(
        config=ConfigParameter("core", "dag_file_processor_timeout"),
        renamed_to=ConfigParameter("dag_processor", "dag_file_processor_timeout"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "dag_processor_manager_log_location"),
    ),
    ConfigChange(
        config=ConfigParameter("core", "log_processor_filename_template"),
    ),
    # api
    ConfigChange(
        config=ConfigParameter("api", "access_control_allow_origin"),
        renamed_to=ConfigParameter("api", "access_control_allow_origins"),
    ),
    ConfigChange(
        config=ConfigParameter("api", "auth_backend"),
        renamed_to=ConfigParameter("api", "auth_backends"),
    ),
    # logging
    ConfigChange(
        config=ConfigParameter("logging", "enable_task_context_logger"),
        suggestion="Remove TaskContextLogger: Replaced by the Log table for better handling of task log "
        "messages outside the execution context.",
    ),
    ConfigChange(
        config=ConfigParameter("logging", "dag_processor_manager_log_location"),
        was_deprecated=False,
    ),
    ConfigChange(
        config=ConfigParameter("logging", "dag_processor_manager_log_stdout"),
        was_deprecated=False,
    ),
    ConfigChange(
        config=ConfigParameter("logging", "log_processor_filename_template"),
        was_deprecated=False,
    ),
    # metrics
    ConfigChange(
        config=ConfigParameter("metrics", "metrics_use_pattern_match"),
    ),
    ConfigChange(
        config=ConfigParameter("metrics", "timer_unit_consistency"),
        suggestion="In Airflow 3.0, the `timer_unit_consistency` setting in the `metrics` section is "
        "removed as it is now the default behaviour. This is done to standardize all timer and "
        "timing metrics to milliseconds across all metric loggers",
    ),
    ConfigChange(
        config=ConfigParameter("metrics", "statsd_allow_list"),
        renamed_to=ConfigParameter("metrics", "metrics_allow_list"),
    ),
    ConfigChange(
        config=ConfigParameter("metrics", "statsd_block_list"),
        renamed_to=ConfigParameter("metrics", "metrics_block_list"),
    ),
    # traces
    ConfigChange(
        config=ConfigParameter("traces", "otel_task_log_event"),
    ),
    # operators
    ConfigChange(
        config=ConfigParameter("operators", "allow_illegal_arguments"),
    ),
    # webserver
    ConfigChange(
        config=ConfigParameter("webserver", "allow_raw_html_descriptions"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "cookie_samesite"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "update_fab_perms"),
        renamed_to=ConfigParameter("fab", "update_fab_perms"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "auth_rate_limited"),
        renamed_to=ConfigParameter("fab", "auth_rate_limited"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", option="auth_rate_limit"),
        renamed_to=ConfigParameter("fab", "auth_rate_limit"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "session_lifetime_days"),
        renamed_to=ConfigParameter("webserver", "session_lifetime_minutes"),
    ),
    ConfigChange(
        config=ConfigParameter("webserver", "force_log_out_after"),
        renamed_to=ConfigParameter("webserver", "session_lifetime_minutes"),
    ),
    # policy
    ConfigChange(
        config=ConfigParameter("policy", "airflow_local_settings"),
        renamed_to=ConfigParameter("policy", "task_policy"),
    ),
    # scheduler
    ConfigChange(
        config=ConfigParameter("scheduler", "dependency_detector"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "allow_trigger_in_future"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "processor_poll_interval"),
        renamed_to=ConfigParameter("scheduler", "scheduler_idle_sleep_time"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "deactivate_stale_dags_interval"),
        renamed_to=ConfigParameter("scheduler", "parsing_cleanup_interval"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "statsd_on"), renamed_to=ConfigParameter("metrics", "statsd_on")
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "max_threads"),
        renamed_to=ConfigParameter("dag_processor", "parsing_processes"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "statsd_host"),
        renamed_to=ConfigParameter("metrics", "statsd_host"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "statsd_port"),
        renamed_to=ConfigParameter("metrics", "statsd_port"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "statsd_prefix"),
        renamed_to=ConfigParameter("metrics", "statsd_prefix"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "statsd_allow_list"),
        renamed_to=ConfigParameter("metrics", "statsd_allow_list"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "stat_name_handler"),
        renamed_to=ConfigParameter("metrics", "stat_name_handler"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "statsd_datadog_enabled"),
        renamed_to=ConfigParameter("metrics", "statsd_datadog_enabled"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "statsd_datadog_tags"),
        renamed_to=ConfigParameter("metrics", "statsd_datadog_tags"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "statsd_datadog_metrics_tags"),
        renamed_to=ConfigParameter("metrics", "statsd_datadog_metrics_tags"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "statsd_custom_client_path"),
        renamed_to=ConfigParameter("metrics", "statsd_custom_client_path"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "parsing_processes"),
        renamed_to=ConfigParameter("dag_processor", "parsing_processes"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "file_parsing_sort_mode"),
        renamed_to=ConfigParameter("dag_processor", "file_parsing_sort_mode"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "max_callbacks_per_loop"),
        renamed_to=ConfigParameter("dag_processor", "max_callbacks_per_loop"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "min_file_process_interval"),
        renamed_to=ConfigParameter("dag_processor", "min_file_process_interval"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "stale_dag_threshold"),
        renamed_to=ConfigParameter("dag_processor", "stale_dag_threshold"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "print_stats_interval"),
        renamed_to=ConfigParameter("dag_processor", "print_stats_interval"),
    ),
    ConfigChange(
        config=ConfigParameter("scheduler", "dag_dir_list_interval"),
        renamed_to=ConfigParameter("dag_processor", "refresh_interval"),
    ),
    # celery
    ConfigChange(
        config=ConfigParameter("celery", "stalled_task_timeout"),
        renamed_to=ConfigParameter("scheduler", "task_queued_timeout"),
    ),
    ConfigChange(
        config=ConfigParameter("celery", "default_queue"),
        renamed_to=ConfigParameter("operators", "default_queue"),
    ),
    ConfigChange(
        config=ConfigParameter("celery", "task_adoption_timeout"),
        renamed_to=ConfigParameter("scheduler", "task_queued_timeout"),
    ),
    # kubernetes_executor
    ConfigChange(
        config=ConfigParameter("kubernetes_executor", "worker_pods_pending_timeout"),
        renamed_to=ConfigParameter("scheduler", "task_queued_timeout"),
    ),
    ConfigChange(
        config=ConfigParameter("kubernetes_executor", "worker_pods_pending_timeout_check_interval"),
        renamed_to=ConfigParameter("scheduler", "task_queued_timeout_check_interval"),
    ),
    # smtp
    ConfigChange(
        config=ConfigParameter("smtp", "smtp_user"),
        suggestion="Please use the SMTP connection (`smtp_default`).",
    ),
    ConfigChange(
        config=ConfigParameter("smtp", "smtp_password"),
        suggestion="Please use the SMTP connection (`smtp_default`).",
    ),
]


@providers_configuration_loaded
def lint_config(args) -> None:
    """
    Lint the airflow.cfg file for removed, or renamed configurations.

    This function scans the Airflow configuration file for parameters that are removed or renamed in
    Airflow 3.0. It provides suggestions for alternative parameters or  settings where applicable.
    CLI Arguments:
        --section: str (optional)
            The specific section of the configuration to lint.
            Example: --section core

        --option: str (optional)
            The specific option within a section to lint.
            Example: --option check_slas

        --ignore-section: str (optional)
            A section to ignore during linting.
            Example: --ignore-section webserver

        --ignore-option: str (optional)
            An option to ignore during linting.
            Example: --ignore-option smtp_user

        --verbose: flag (optional)
            Enables detailed output, including the list of ignored sections and options.
            Example: --verbose

    Examples:
        1. Lint all sections and options:
            airflow config lint

        2. Lint a specific sections:
            airflow config lint --section core,webserver

        3. Lint a specific sections and options:
            airflow config lint --section smtp --option smtp_user

        4. Ignore a sections:
            irflow config lint --ignore-section webserver,api

        5. Ignore an options:
            airflow config lint --ignore-option smtp_user,session_lifetime_days

        6. Enable verbose output:
            airflow config lint --verbose

    :param args: The CLI arguments for linting configurations.
    """
    console = AirflowConsole()
    lint_issues = []

    section_to_check_if_provided = args.section or []
    option_to_check_if_provided = args.option or []

    ignore_sections = args.ignore_section or []
    ignore_options = args.ignore_option or []

    for configuration in CONFIGS_CHANGES:
        if section_to_check_if_provided and configuration.config.section not in section_to_check_if_provided:
            continue

        if option_to_check_if_provided and configuration.config.option not in option_to_check_if_provided:
            continue

        if configuration.config.section in ignore_sections or configuration.config.option in ignore_options:
            continue

        if conf.has_option(
            configuration.config.section, configuration.config.option, lookup_from_deprecated_options=False
        ):
            lint_issues.append(configuration.message)

    if lint_issues:
        console.print("[red]Found issues in your airflow.cfg:[/red]")
        for issue in lint_issues:
            console.print(f"  - [yellow]{issue}[/yellow]")
        if args.verbose:
            console.print("\n[blue]Detailed Information:[/blue]")
            console.print(f"Ignored sections: [green]{', '.join(ignore_sections)}[/green]")
            console.print(f"Ignored options: [green]{', '.join(ignore_options)}[/green]")
        console.print("\n[red]Please update your configuration file accordingly.[/red]")
    else:
        console.print("[green]No issues found in your airflow.cfg. It is ready for Airflow 3![/green]")
