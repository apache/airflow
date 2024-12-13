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

from io import StringIO

import pygments
from pygments.lexers.configs import IniLexer

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


class ConfigChange:
    """Class representing the configuration changes in Airflow 3.0."""

    def __init__(
        self, section: str, option: str, suggestion: str = "", renamed_to: tuple[str, str] | None = None
    ) -> None:
        """
        Initialize a ConfigChange instance.

        :param section: The section of the configuration.
        :param option: The option within the section that is removed or deprecated.
        :param suggestion: A suggestion for replacing or handling the removed configuration.
        :param renamed_to: The new section and option if the configuration is renamed.
        """
        self.section = section
        self.option = option
        self.suggestion = suggestion
        self.renamed_to = renamed_to

    def get_message(self) -> str:
        """Generate a message for this configuration change."""
        lint_message = f"Removed deprecated `{self.option}` configuration parameter from `{self.section}` section. {self.suggestion}"

        if self.renamed_to:
            new_section, new_option = self.renamed_to
            rename_message = f" Please use `{new_option}` from section `{new_section}` instead."
            lint_message = lint_message + rename_message
        return lint_message


CONFIGS_CHANGES = [
    ConfigChange(
        section="admin",
        option="hide_sensitive_variable_fields",
        renamed_to=("core", "hide_sensitive_var_conn_fields"),
    ),
    ConfigChange(
        section="admin",
        option="sensitive_variable_fields",
        renamed_to=("core", "sensitive_var_conn_names"),
    ),
    ConfigChange(
        section="core",
        option="check_slas",
        suggestion="The SLA feature is removed in Airflow 3.0, to be replaced with Airflow Alerts in "
        "Airflow 3.1",
    ),
    ConfigChange(
        section="core",
        option="strict_asset_uri_validation",
        suggestion="Asset URI with a defined scheme will now always be validated strictly, "
        "raising a hard error on validation failure.",
    ),
    ConfigChange(
        section="core",
        option="worker_precheck",
        renamed_to=("celery", "worker_precheck"),
    ),
    ConfigChange(
        section="core",
        option="non_pooled_task_slot_count",
        renamed_to=("core", "default_pool_task_slot_count"),
    ),
    ConfigChange(
        section="core",
        option="dag_concurrency",
        renamed_to=("core", "max_active_tasks_per_dag"),
    ),
    ConfigChange(
        section="core",
        option="sql_alchemy_conn",
        renamed_to=("database", "sql_alchemy_conn"),
    ),
    ConfigChange(
        section="core",
        option="sql_engine_encoding",
        renamed_to=("database", "sql_engine_encoding"),
    ),
    ConfigChange(
        section="core",
        option="sql_engine_collation_for_ids",
        renamed_to=("database", "sql_engine_collation_for_ids"),
    ),
    ConfigChange(
        section="core",
        option="sql_alchemy_pool_enabled",
        renamed_to=("database", "sql_alchemy_pool_enabled"),
    ),
    ConfigChange(
        section="core",
        option="sql_alchemy_pool_size",
        renamed_to=("database", "sql_alchemy_pool_size"),
    ),
    ConfigChange(
        section="core",
        option="sql_alchemy_max_overflow",
        renamed_to=("database", "sql_alchemy_max_overflow"),
    ),
    ConfigChange(
        section="core",
        option="sql_alchemy_pool_recycle",
        renamed_to=("database", "sql_alchemy_pool_recycle"),
    ),
    ConfigChange(
        section="core",
        option="sql_alchemy_pool_pre_ping",
        renamed_to=("database", "sql_alchemy_pool_pre_ping"),
    ),
    ConfigChange(
        section="core",
        option="sql_alchemy_schema",
        renamed_to=("database", "sql_alchemy_schema"),
    ),
    ConfigChange(
        section="core",
        option="sql_alchemy_connect_args",
        renamed_to=("database", "sql_alchemy_connect_args"),
    ),
    ConfigChange(
        section="core",
        option="load_default_connections",
        renamed_to=("database", "load_default_connections"),
    ),
    ConfigChange(
        section="core",
        option="max_db_retries",
        renamed_to=("database", "max_db_retries"),
    ),
    ConfigChange(
        section="api",
        option="access_control_allow_origin",
        renamed_to=("api", "access_control_allow_origins"),
    ),
    ConfigChange(
        section="api",
        option="auth_backend",
        renamed_to=("api", "auth_backends"),
    ),
    ConfigChange(
        section="logging",
        option="enable_task_context_logger",
        suggestion="Remove TaskContextLogger: Replaced by the Log table for better handling of task log "
        "messages outside the execution context.",
    ),
    ConfigChange(
        section="metrics",
        option="metrics_use_pattern_match",
    ),
    ConfigChange(
        section="metrics",
        option="timer_unit_consistency",
        suggestion="In Airflow 3.0, the ``timer_unit_consistency`` setting in the ``metrics`` section is "
        "removed as it is now the default behaviour. This is done to standardize all timer and "
        "timing metrics to milliseconds across all metric loggers",
    ),
    ConfigChange(
        section="metrics",
        option="statsd_allow_list",
        renamed_to=("metrics", "metrics_allow_list"),
    ),
    ConfigChange(
        section="metrics",
        option="statsd_block_list",
        renamed_to=("metrics", "metrics_block_list"),
    ),
    ConfigChange(
        section="traces",
        option="otel_task_log_event",
    ),
    ConfigChange(
        section="operators",
        option="allow_illegal_arguments",
    ),
    ConfigChange(
        section="webserver",
        option="allow_raw_html_descriptions",
    ),
    ConfigChange(
        section="webserver",
        option="session_lifetime_days",
        suggestion="Please use ``session_lifetime_minutes``.",
    ),
    ConfigChange(section="webserver", option="update_fab_perms", renamed_to=("fab", "update_fab_perms")),
    ConfigChange(section="webserver", option="auth_rate_limited", renamed_to=("fab", "auth_rate_limited")),
    ConfigChange(section="webserver", option="auth_rate_limit", renamed_to=("fab", "auth_rate_limit")),
    ConfigChange(
        section="webserver",
        option="session_lifetime_days",
        renamed_to=("webserver", "session_lifetime_minutes"),
    ),
    ConfigChange(
        section="webserver",
        option="force_log_out_after",
        renamed_to=("webserver", "session_lifetime_minutes"),
    ),
    ConfigChange(section="policy", option="airflow_local_settings", renamed_to=("policy", "task_policy")),
    ConfigChange(
        section="scheduler",
        option="dependency_detector",
    ),
    ConfigChange(
        section="scheduler",
        option="processor_poll_interval",
        renamed_to=("scheduler", "scheduler_idle_sleep_time"),
    ),
    ConfigChange(
        section="scheduler",
        option="deactivate_stale_dags_interval",
        renamed_to=("scheduler", "parsing_cleanup_interval"),
    ),
    ConfigChange(section="scheduler", option="statsd_on", renamed_to=("metrics", "statsd_on")),
    ConfigChange(section="scheduler", option="max_threads", renamed_to=("scheduler", "parsing_processes")),
    ConfigChange(section="scheduler", option="statsd_host", renamed_to=("metrics", "statsd_host")),
    ConfigChange(section="scheduler", option="statsd_port", renamed_to=("metrics", "statsd_port")),
    ConfigChange(section="scheduler", option="statsd_prefix", renamed_to=("metrics", "statsd_prefix")),
    ConfigChange(
        section="scheduler", option="statsd_allow_list", renamed_to=("metrics", "statsd_allow_list")
    ),
    ConfigChange(
        section="scheduler", option="stat_name_handler", renamed_to=("metrics", "stat_name_handler")
    ),
    ConfigChange(
        section="scheduler", option="statsd_datadog_enabled", renamed_to=("metrics", "statsd_datadog_enabled")
    ),
    ConfigChange(
        section="scheduler", option="statsd_datadog_tags", renamed_to=("metrics", "statsd_datadog_tags")
    ),
    ConfigChange(
        section="scheduler",
        option="statsd_datadog_metrics_tags",
        renamed_to=("metrics", "statsd_datadog_metrics_tags"),
    ),
    ConfigChange(
        section="scheduler",
        option="statsd_custom_client_path",
        renamed_to=("metrics", "statsd_custom_client_path"),
    ),
    ConfigChange(
        section="celery", option="stalled_task_timeout", renamed_to=("scheduler", "task_queued_timeout")
    ),
    ConfigChange(section="celery", option="default_queue", renamed_to=("operators", "default_queue")),
    ConfigChange(
        section="celery", option="task_adoption_timeout", renamed_to=("scheduler", "task_queued_timeout")
    ),
    ConfigChange(
        section="kubernetes_executor",
        option="worker_pods_pending_timeout",
        renamed_to=("scheduler", "task_queued_timeout"),
    ),
    ConfigChange(
        section="kubernetes_executor",
        option="worker_pods_pending_timeout_check_interval",
        renamed_to=("scheduler", "task_queued_timeout_check_interval"),
    ),
    ConfigChange(
        section="smtp", option="smtp_user", suggestion="Please use the SMTP connection (`smtp_default`)."
    ),
    ConfigChange(
        section="smtp", option="smtp_password", suggestion="Please use the SMTP connection (`smtp_default`)."
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

        2. Lint a specific section:
            airflow config lint --section core

        3. Lint a specific section and option:
            airflow config lint --section smtp --option smtp_user

        4. Ignore a section:
            airflow config lint --ignore-section webserver

        5. Ignore an option:
            airflow config lint --ignore-option smtp_user

        6. Enable verbose output:
            airflow config lint --verbose

    :param args: The CLI arguments for linting configurations.
    """
    lint_issues = []

    section_to_check_if_provided = args.section
    option_to_check_if_provided = args.option

    ignore_sections = [args.ignore_section] if args.ignore_section else []
    ignore_options = [args.ignore_option] if args.ignore_option else []

    for config in CONFIGS_CHANGES:
        if section_to_check_if_provided and config.section != section_to_check_if_provided:
            continue

        if option_to_check_if_provided and config.option != option_to_check_if_provided:
            continue

        if config.section in ignore_sections or config.option in ignore_options:
            continue

        if conf.has_option(config.section, config.option):
            lint_issues.append(config.get_message())

    if lint_issues:
        print("Found issues in your airflow.cfg:")
        for issue in lint_issues:
            print(f"  - {issue}")
        if args.verbose:
            print("\nDetailed Information:")
            print(f"Ignored sections: {', '.join(ignore_sections)}")
            print(f"Ignored options: {', '.join(ignore_options)}")
        print("\nPlease update your configuration file accordingly.")
    else:
        print("No issues found in your airflow.cfg. It is ready for Airflow 3!")
