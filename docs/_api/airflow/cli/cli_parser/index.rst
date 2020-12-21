:mod:`airflow.cli.cli_parser`
=============================

.. py:module:: airflow.cli.cli_parser

.. autoapi-nested-parse::

   Command-line interface



Module Contents
---------------

.. data:: BUILD_DOCS
   

   

.. function:: lazy_load_command(import_path: str) -> Callable
   Create a lazy loader for command


.. py:class:: DefaultHelpParser

   Bases: :class:`argparse.ArgumentParser`

   CustomParser to display help message

   
   .. method:: _check_value(self, action, value)

      Override _check_value and check conditionally added command



   
   .. method:: error(self, message)

      Override error and use print_instead of print_usage




.. data:: _UNSET
   

   

.. py:class:: Arg(flags=_UNSET, help=_UNSET, action=_UNSET, default=_UNSET, nargs=_UNSET, type=_UNSET, choices=_UNSET, required=_UNSET, metavar=_UNSET)

   Class to keep information about command line argument

   
   .. method:: add_to_parser(self, parser: argparse.ArgumentParser)

      Add this argument to an ArgumentParser




.. function:: positive_int(value)
   Define a positive int type for an argument.


.. data:: ARG_DAG_ID
   

   

.. data:: ARG_TASK_ID
   

   

.. data:: ARG_EXECUTION_DATE
   

   

.. data:: ARG_TASK_REGEX
   

   

.. data:: ARG_SUBDIR
   

   

.. data:: ARG_START_DATE
   

   

.. data:: ARG_END_DATE
   

   

.. data:: ARG_OUTPUT_PATH
   

   

.. data:: ARG_DRY_RUN
   

   

.. data:: ARG_PID
   

   

.. data:: ARG_DAEMON
   

   

.. data:: ARG_STDERR
   

   

.. data:: ARG_STDOUT
   

   

.. data:: ARG_LOG_FILE
   

   

.. data:: ARG_YES
   

   

.. data:: ARG_OUTPUT
   

   

.. data:: ARG_COLOR
   

   

.. data:: ARG_DAG_ID_OPT
   

   

.. data:: ARG_NO_BACKFILL
   

   

.. data:: ARG_STATE
   

   

.. data:: ARG_LIMIT
   

   

.. data:: ARG_NUM_EXECUTIONS
   

   

.. data:: ARG_MARK_SUCCESS
   

   

.. data:: ARG_VERBOSE
   

   

.. data:: ARG_LOCAL
   

   

.. data:: ARG_DONOT_PICKLE
   

   

.. data:: ARG_BF_IGNORE_DEPENDENCIES
   

   

.. data:: ARG_BF_IGNORE_FIRST_DEPENDS_ON_PAST
   

   

.. data:: ARG_POOL
   

   

.. data:: ARG_DELAY_ON_LIMIT
   

   

.. data:: ARG_RESET_DAG_RUN
   

   

.. data:: ARG_RERUN_FAILED_TASKS
   

   

.. data:: ARG_RUN_BACKWARDS
   

   

.. data:: ARG_SHOW_DAGRUN
   

   

.. data:: ARG_IMGCAT_DAGRUN
   

   

.. data:: ARG_SAVE_DAGRUN
   

   

.. data:: ARG_TREE
   

   

.. data:: ARG_UPSTREAM
   

   

.. data:: ARG_ONLY_FAILED
   

   

.. data:: ARG_ONLY_RUNNING
   

   

.. data:: ARG_DOWNSTREAM
   

   

.. data:: ARG_EXCLUDE_SUBDAGS
   

   

.. data:: ARG_EXCLUDE_PARENTDAG
   

   

.. data:: ARG_DAG_REGEX
   

   

.. data:: ARG_SAVE
   

   

.. data:: ARG_IMGCAT
   

   

.. data:: ARG_RUN_ID
   

   

.. data:: ARG_CONF
   

   

.. data:: ARG_EXEC_DATE
   

   

.. data:: ARG_POOL_NAME
   

   

.. data:: ARG_POOL_SLOTS
   

   

.. data:: ARG_POOL_DESCRIPTION
   

   

.. data:: ARG_POOL_IMPORT
   

   

.. data:: ARG_POOL_EXPORT
   

   

.. data:: ARG_VAR
   

   

.. data:: ARG_VAR_VALUE
   

   

.. data:: ARG_DEFAULT
   

   

.. data:: ARG_JSON
   

   

.. data:: ARG_VAR_IMPORT
   

   

.. data:: ARG_VAR_EXPORT
   

   

.. data:: ARG_PRINCIPAL
   

   

.. data:: ARG_KEYTAB
   

   

.. data:: ARG_INTERACTIVE
   

   

.. data:: ARG_FORCE
   

   

.. data:: ARG_RAW
   

   

.. data:: ARG_IGNORE_ALL_DEPENDENCIES
   

   

.. data:: ARG_IGNORE_DEPENDENCIES
   

   

.. data:: ARG_IGNORE_DEPENDS_ON_PAST
   

   

.. data:: ARG_SHIP_DAG
   

   

.. data:: ARG_PICKLE
   

   

.. data:: ARG_JOB_ID
   

   

.. data:: ARG_CFG_PATH
   

   

.. data:: ARG_MIGRATION_TIMEOUT
   

   

.. data:: ARG_PORT
   

   

.. data:: ARG_SSL_CERT
   

   

.. data:: ARG_SSL_KEY
   

   

.. data:: ARG_WORKERS
   

   

.. data:: ARG_WORKERCLASS
   

   

.. data:: ARG_WORKER_TIMEOUT
   

   

.. data:: ARG_HOSTNAME
   

   

.. data:: ARG_DEBUG
   

   

.. data:: ARG_ACCESS_LOGFILE
   

   

.. data:: ARG_ERROR_LOGFILE
   

   

.. data:: ARG_NUM_RUNS
   

   

.. data:: ARG_DO_PICKLE
   

   

.. data:: ARG_QUEUES
   

   

.. data:: ARG_CONCURRENCY
   

   

.. data:: ARG_CELERY_HOSTNAME
   

   

.. data:: ARG_UMASK
   

   

.. data:: ARG_BROKER_API
   

   

.. data:: ARG_FLOWER_HOSTNAME
   

   

.. data:: ARG_FLOWER_PORT
   

   

.. data:: ARG_FLOWER_CONF
   

   

.. data:: ARG_FLOWER_URL_PREFIX
   

   

.. data:: ARG_FLOWER_BASIC_AUTH
   

   

.. data:: ARG_TASK_PARAMS
   

   

.. data:: ARG_POST_MORTEM
   

   

.. data:: ARG_ENV_VARS
   

   

.. data:: ARG_CONN_ID
   

   

.. data:: ARG_CONN_ID_FILTER
   

   

.. data:: ARG_CONN_URI
   

   

.. data:: ARG_CONN_TYPE
   

   

.. data:: ARG_CONN_HOST
   

   

.. data:: ARG_CONN_LOGIN
   

   

.. data:: ARG_CONN_PASSWORD
   

   

.. data:: ARG_CONN_SCHEMA
   

   

.. data:: ARG_CONN_PORT
   

   

.. data:: ARG_CONN_EXTRA
   

   

.. data:: ARG_CONN_EXPORT
   

   

.. data:: ARG_CONN_EXPORT_FORMAT
   

   

.. data:: ARG_USERNAME
   

   

.. data:: ARG_USERNAME_OPTIONAL
   

   

.. data:: ARG_FIRSTNAME
   

   

.. data:: ARG_LASTNAME
   

   

.. data:: ARG_ROLE
   

   

.. data:: ARG_EMAIL
   

   

.. data:: ARG_EMAIL_OPTIONAL
   

   

.. data:: ARG_PASSWORD
   

   

.. data:: ARG_USE_RANDOM_PASSWORD
   

   

.. data:: ARG_USER_IMPORT
   

   

.. data:: ARG_USER_EXPORT
   

   

.. data:: ARG_CREATE_ROLE
   

   

.. data:: ARG_LIST_ROLES
   

   

.. data:: ARG_ROLES
   

   

.. data:: ARG_AUTOSCALE
   

   

.. data:: ARG_SKIP_SERVE_LOGS
   

   

.. data:: ARG_ANONYMIZE
   

   

.. data:: ARG_FILE_IO
   

   

.. data:: ARG_SECTION
   

   

.. data:: ARG_OPTION
   

   

.. data:: ARG_NAMESPACE
   

   

.. data:: ALTERNATIVE_CONN_SPECS_ARGS
   

   

.. py:class:: ActionCommand

   Bases: :class:`typing.NamedTuple`

   Single CLI command

   .. attribute:: name
      :annotation: :str

      

   .. attribute:: help
      :annotation: :str

      

   .. attribute:: func
      :annotation: :Callable

      

   .. attribute:: args
      :annotation: :Iterable[Arg]

      

   .. attribute:: description
      :annotation: :Optional[str]

      

   .. attribute:: epilog
      :annotation: :Optional[str]

      


.. py:class:: GroupCommand

   Bases: :class:`typing.NamedTuple`

   ClI command with subcommands

   .. attribute:: name
      :annotation: :str

      

   .. attribute:: help
      :annotation: :str

      

   .. attribute:: subcommands
      :annotation: :Iterable

      

   .. attribute:: description
      :annotation: :Optional[str]

      

   .. attribute:: epilog
      :annotation: :Optional[str]

      


.. data:: CLICommand
   

   

.. data:: DAGS_COMMANDS
   

   

.. data:: TASKS_COMMANDS
   

   

.. data:: POOLS_COMMANDS
   

   

.. data:: VARIABLES_COMMANDS
   

   

.. data:: DB_COMMANDS
   

   

.. data:: CONNECTIONS_COMMANDS
   

   

.. data:: USERS_COMMANDS
   

   

.. data:: ROLES_COMMANDS
   

   

.. data:: CELERY_COMMANDS
   

   

.. data:: CONFIG_COMMANDS
   

   

.. data:: KUBERNETES_COMMANDS
   

   

.. data:: airflow_commands
   :annotation: :List[CLICommand]

   

.. data:: ALL_COMMANDS_DICT
   :annotation: :Dict[str, CLICommand]

   

.. data:: DAG_CLI_COMMANDS
   :annotation: :Set[str]

   

.. py:class:: AirflowHelpFormatter

   Bases: :class:`argparse.HelpFormatter`

   Custom help formatter to display help message.

   It displays simple commands and groups of commands in separate sections.

   
   .. method:: _format_action(self, action: Action)




.. function:: get_parser(dag_parser: bool = False) -> argparse.ArgumentParser
   Creates and returns command line argument parser


.. function:: _sort_args(args: Iterable[Arg]) -> Iterable[Arg]
   Sort subcommand optional args, keep positional args


.. function:: _add_command(subparsers: argparse._SubParsersAction, sub: CLICommand) -> None

.. function:: _add_action_command(sub: ActionCommand, sub_proc: argparse.ArgumentParser) -> None

.. function:: _add_group_command(sub: GroupCommand, sub_proc: argparse.ArgumentParser) -> None

