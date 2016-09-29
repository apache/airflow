# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.qubole_hook import QuboleHook


class QuboleOperator(BaseOperator):
    """
    Execute tasks (commands) on QDS (https://qubole.com).

    :param qubole_conn_id: Connection id which consists of qds auth_token
    :type qubole_conn_id: str

    kwargs:
        :command_type: type of command to be executed, e.g. hivecmd, shellcmd, hadoopcmd
        :tags: array of tags to be assigned with the command
        :cluster_label: cluster label on which the command will be executed
        :name: name to be given to command
        :notify: whether to send email on command completion or not (default is False)

        **Arguments specific to command types**

        hivecmd:
            :query: inline query statement
            :script_location: s3 location containing query statement
            :sample_size: size of sample in bytes on which to run query
            :macros: macro values which were used in query
        prestocmd:
            :query: inline query statement
            :script_location: s3 location containing query statement
            :macros: macro values which were used in query
        hadoopcmd:
            :sub_commnad: must be one these ["jar", "s3distcp", "streaming"] followed by
                1 or more args
        shellcmd:
            :script: inline command with args
            :script_location: s3 location containing query statement
            :files: list of files in s3 bucket as file1,file2 format. These files will be
                copied into the working directory where the qubole command is being
                executed.
            :archives: list of archives in s3 bucket as archive1,archive2 format. These
                will be unarchived intothe working directory where the qubole command is
                being executed
            :parameters: any extra args which need to be passed to script (only when
                script_location is supplied)
        pigcmd:
            :script: inline query statement (latin_statements)
            :script_location: s3 location containing pig query
            :parameters: any extra args which need to be passed to script (only when
                script_location is supplied
        sparkcmd:
            :program: the complete Spark Program in Scala, SQL, Command, R, or Python
            :cmdline: spark-submit command line, all required information must be specify
                in cmdline itself.
            :sql: inline sql query
            :script_location: s3 location containing query statement
            :language: language of the program, Scala, SQL, Command, R, or Python
            :app_id: ID of an Spark job server app
            :arguments: spark-submit command line arguments
            :user_program_arguments: arguments that the user program takes in
            :macros: macro values which were used in query
        dbtapquerycmd:
            :db_tap_id: data store ID of the target database, in Qubole.
            :query: inline query statement
            :macros: macro values which were used in query
        dbexportcmd:
            :mode: 1 (simple), 2 (advance)
            :hive_table: Name of the hive table
            :partition_spec: partition specification for Hive table.
            :dbtap_id: data store ID of the target database, in Qubole.
            :db_table: name of the db table
            :db_update_mode: allowinsert or updateonly
            :db_update_keys: columns used to determine the uniqueness of rows
            :export_dir: HDFS/S3 location from which data will be exported.
            :fields_terminated_by: hex of the char used as column separator in the dataset
        dbimportcmd:
            :mode: 1 (simple), 2 (advance)
            :hive_table: Name of the hive table
            :dbtap_id: data store ID of the target database, in Qubole.
            :db_table: name of the db table
            :where_clause: where clause, if any
            :parallelism: number of parallel db connections to use for extracting data
            :extract_query: SQL query to extract data from db. $CONDITIONS must be part
                of the where clause.
            :boundary_query: Query to be used get range of row IDs to be extracted
            :split_column: Column used as row ID to split data into ranges (mode 2)

    .. note:: Following fields are template-supported : ``query``, ``script_location``,
        ``sub_command``, ``script``, ``files``, ``archives``, ``program``, ``cmdline``,
        ``sql``, ``where_clause``, ``extract_query``, ``boundary_query``, ``macros``,
        ``tags``, ``name``, ``parameters``, ``dbtap_id``, ``hive_table``, ``db_table``,
        ``split_column``, ``db_update_keys``, ``export_dir``, ``partition_spec``. You
        can also use ``.txt`` files for template driven use cases.

    .. note:: In QuboleOperator there is a default handler for task failures and retries,
        which generally kills the command running at QDS for the corresponding task
        instance. You can override this behavior by providing your own failure and retry
        handler in task definition.
    """

    template_fields = ('query', 'script_location', 'sub_command', 'script', 'files',
                       'archives', 'program', 'cmdline', 'sql', 'where_clause', 'tags',
                       'extract_query', 'boundary_query', 'macros', 'name', 'parameters',
                       'dbtap_id', 'hive_table', 'db_table', 'split_column',
                       'db_update_keys', 'export_dir', 'partition_spec')
    template_ext = ('.txt',)
    ui_color = '#3064A1'
    ui_fgcolor = '#fff'

    @apply_defaults
    def __init__(self, qubole_conn_id="qubole_default", *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.kwargs['qubole_conn_id'] = qubole_conn_id
        self.hook = QuboleHook(*self.args, **self.kwargs)
        super(QuboleOperator, self).__init__(*args, **kwargs)

        if self.on_failure_callback is None:
            self.on_failure_callback = QuboleHook.handle_failure_retry

        if self.on_retry_callback is None:
            self.on_retry_callback = QuboleHook.handle_failure_retry

    def execute(self, context):
        # Reinitiating the hook, as some template fields might have changed
        self.hook = QuboleHook(*self.args, **self.kwargs)
        return self.hook.execute(context)

    def on_kill(self, ti):
        self.hook.kill(ti)

    def get_results(self, ti=None, fp=None, inline=True, delim=None, fetch=True):
        return self.hook.get_results(ti, fp, inline, delim, fetch)

    def get_log(self, ti):
        return self.hook.get_log(ti)

    def get_jobs_id(self, ti):
        return self.hook.get_jobs_id(ti)

    def __getattribute__(self, name):
        if name in QuboleOperator.template_fields:
            if name in self.kwargs:
                return self.kwargs[name]
            else:
                return ''
        else:
            return object.__getattribute__(self, name)

    def __setattr__(self, name, value):
        if name in QuboleOperator.template_fields:
            self.kwargs[name] = value
        else:
            object.__setattr__(self, name, value)
