from airflow.models import BaseOperator
from airflow.utils import apply_defaults
from airflow.contrib.hooks import QuboleHook


class QuboleOperator(BaseOperator):
    """
    Executes commands on Qubole (https://qubole.com).

    mandatory:
        :param command_type: type of command to be executed, e.g. hivecmd, shellcmd, hadoopcmd
    other:
        hivecmd:
            :param query: inline query statement
            :param script_location: s3 location containing query statement
            :param sample_size:
            :param macros: macro values which were used in query
            :param tags: array of tags to be assigned with the command
            :param cluster_label: cluster label on which to execute command
            :param name: name to be given to command
        prestocmd:
            :param query: inline query statement
            :param script_location: s3 location containing query statement
            :param macros: macro values which were used in query
            :param tags: array of tags to be assigned with the command
            :param cluster_label: cluster label on which to execute command
            :param name: name to be given to command
        hadoopcmd:
            :param sub_commnad: must be one these ["jar", "s3distcp", "streaming"] followed by 1 or more args
            :param tags: array of tags to be assigned with the command
            :param cluster_label: cluster label on which to execute command
            :param name: name to be given to command
        shellcmd:
            :param script: inline command with args
            :param script_location: s3 location containing query statement
            :param files: list of files in s3 bucket as file1,file2 format. These files will be copied into the working
                          directory where the qubole command is being executed.
            :param archives: list of archives in s3 bucket as archive1,archive2 format. These will be unarchived into
                             the working directory where the qubole command is being executed
            :param parameters: any extra args which need to be passed to script (only wwhen script_location is supplied)
            :param tags: array of tags to be assigned with the command
            :param cluster_label: cluster label on which to execute command
            :param name: name to be given to command
        pigcmd:
            :param script: inline query statement (latin_statements)
            :param script_location: s3 location containing pig query
            :param parameters: any extra args which need to be passed to script (only wwhen script_location is supplied
            :param tags: array of tags to be assigned with the command
            :param cluster_label: cluster label on which to execute command
            :param name: name to be given to command
        dbtapquerycmd:
            :param db_tap_id: data store ID of the target database, in Qubole.
            :param query: inline query statement
            :param macros: macro values which were used in query
            :param tags: array of tags to be assigned with the command
            :param name: name to be given to command
        sparkcmd:
            :param program: the complete Spark Program in Scala, SQL, Command, R, or Python
            :param cmdline: spark-submit command line, all required information must be specify in cmdline itself.
            :param sql: inline sql query
            :param script_location: s3 location containing query statement
            :param language: language of the program, Scala, SQL, Command, R, or Python
            :param app_id: ID of an Spark job server app
            :param arguments: spark-submit command line arguments
            :param user_program_arguments: arguments that the user program takes in
            :param macros: macro values which were used in query
            :param tags: array of tags to be assigned with the command
            :param cluster_label: cluster label on which to execute command
            :param name: name to be given to command
        dbexportcmd:
            :param mode: 1 (simple), 2 (advance)
            :param hive_table: Name of the hive table
            :param partition_spec: partition specification for Hive table.
            :param dbtap_id: data store ID of the target database, in Qubole.
            :param db_table: name of the db table
            :param db_update_mode: allowinsert or updateonly
            :param db_update_keys: columns used to determine the uniqueness of rows
            :param export_dir: HDFS/S3 location from which data will be exported.
            :param fields_terminated_by:
            :param tags: array of tags to be assigned with the command
            :param name: name to be given to command
        dbimportcmd:
            :param mode: 1 (simple), 2 (advance)
            :param hive_table: Name of the hive table
            :param dbtap_id: data store ID of the target database, in Qubole.
            :param db_table: name of the db table
            :param where_clause: where clause, if any
            :param parallelism: number of parallel db connections to use for extracting data
            :param extract_query: SQL query to extract data from db. $CONDITIONS must be part of the where clause.
            :param boundary_query: Query to be used get range of row IDs to be extracted
            :param split_column: Column used as row ID to split data into ranges (mode 2)
            :param tags: array of tags to be assigned with the command
            :param name: name to be given to command

    """

    ui_color = '#3064A1'
    ui_fgcolor = '#fff'

    @apply_defaults
    def __init__(self, qubole_conn_id="qubole_default", *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.kwargs['qubole_conn_id'] = qubole_conn_id
        self.hook = QuboleHook(*self.args, **self.kwargs)
        super(QuboleOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        return self.hook.execute(context)

    def on_kill(self, ti):
        self.hook.kill(ti)

    def get_results(self, ti=None, fp=None, inline=True, delim=None, fetch=True):
        return self.hook.get_results(ti, fp, inline, delim, fetch)

    def get_log(self, ti):
        return self.hook.get_log(ti)

    def get_jobs_id(self, ti):
        return self.hook.get_jobs_id(ti)



