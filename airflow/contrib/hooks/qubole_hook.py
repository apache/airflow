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
#

import os
import time
import datetime
import logging

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow import configuration

from qds_sdk.qubole import Qubole
from qds_sdk.commands import Command, HiveCommand, PrestoCommand, HadoopCommand, PigCommand, ShellCommand, \
    SparkCommand, DbTapQueryCommand, DbExportCommand, DbImportCommand


COMMAND_CLASSES = {
    "hivecmd": HiveCommand,
    "prestocmd": PrestoCommand,
    "hadoopcmd": HadoopCommand,
    "shellcmd": ShellCommand,
    "pigcmd":  PigCommand,
    "sparkcmd": SparkCommand,
    "dbtapquerycmd": DbTapQueryCommand,
    "dbexportcmd": DbExportCommand,
    "dbimportcmd": DbImportCommand
}

HYPHEN_ARGS = ['cluster_label', 'app_id']

POSITIONAL_ARGS = ['sub_command', 'parameters']

COMMAND_ARGS = {
    "hivecmd": ['query', 'script_location', 'macros', 'tags', 'sample_size', 'cluster_label', 'name'],
    'prestocmd': ['query', 'script_location', 'macros', 'tags', 'cluster_label', 'name'],
    'hadoopcmd': ['sub_command', 'tags', 'cluster_label', 'name'],
    'shellcmd': ['script', 'script_location', 'files', 'archives', 'parameters', 'tags', 'cluster_label', 'name'],
    'pigcmd': ['script', 'script_location', 'parameters', 'tags', 'cluster_label', 'name'],
    'dbtapquerycmd': ['db_tap_id', 'query', 'macros', 'tags', 'name'],
    'sparkcmd': ['program', 'cmdline', 'sql', 'script_location', 'macros', 'tags', 'cluster_label', 'language', 'app_id', 'name', 'arguments', 'user_program_arguments'],
    'dbexportcmd': ['mode', 'hive_table', 'partition_spec', 'dbtap_id', 'db_table', 'db_update_mode', 'db_update_keys', 'export_dir', 'fields_terminated_by', 'tags', 'name'],
    'dbimportcmd': ['mode', 'hive_table', 'dbtap_id', 'db_table', 'where_clause', 'parallelism', 'extract_query', 'boundary_query', 'split_column', 'tags', 'name']
}

class QuboleHook(BaseHook):
    def __init__(self, *args, **kwargs):
        conn = self.get_connection(kwargs['qubole_conn_id'])
        Qubole.configure(api_token=conn.password, api_url=conn.host)
        self.task_id = kwargs['task_id']
        self.dag_id = kwargs['dag'].dag_id
        self.kwargs = kwargs
        self.args = self.create_cmd_args()
        self.cls = COMMAND_CLASSES[self.kwargs['command_type']]
        self.cmd = None

    def execute(self, context):
        args = self.cls.parse(self.args)
        self.cmd = self.cls.create(**args)
        context['task_instance'].xcom_push(key='qbol_cmd_id', value=self.cmd.id)
        logging.info("Qubole command created with Id: {0} and Status: {1}".format(str(self.cmd.id), self.cmd.status))

        while not Command.is_done(self.cmd.status):
            time.sleep(Qubole.poll_interval)
            self.cmd = self.cls.find(self.cmd.id)
            logging.info("Command Id: {0} and Status: {1}".format(str(self.cmd.id), self.cmd.status))

        if self.kwargs.has_key('fetch_logs') and self.kwargs['fetch_logs'] == True:
            logging.info("Logs for Command Id: {0} \n{1}".format(str(self.cmd.id), self.cmd.get_log()))

        if self.cmd.status != 'done':
            raise AirflowException('Command Id: {0} failed with Status: {1}'.format(self.cmd.id, self.cmd.status))


    def kill(self, ti):
        """
        Kill (cancel) a Qubole commmand
        :param ti: Task Instance of the dag, used to determine the Quboles command id
        :return: response from Qubole
        """
        if self.cmd is None:
            cmd_id = ti.xcom_pull(key="return_value", task_ids=self.task_id)
            self.cmd = self.cls.find(cmd_id)
        if self.cls and self.cmd:
            logging.info('Sending KILL signal to Qubole Command Id: {0}'.format(self.cmd.id))
            self.cmd.cancel()

    def get_results(self, ti=None, fp=None, inline=True, delim=None, fetch=True):
        """
        Get results (or just s3 locations) of a command from Qubole and save it into a file
        :param ti: Task Instance of the dag, used to determine the Quboles command id
        :param fp: Optional file pointer, will create one and return if None passed
        :param inline: True to download actual results, False to get s3 locations only
        :param delim: Replaces the CTL-A chars with the given delim, defalt to ',' if None given
        :param fetch: when inline is True, get results directly from s3 if results are large
        :return: file location containing actual results or s3 locations of results
        """
        if fp is None:
            iso = datetime.datetime.utcnow().isoformat()
            logpath = os.path.expanduser(configuration.get('core', 'BASE_LOG_FOLDER'))
            resultpath = logpath + '/' + self.dag_id + '/' + self.task_id + '/results'
            configuration.mkdir_p(resultpath)
            fp = open(resultpath + '/' + iso, 'wb')

        if self.cmd is None:
            cmd_id = ti.xcom_pull(key="qbol_cmd_id", task_ids=self.task_id)
            self.cmd = self.cls.find(cmd_id)

        self.cmd.get_results(fp, inline, delim, fetch)
        fp.flush()
        fp.close()
        return fp.name

    def get_log(self, ti):
        """
        Get Logs of a command from Qubole
        :param ti: Task Instance of the dag, used to determine the Quboles command id
        :return: command log as text
        """
        if self.cmd is None:
            cmd_id = ti.xcom_pull(key="qbol_cmd_id", task_ids=self.task_id)
        Command.get_log_id(self.cls, cmd_id)

    def get_jobs_id(self, ti):
        """
        Get jobs associated with a Qubole commands
        :param ti: Task Instance of the dag, used to determine the Quboles command id
        :return: Job informations assoiciated with command
        """
        if self.cmd is None:
            cmd_id = ti.xcom_pull(key="qbol_cmd_id", task_ids=self.task_id)
        Command.get_jobs_id(self.cls, cmd_id)

    def create_cmd_args(self):
        args = []
        cmd_type = self.kwargs['command_type']
        inplace_args = None

        for k,v in self.kwargs.items():
            if k in COMMAND_ARGS[cmd_type]:
                if k in HYPHEN_ARGS:
                    args.append("--{0}={1}".format(k.replace('_', '-'),v))
                elif k in POSITIONAL_ARGS:
                    inplace_args = v
                else:
                    args.append("--{0}={1}".format(k,v))

            if k == 'notify' and v is True:
                args.append("--notify")

        if inplace_args is not None:
            if cmd_type == 'hadoopcmd':
                args += inplace_args.split(' ', 1)
            else:
                args += inplace_args.split(' ')

        return args
