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

"""
This module contains a sqoop 1 hook
"""

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException

import logging
import subprocess

log = logging.getLogger(__name__)


class SqoopHook(BaseHook):
    """
    This Hook is a wrapper around the sqoop 1 binary. To be able to use te hook
    it is required that "sqoop" is in the PATH.
    :param hive_home: (from json) The location of hive-site.xml
    :type hive_home: str
    :param job_tracker: (from json) <local|jobtracker:port> specify a job tracker
    :type job_tracker: str
    :param namenode: (from json) specify a namenode
    :type namenode: str
    :param lib_jars: (from json) specify comma separated jar files to
        include in the classpath.
    :type lib_jars: str
    :param files: (from json) specify comma separated files to be copied
        to the map reduce cluster
    :type files: (from json) str
    :param archives: (from json)  specify comma separated archives to be
        unarchived on the compute machines.
    :type archives: str
    """
    def __init__(self, conn_id='sqoop_default'):
        conn = self.get_connection(conn_id)
        self.hive_home = conn.extra_dejson.get('hive_home', None)
        self.job_tracker = conn.extra_dejson.get('job_tracker', None)
        self.namenode = conn.extra_dejson.get('namenode', None)
        self.lib_jars = conn.extra_dejson.get('libjars', None)
        self.files = conn.extra_dejson.get('files', None)
        self.archives = conn.extra_dejson.get('archives', None)
        self.conn = conn

    def get_conn(self):
        pass

    def Popen(self, cmd, export=False, **kwargs):
        """
        Remote Popen

        :param cmd: command to remotely execute
        :param kwargs: extra arguments to Popen (see subprocess.Popen)
        :return: handle to subprocess
        """
        prefixed_cmd = self._prepare_command(cmd, export=export)
        print prefixed_cmd
        return subprocess.Popen(prefixed_cmd, **kwargs)

    def _prepare_command(self, cmd, export=False):

        connection_cmd = ""

        if export:
            connection_cmd = ["sqoop", "export", "--verbose"]
        else:
            connection_cmd = ["sqoop", "import", "--verbose"]

        if self.job_tracker:
            connection_cmd += ["-jt", self.job_tracker]
        if self.conn.login:
            connection_cmd += ["--username", self.conn.login]
        # todo: put this in a password file
        if self.conn.password:
            connection_cmd += ["--password", self.conn.password]
        if self.lib_jars:
            connection_cmd += ["-libjars", self.lib_jars]
        if self.files:
            connection_cmd += ["-files", self.files]
        if self.namenode:
            connection_cmd += ["-fs", self.namenode]
        if self.archives:
            connection_cmd += ["-archives", self.archives]

        connection_cmd += ["--connect", "{}:{}/{}".format(self.conn.host, self.conn.port, self.conn.schema)]
        connection_cmd += cmd

        return connection_cmd

    def _import_cmd(self, target_dir,
                    append=False, type="text",
                    num_mappers=None, split_by=None):

        cmd = ["--target-dir", target_dir]

        if not num_mappers:
            num_mappers = 1

        cmd += ["--num-mappers", str(num_mappers)]

        if split_by:
            cmd += ["--split-by", split_by]

        if append:
            cmd += ["--append"]

        if type == "avro":
            cmd += ["--as-avrodatafile"]
        elif type == "sequence":
            cmd += ["--as-sequencefile"]
        else:
            cmd += ["--as-textfile"]

        return cmd

    def import_table(self, table, target_dir,
                     append=False, type="text", columns=None,
                     num_mappers=None, split_by=None, where=None):
        """
        Imports table from remote location to target dir. Arguments are
        copies of direct sqoop command line arguments
        :param table: Table to read
        :param target_dir: HDFS destination dir
        :param append: Append data to an existing dataset in HDFS
        :param type: "avro", "sequence", "text" Imports data to into the specified
            format. Defaults to text.
        :param columns: <col,col,col…> Columns to import from table
        :param num_mappers: Use n map tasks to import in parallel
        :param split_by: Column of the table used to split work units
        :param where: WHERE clause to use during import
        """
        cmd = self._import_cmd(target_dir, append, type,
                               num_mappers, split_by)
        cmd += ["--table", table]
        if columns:
            cmd += ["--columns", columns]
        if where:
            cmd += ["--where", where]

        p = self.Popen(cmd, export=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, stderr = p.communicate()

        if p.returncode != 0:
            # I like this better: RemoteCalledProcessError(p.returncode, cmd, self.host, output=output)
            raise AirflowException("Cannot execute {} on {}. Error code is: "
                                   "{}. Output: {}, Stderr: {}"
                                   .format(cmd, self.conn.host,
                                           p.returncode, output, stderr))

    def _export_cmd(self, export_dir, num_mappers=None):

        cmd = ["--export-dir", export_dir]

        if not num_mappers:
            num_mappers = 1

        cmd += ["--num-mappers", str(num_mappers)]

        return cmd

    def export_table(self, table, export_dir,
                     num_mappers=None):
        """
        Exports Hive table to remote location. Arguments are copies of direct
        sqoop command line Arguments
        :param table: Table remote destination
        :param export_dir: Hive table to export
        :param num_mappers: Use n map tasks to import in parallel
        """

        cmd = self._export_cmd(export_dir, num_mappers)
        cmd += ["--table", table]

        p = self.Popen(cmd, export=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, stderr = p.communicate()

        if p.returncode != 0:
            # I like this better: RemoteCalledProcessError(p.returncode, cmd, self.host, output=output)
            raise AirflowException("Cannot execute {} on {}. Error code is: "
                                   "{}. Output: {}, Stderr: {}"
                                   .format(cmd, self.conn.host,
                                           p.returncode, output, stderr))

    def import_query(self, query, target_dir,
                     append=False, type="text",
                     num_mappers=None, split_by=None):
        """

        :param query: Free format query to run
        :param target_dir: HDFS destination dir
        :param append: Append data to an existing dataset in HDFS
        :param type: "avro", "sequence", "text" Imports data to into the specified
            format. Defaults to text.
        :param num_mappers: Use n map tasks to import in parallel
        :param split_by: Column of the table used to split work units
        """
        cmd = self._import_cmd(target_dir, append, type,
                               num_mappers, split_by)
        cmd += ["--query", query]

        p = self.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, stderr = p.communicate()

        if p.returncode != 0:
            # I like this better: RemoteCalledProcessError(p.returncode, cmd, self.host, output=output)
            raise AirflowException("Cannot execute {} on {}. Error code is: "
                                   "{}. Output: {}, Stderr: {}"
                                   .format(cmd, self.conn.host,
                                           p.returncode, output, stderr))
