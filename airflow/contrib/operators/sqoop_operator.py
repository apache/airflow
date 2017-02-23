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
This module contains a sqoop 1 operator
"""

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.sqoop_hook import SqoopHook


class SqoopOperator(BaseOperator):
    """
    execute sqoop job
    """
    @apply_defaults
    def __init__(self,
                 conn_id='sqoop_default',
                 type_cmd='import',
                 table='',
                 target_dir=None,
                 append=None,
                 type=None,
                 columns=None,
                 num_mappers='1',
                 split_by=None,
                 where=None,
                 export_dir=None,
                 *args,
                 **kwargs):
        """
        :param conn_id: str
        :param type_cmd: str specify command to execute "export" or "import"
        :param table: Table to read
        :param target_dir: HDFS destination dir
        :param append: Append data to an existing dataset in HDFS
        :param type: "avro", "sequence", "text" Imports data to into the specified
           format. Defaults to text.
        :param columns: <col,col,col> Columns to import from table
        :param num_mappers: U n map task to import/export in parallel
        :param split_by: Column of the table used to split work units
        :param where: WHERE clause to use during import
        :param export_dir: HDFS Hive database directory to export
        """
        super(SqoopOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.type_cmd = type_cmd
        self.table = table
        self.target_dir = target_dir
        self.append = append
        self.type = type
        self.columns = columns
        self.num_mappers = num_mappers
        self.split_by = split_by
        self.where = where
        self.export_dir = export_dir

    def execute(self, context):
        """
        Execute sqoop job
        """
        hook = SqoopHook(conn_id=self.conn_id)

        if self.type_cmd is 'export':
            hook.export_table(
                table=self.table,
                export_dir=self.export_dir,
                num_mappers=self.num_mappers)
        else:
            hook.import_table(
                table=self.table,
                target_dir=self.target_dir,
                append=self.append,
                type=self.type,
                columns=self.columns,
                num_mappers=self.num_mappers,
                split_by=self.split_by,
                where=self.where)
