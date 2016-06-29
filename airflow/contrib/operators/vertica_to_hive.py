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

from builtins import chr
from collections import OrderedDict
import unicodecsv as csv
import logging
from tempfile import NamedTemporaryFile

from airflow.hooks.hive_hooks import HiveCliHook
from airflow.contrib.hooks.vertica_hook import VerticaHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class VerticaToHiveTransfer(BaseOperator):
    """
    Moves data from Vertia to Hive. The operator runs
    your query against Vertia, stores the file locally
    before loading it into a Hive table. If the ``create`` or
    ``recreate`` arguments are set to ``True``,
    a ``CREATE TABLE`` and ``DROP TABLE`` statements are generated.
    Hive data types are inferred from the cursor's metadata.
    Note that the table generated in Hive uses ``STORED AS textfile``
    which isn't the most efficient serialization format. If a
    large amount of data is loaded and/or if the table gets
    queried considerably, you may want to use this operator only to
    stage the data into a temporary table before loading it into its
    final destination using a ``HiveOperator``.

    :param sql: SQL query to execute against the Vertia database
    :type sql: str
    :param hive_table: target Hive table, use dot notation to target a
        specific database
    :type hive_table: str
    :param create: whether to create the table if it doesn't exist
    :type create: bool
    :param recreate: whether to drop and recreate the table at every execution
    :type recreate: bool
    :param partition: target partition as a dict of partition columns and values
    :type partition: dict
    :param delimiter: field delimiter in the file
    :type delimiter: str
    :param vertica_conn_id: source Vertica connection
    :type vertica_conn_id: str
    :param hive_conn_id: destination hive connection
    :type hive_conn_id: str

    """

    template_fields = ('sql', 'partition', 'hive_table')
    template_ext = ('.sql',)
    ui_color = '#b4e0ff'

    @apply_defaults
    def __init__(
            self,
            sql,
            hive_table,
            create=True,
            recreate=False,
            partition=None,
            delimiter=chr(1),
            vertica_conn_id='vertica_default',
            hive_cli_conn_id='hive_cli_default',
            *args, **kwargs):
        super(VerticaToHiveTransfer, self).__init__(*args, **kwargs)
        self.sql = sql
        self.hive_table = hive_table
        self.partition = partition
        self.create = create
        self.recreate = recreate
        self.delimiter = str(delimiter)
        self.vertica_conn_id = vertica_conn_id
        self.hive_cli_conn_id = hive_cli_conn_id
        self.partition = partition or {}

    @classmethod
    def type_map(cls, vertica_type):
        # vertica-python datatype.py donot provied the full type mapping access.
        # Manual hack. Reference: https://github.com/uber/vertica-python/blob/master/vertica_python/vertica/column.py
        d = {
            5: 'BOOLEAN',
            6: 'INT',
            7: 'FLOAT',
            8: 'STRING',
            9: 'STRING',
            16: 'FLOAT',
        }
        return d[vertica_type] if vertica_type in d else 'STRING'

    def execute(self, context):
        hive = HiveCliHook(hive_cli_conn_id=self.hive_cli_conn_id)
        vertica = VerticaHook(vertica_conn_id=self.vertica_conn_id)

        logging.info("Dumping Vertica query results to local file")
        conn = vertica.get_conn()
        cursor = conn.cursor()
        cursor.execute(self.sql)
        with NamedTemporaryFile("w") as f:
            csv_writer = csv.writer(f, delimiter=self.delimiter, encoding='utf-8')
            field_dict = OrderedDict()
            col_count = 0
            for field in cursor.description:
                col_count += 1
                col_position = "Column{position}".format(position=col_count)
                field_dict[col_position if field[0] == '' else field[0]] = self.type_map(field[1])
            csv_writer.writerows(cursor.iterate())
            f.flush()
            cursor.close()
            conn.close()
            logging.info("Loading file into Hive")
            hive.load_file(
                f.name,
                self.hive_table,
                field_dict=field_dict,
                create=self.create,
                partition=self.partition,
                delimiter=self.delimiter,
                recreate=self.recreate)
