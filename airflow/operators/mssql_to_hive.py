from collections import OrderedDict
import unicodecsv as csv
import logging
from tempfile import NamedTemporaryFile
import pymssql


from airflow.hooks import HiveCliHook, MsSqlHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults


class MsSqlToHiveTransfer(BaseOperator):
    """
    Moves data from Microsoft SQL Server to Hive. The operator runs your query against
    Microsoft SQL Server, stores the file locally before loading it into a Hive table.
    If the ``create`` or ``recreate`` arguments are set to ``True``,
    a ``CREATE TABLE`` and ``DROP TABLE`` statements are generated.
    Hive data types are inferred from the cursor's metadata.
    Note that the table generated in Hive uses ``STORED AS textfile``
    which isn't the most efficient serialization format. If a
    large amount of data is loaded and/or if the table gets
    queried considerably, you may want to use this operator only to
    stage the data into a temporary table before loading it into its
    final destination using a ``HiveOperator``.
    :param sql: SQL query to execute against the Microsoft SQL Server database
    :type sql: str
    :param hive_table: target Hive table, use dot notation to target a
        specific database
    :type hive_table: str
    :param create: whether to create the table if it doesn't exist
    :type create: bool
    :param recreate: whether to drop and recreate the table at every
        execution
    :type recreate: bool
    :param partition: target partition as a dict of partition columns
        and values
    :type partition: dict
    :param delimiter: field delimiter in the file
    :type delimiter: str
    :param mssql_conn_id: source Microsoft SQL Server connection
    :type mssql_conn_id: str
    :param hive_conn_id: destination hive connection
    :type hive_conn_id: str
    """

    template_fields = ('sql', 'partition', 'hive_table')
    template_ext = ('.sql',)
    ui_color = '#a0e08c'

    @apply_defaults
    def __init__(
            self,
            sql,
            hive_table,
            create=True,
            recreate=False,
            partition=None,
            delimiter=chr(1),
            mssql_conn_id='mssql_default',
            hive_cli_conn_id='hive_cli_default',
            *args, **kwargs):
        super(MsSqlToHiveTransfer, self).__init__(*args, **kwargs)
        self.sql = sql
        self.hive_table = hive_table
        self.partition = partition
        self.create = create
        self.recreate = recreate
        self.delimiter = delimiter
        self.mssql_conn_id = mssql_conn_id
        self.hive_cli_conn_id = hive_cli_conn_id
        self.partition = partition or {}

    @classmethod
    def type_map(cls, mssql_type):
        t = pymssql
        d = {
            t.BINARY.value: 'INT',
            t.DECIMAL.value: 'FLOAT',
            t.NUMBER.value: 'INT',
        }
        return d[mssql_type] if mssql_type in d else 'STRING'

    def execute(self, context):
        hive = HiveCliHook(hive_cli_conn_id=self.hive_cli_conn_id)
        mssql = MsSqlHook(mssql_conn_id=self.mssql_conn_id)

        logging.info("Dumping Microsoft SQL Server query results to local file")
        conn = mssql.get_conn()
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
            csv_writer.writerows(cursor)
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