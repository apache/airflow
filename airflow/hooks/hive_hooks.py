import csv
import logging
import json
import subprocess
from tempfile import NamedTemporaryFile


from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from hive_service import ThriftHive
import pyhs2

from airflow.utils import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.utils import TemporaryDirectory


class HiveCliHook(BaseHook):
    '''
    Simple wrapper around the hive CLI
    '''
    def __init__(
            self,
            hive_cli_conn_id="hive_cli_default"
            ):
        conn = self.get_connection(hive_cli_conn_id)
        self.hive_cli_params = ""
        try:
            self.hive_cli_params = json.loads(conn.extra)['hive_cli_params']
        except:
            pass

    def run_cli(self, hql, schema=None):
        '''
        Run an hql statement using the hive cli

        >>> hh = HiveCliHook()
        >>> hh.run_cli("USE airflow;")
        '''
        if schema:
            hql = "USE {schema};\n{hql}".format(**locals())

        with TemporaryDirectory(prefix='airflow_hiveop_') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir) as f:
                f.write(hql)
                f.flush()
                fname = f.name
                hive_cmd = ['hive', '-f', fname]
                if self.hive_cli_params:
                    hive_params_list = self.hive_cli_params.split()
                    hive_cmd.extend(hive_params_list)
                sp = subprocess.Popen(
                    hive_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    cwd=tmp_dir)
                all_err = ''
                self.sp = sp
                stdout = ""
                for line in iter(sp.stdout.readline, ''):
                    stdout += line
                    logging.info(line.strip())
                sp.wait()

                if sp.returncode:
                    raise AirflowException(all_err)

                return stdout

    def load_file(
            self,
            filepath,
            table,
            delimiter=",",
            field_dict=None,
            create=True,
            overwrite=True,
            partition=None,
            recreate=False):
        """
        Loads a local file into Hive

        Note that the table generated in Hive uses ``STORED AS textfile``
        which isn't the most efficient serialization format. If a
        large amount of data is loaded and/or if the tables gets
        queried considerably, you may want to use this operator only to
        stage the data into a temporary table before loading it into its
        final destination using a ``HiveOperator``.

        :param table: target Hive table, use dot notation to target a
            specific database
        :type table: str
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
        """
        hql = ''
        if recreate:
            hql += "DROP TABLE IF EXISTS {table};\n"
        if create or recreate:
            fields = ",\n    ".join(
                [k + ' ' + v for k, v in field_dict.items()])
            hql += "CREATE EXTERNAL TABLE IF NOT EXISTS {table} (\n{fields})\n"
            if partition:
                pfields = ",\n    ".join(
                    [p + " STRING" for p in partition])
                hql += "PARTITIONED BY ({pfields})\n"
            hql += "ROW FORMAT DELIMITED\n"
            hql += "FIELDS TERMINATED BY '{delimiter}'\n"
            hql += "STORED AS textfile;"
        hql = hql.format(**locals())
        logging.info(hql)
        self.run_cli(hql)
        hql = "LOAD DATA LOCAL INPATH '{filepath}' "
        if overwrite:
            hql += "OVERWRITE "
        hql += "INTO TABLE {table} "
        if partition:
            pvals = ", ".join(
                ["{0}='{1}'".format(k, v) for k, v in partition.items()])
            hql += "PARTITION ({pvals});"
        hql = hql.format(**locals())
        logging.info(hql)
        self.run_cli(hql)

    def kill(self):
        if hasattr(self, 'sp'):
            if self.sp.poll() is None:
                print("Killing the Hive job")
                self.sp.kill()


class HiveMetastoreHook(BaseHook):
    '''
    Wrapper to interact with the Hive Metastore
    '''
    def __init__(self, metastore_conn_id='metastore_default'):
        self.metastore_conn = self.get_connection(metastore_conn_id)
        self.metastore = self.get_metastore_client()

    def __getstate__(self):
        # This is for pickling to work despite the thirft hive client not
        # being pickable
        d = dict(self.__dict__)
        del d['metastore']
        return d

    def __setstate__(self, d):
        self.__dict__.update(d)
        self.__dict__['metastore'] = self.get_metastore_client()

    def get_metastore_client(self):
        '''
        Returns a Hive thrift client.
        '''
        ms = self.metastore_conn
        transport = TSocket.TSocket(ms.host, ms.port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        return ThriftHive.Client(protocol)

    def get_conn(self):
        return self.metastore

    def check_for_partition(self, schema, table, partition):
        '''
        Checks whether a partition exists

        >>> hh = HiveMetastoreHook()
        >>> t = 'static_babynames_partitioned'
        >>> hh.check_for_partition('airflow', t, "ds='2015-01-01'")
        True
        '''
        self.metastore._oprot.trans.open()
        partitions = self.metastore.get_partitions_by_filter(
            schema, table, partition, 1)
        self.metastore._oprot.trans.close()
        if partitions:
            return True
        else:
            return False

    def get_table(self, table_name, db='default'):
        '''
        Get a metastore table object

        >>> hh = HiveMetastoreHook()
        >>> t = hh.get_table(db='airflow', table_name='static_babynames')
        >>> t.tableName
        'static_babynames'
        >>> [col.name for col in t.sd.cols]
        ['state', 'year', 'name', 'gender', 'num']
        '''
        self.metastore._oprot.trans.open()
        if db == 'default' and '.' in table_name:
            db, table_name = table_name.split('.')[:2]
        table = self.metastore.get_table(dbname=db, tbl_name=table_name)
        self.metastore._oprot.trans.close()
        return table

    def get_tables(self, db, pattern='*'):
        '''
        Get a metastore table object
        '''
        self.metastore._oprot.trans.open()
        tables = self.metastore.get_tables(db_name=db, pattern=pattern)
        objs = self.metastore.get_table_objects_by_name(db, tables)
        self.metastore._oprot.trans.close()
        return objs

    def get_databases(self, pattern='*'):
        '''
        Get a metastore table object
        '''
        self.metastore._oprot.trans.open()
        dbs = self.metastore.get_databases(pattern)
        self.metastore._oprot.trans.close()
        return dbs

    def get_partitions(
            self, schema, table_name, filter=None):
        '''
        Returns a list of all partitions in a table. Works only
        for tables with less than 32767 (java short max val).
        For subpartitioned table, the number might easily exceed this.

        >>> hh = HiveMetastoreHook()
        >>> t = 'static_babynames_partitioned'
        >>> parts = hh.get_partitions(schema='airflow', table_name=t)
        >>> len(parts)
        1
        >>> parts
        [{'ds': '2015-01-01'}]
        '''
        self.metastore._oprot.trans.open()
        table = self.metastore.get_table(dbname=schema, tbl_name=table_name)
        if len(table.partitionKeys) == 0:
            raise AirflowException("The table isn't partitioned")
        else:
            if filter:
                parts = self.metastore.get_partitions_by_filter(
                    db_name=schema, tbl_name=table_name,
                    filter=filter, max_parts=32767)
            else:
                parts = self.metastore.get_partitions(
                    db_name=schema, tbl_name=table_name, max_parts=32767)

            self.metastore._oprot.trans.close()
            pnames = [p.name for p in table.partitionKeys]
            return [dict(zip(pnames, p.values)) for p in parts]

    def max_partition(self, schema, table_name, field=None, filter=None):
        '''
        Returns the maximum value for all partitions in a table. Works only
        for tables that have a single partition key. For subpartitioned
        table, we recommend using signal tables.

        >>> hh = HiveMetastoreHook()
        >>> t = 'static_babynames_partitioned'
        >>> hh.max_partition(schema='airflow', table_name=t)
        '2015-01-01'
        '''
        parts = self.get_partitions(schema, table_name, filter)
        if not parts:
            return None
        elif len(parts[0]) == 1:
            field = parts[0].keys()[0]
        elif not field:
            raise AirflowException(
                "Please specify the field you want the max "
                "value for")

        return max([p[field] for p in parts])


class HiveServer2Hook(BaseHook):
    '''
    Wrapper around the pyhs2 lib
    '''
    def __init__(self, hiveserver2_conn_id='hiveserver2_default'):
        self.hiveserver2_conn = self.get_connection(hiveserver2_conn_id)

    def get_results(self, hql, schema='default', arraysize=1000):
        schema = schema or 'default'
        with pyhs2.connect(
                host=self.hiveserver2_conn.host,
                port=self.hiveserver2_conn.port,
                authMechanism="NOSASL",
                user='airflow',
                database=schema) as conn:
            with conn.cursor() as cur:
                cur.execute(hql)
                results = cur.fetchall()
                if results:
                    return {
                        'data': results,
                        'header': cur.getSchema(),
                    }
                else:
                    return {
                        'data': [],
                        'header': [],
                    }

    def to_csv(self, hql, csv_filepath, schema='default'):
        schema = schema or 'default'
        with pyhs2.connect(
                host=self.hiveserver2_conn.host,
                port=self.hiveserver2_conn.port,
                authMechanism="NOSASL",
                user='airflow',
                database=schema) as conn:
            with conn.cursor() as cur:
                logging.info("Running query: " + hql)
                cur.execute(hql)
                schema = cur.getSchema()
                with open(csv_filepath, 'w') as f:
                    writer = csv.writer(f)
                    writer.writerow([c['columnName'] for c in cur.getSchema()])
                    i = 0
                    while cur.hasMoreRows:
                        rows = [row for row in cur.fetchmany() if row]
                        writer.writerows(rows)
                        i += len(rows)
                        logging.info("Written {0} rows so far.".format(i))
                    logging.info("Done. Loaded a total of {0} rows.".format(i))

    def get_records(self, hql, schema='default'):
        '''
        Get a set of records from a Hive query.

        >>> hh = HiveServer2Hook()
        >>> sql = "SELECT * FROM airflow.static_babynames LIMIT 100"
        >>> len(hh.get_records(sql))
        100
        '''
        return self.get_results(hql, schema=schema)['data']

    def get_pandas_df(self, hql, schema='default'):
        '''
        Get a pandas dataframe from a Hive query

        >>> hh = HiveServer2Hook()
        >>> sql = "SELECT * FROM airflow.static_babynames LIMIT 100"
        >>> df = hh.get_pandas_df(sql)
        >>> len(df.index)
        100
        '''
        import pandas as pd
        res = self.get_results(hql, schema=schema)
        df = pd.DataFrame(res['data'])
        df.columns = [c['columnName'] for c in res['header']]
        return df

    def run(self, hql, schema=None):
        self.hive._oprot.trans.open()
        if schema:
            self.hive.execute("USE " + schema)
        self.hive.execute(hql)
        self.hive._oprot.trans.close()
