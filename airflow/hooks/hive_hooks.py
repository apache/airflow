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

from __future__ import print_function
from builtins import zip
from past.builtins import basestring

import collections
import unicodecsv as csv
import itertools
import logging
import re
import subprocess
import time
from tempfile import NamedTemporaryFile
import hive_metastore

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.utils.helpers import as_flattened_list
from airflow.utils.file import TemporaryDirectory
from airflow import configuration
import airflow.security.utils as utils

HIVE_QUEUE_PRIORITIES = ['VERY_HIGH', 'HIGH', 'NORMAL', 'LOW', 'VERY_LOW']


class HiveCliHook(BaseHook):

    """Simple wrapper around the hive CLI.

    It also supports the ``beeline``
    a lighter CLI that runs JDBC and is replacing the heavier
    traditional CLI. To enable ``beeline``, set the use_beeline param in the
    extra field of your connection as in ``{ "use_beeline": true }``

    Note that you can also set default hive CLI parameters using the
    ``hive_cli_params`` to be used in your connection as in
    ``{"hive_cli_params": "-hiveconf mapred.job.tracker=some.jobtracker:444"}``
    Parameters passed here can be overridden by run_cli's hive_conf param

    The extra connection parameter ``auth`` gets passed as in the ``jdbc``
    connection string as is.

    :param mapred_queue: queue used by the Hadoop Scheduler (Capacity or Fair)
    :type  mapred_queue: string
    :param mapred_queue_priority: priority within the job queue.
        Possible settings include: VERY_HIGH, HIGH, NORMAL, LOW, VERY_LOW
    :type  mapred_queue_priority: string
    :param mapred_job_name: This name will appear in the jobtracker.
        This can make monitoring easier.
    :type  mapred_job_name: string
    """

    def __init__(
            self,
            hive_cli_conn_id="hive_cli_default",
            run_as=None,
            mapred_queue=None,
            mapred_queue_priority=None,
            mapred_job_name=None):
        conn = self.get_connection(hive_cli_conn_id)
        self.hive_cli_params = conn.extra_dejson.get('hive_cli_params', '')
        self.use_beeline = conn.extra_dejson.get('use_beeline', False)
        self.auth = conn.extra_dejson.get('auth', 'noSasl')
        self.conn = conn
        self.run_as = run_as

        if mapred_queue_priority:
            mapred_queue_priority = mapred_queue_priority.upper()
            if mapred_queue_priority not in HIVE_QUEUE_PRIORITIES:
                raise AirflowException(
                    "Invalid Mapred Queue Priority.  Valid values are: "
                    "{}".format(', '.join(HIVE_QUEUE_PRIORITIES)))

        self.mapred_queue = mapred_queue
        self.mapred_queue_priority = mapred_queue_priority
        self.mapred_job_name = mapred_job_name

    def _prepare_cli_cmd(self):
        """
        This function creates the command list from available information
        """
        conn = self.conn
        hive_bin = 'hive'
        cmd_extra = []

        if self.use_beeline:
            hive_bin = 'beeline'
            jdbc_url = "jdbc:hive2://{conn.host}:{conn.port}/{conn.schema}"
            if configuration.get('core', 'security') == 'kerberos':
                template = conn.extra_dejson.get(
                    'principal', "hive/_HOST@EXAMPLE.COM")
                if "_HOST" in template:
                    template = utils.replace_hostname_pattern(
                        utils.get_components(template))

                proxy_user = ""  # noqa
                if conn.extra_dejson.get('proxy_user') == "login" and conn.login:
                    proxy_user = "hive.server2.proxy.user={0}".format(conn.login)
                elif conn.extra_dejson.get('proxy_user') == "owner" and self.run_as:
                    proxy_user = "hive.server2.proxy.user={0}".format(self.run_as)

                jdbc_url += ";principal={template};{proxy_user}"
            elif self.auth:
                jdbc_url += ";auth=" + self.auth

            jdbc_url = jdbc_url.format(**locals())

            cmd_extra += ['-u', jdbc_url]
            if conn.login:
                cmd_extra += ['-n', conn.login]
            if conn.password:
                cmd_extra += ['-p', conn.password]

        hive_params_list = self.hive_cli_params.split()

        return [hive_bin] + cmd_extra + hive_params_list

    def _prepare_hiveconf(self, d):
        """
        This function prepares a list of hiveconf params
        from a dictionary of key value pairs.

        :param d:
        :type d: dict

        >>> hh = HiveCliHook()
        >>> hive_conf = {"hive.exec.dynamic.partition": "true",
        ... "hive.exec.dynamic.partition.mode": "nonstrict"}
        >>> hh._prepare_hiveconf(hive_conf)
        ["-hiveconf", "hive.exec.dynamic.partition=true",\
 "-hiveconf", "hive.exec.dynamic.partition.mode=nonstrict"]
        """
        if not d:
            return []
        return as_flattened_list(
            itertools.izip(
                ["-hiveconf"] * len(d),
                ["{}={}".format(k, v) for k, v in d.items()]
                )
            )

    def run_cli(self, hql, schema=None, verbose=True, hive_conf=None):
        """
        Run an hql statement using the hive cli. If hive_conf is specified
        it should be a dict and the entries will be set as key/value pairs
        in HiveConf


        :param hive_conf: if specified these key value pairs will be passed
            to hive as ``-hiveconf "key"="value"``. Note that they will be
            passed after the ``hive_cli_params`` and thus will override
            whatever values are specified in the database.
        :type hive_conf: dict

        >>> hh = HiveCliHook()
        >>> result = hh.run_cli("USE airflow;")
        >>> ("OK" in result)
        True
        """
        conn = self.conn
        schema = schema or conn.schema
        if schema:
            hql = "USE {schema};\n{hql}".format(**locals())

        with TemporaryDirectory(prefix='airflow_hiveop_') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir) as f:
                f.write(hql.encode('UTF-8'))
                f.flush()
                hive_cmd = self._prepare_cli_cmd()
                hive_conf_params = self._prepare_hiveconf(hive_conf)
                if self.mapred_queue:
                    hive_conf_params.extend(
                        ['-hiveconf',
                         'mapreduce.job.queuename={}'
                         .format(self.mapred_queue)])

                if self.mapred_queue_priority:
                    hive_conf_params.extend(
                        ['-hiveconf',
                         'mapreduce.job.priority={}'
                         .format(self.mapred_queue_priority)])

                if self.mapred_job_name:
                    hive_conf_params.extend(
                        ['-hiveconf',
                         'mapred.job.name={}'
                         .format(self.mapred_job_name)])

                hive_cmd.extend(hive_conf_params)
                hive_cmd.extend(['-f', f.name])

                if verbose:
                    logging.info(" ".join(hive_cmd))
                sp = subprocess.Popen(
                    hive_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    cwd=tmp_dir)
                self.sp = sp
                stdout = ''
                while True:
                    line = sp.stdout.readline()
                    if not line:
                        break
                    stdout += line.decode('UTF-8')
                    if verbose:
                        logging.info(line.decode('UTF-8').strip())
                sp.wait()

                if sp.returncode:
                    raise AirflowException(stdout)

                return stdout

    def test_hql(self, hql):
        """
        Test an hql statement using the hive cli and EXPLAIN

        """
        create, insert, other = [], [], []
        for query in hql.split(';'):  # naive
            query_original = query
            query = query.lower().strip()

            if query.startswith('create table'):
                create.append(query_original)
            elif query.startswith(('set ',
                                   'add jar ',
                                   'create temporary function')):
                other.append(query_original)
            elif query.startswith('insert'):
                insert.append(query_original)
        other = ';'.join(other)
        for query_set in [create, insert]:
            for query in query_set:

                query_preview = ' '.join(query.split())[:50]
                logging.info("Testing HQL [{0} (...)]".format(query_preview))
                if query_set == insert:
                    query = other + '; explain ' + query
                else:
                    query = 'explain ' + query
                try:
                    self.run_cli(query, verbose=False)
                except AirflowException as e:
                    message = e.args[0].split('\n')[-2]
                    logging.info(message)
                    error_loc = re.search('(\d+):(\d+)', message)
                    if error_loc and error_loc.group(1).isdigit():
                        l = int(error_loc.group(1))
                        begin = max(l-2, 0)
                        end = min(l+3, len(query.split('\n')))
                        context = '\n'.join(query.split('\n')[begin:end])
                        logging.info("Context :\n {0}".format(context))
                else:
                    logging.info("SUCCESS")

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
            hql += "CREATE TABLE IF NOT EXISTS {table} (\n{fields})\n"
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
                self.sp.terminate()
                time.sleep(60)
                self.sp.kill()


class HiveMetastoreHook(BaseHook):

    """ Wrapper to interact with the Hive Metastore"""

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
        """
        Returns a Hive thrift client.
        """
        from thrift.transport import TSocket, TTransport
        from thrift.protocol import TBinaryProtocol
        from hive_service import ThriftHive
        ms = self.metastore_conn
        auth_mechanism = ms.extra_dejson.get('authMechanism', 'NOSASL')
        if configuration.get('core', 'security') == 'kerberos':
            auth_mechanism = ms.extra_dejson.get('authMechanism', 'GSSAPI')
            kerberos_service_name = ms.extra_dejson.get('kerberos_service_name', 'hive')

        socket = TSocket.TSocket(ms.host, ms.port)
        if configuration.get('core', 'security') == 'kerberos' and auth_mechanism == 'GSSAPI':
            try:
                import saslwrapper as sasl
            except ImportError:
                import sasl

            def sasl_factory():
                sasl_client = sasl.Client()
                sasl_client.setAttr("host", ms.host)
                sasl_client.setAttr("service", kerberos_service_name)
                sasl_client.init()
                return sasl_client

            from thrift_sasl import TSaslClientTransport
            transport = TSaslClientTransport(sasl_factory, "GSSAPI", socket)
        else:
            transport = TTransport.TBufferedTransport(socket)

        protocol = TBinaryProtocol.TBinaryProtocol(transport)

        return ThriftHive.Client(protocol)

    def get_conn(self):
        return self.metastore

    def check_for_partition(self, schema, table, partition):
        """
        Checks whether a partition exists

        :param schema: Name of hive schema (database) @table belongs to
        :type schema: string
        :param table: Name of hive table @partition belongs to
        :type schema: string
        :partition: Expression that matches the partitions to check for
            (eg `a = 'b' AND c = 'd'`)
        :type schema: string
        :rtype: boolean

        >>> hh = HiveMetastoreHook()
        >>> t = 'static_babynames_partitioned'
        >>> hh.check_for_partition('airflow', t, "ds='2015-01-01'")
        True
        """
        self.metastore._oprot.trans.open()
        partitions = self.metastore.get_partitions_by_filter(
            schema, table, partition, 1)
        self.metastore._oprot.trans.close()
        if partitions:
            return True
        else:
            return False

    def check_for_named_partition(self, schema, table, partition_name):
        """
        Checks whether a partition with a given name exists

        :param schema: Name of hive schema (database) @table belongs to
        :type schema: string
        :param table: Name of hive table @partition belongs to
        :type schema: string
        :partition: Name of the partitions to check for (eg `a=b/c=d`)
        :type schema: string
        :rtype: boolean

        >>> hh = HiveMetastoreHook()
        >>> t = 'static_babynames_partitioned'
        >>> hh.check_for_named_partition('airflow', t, "ds=2015-01-01")
        True
        >>> hh.check_for_named_partition('airflow', t, "ds=xxx")
        False
        """
        self.metastore._oprot.trans.open()
        try:
            self.metastore.get_partition_by_name(
                schema, table, partition_name)
            return True
        except hive_metastore.ttypes.NoSuchObjectException:
            return False
        finally:
            self.metastore._oprot.trans.close()

    def get_table(self, table_name, db='default'):
        """Get a metastore table object

        >>> hh = HiveMetastoreHook()
        >>> t = hh.get_table(db='airflow', table_name='static_babynames')
        >>> t.tableName
        'static_babynames'
        >>> [col.name for col in t.sd.cols]
        ['state', 'year', 'name', 'gender', 'num']
        """
        self.metastore._oprot.trans.open()
        if db == 'default' and '.' in table_name:
            db, table_name = table_name.split('.')[:2]
        table = self.metastore.get_table(dbname=db, tbl_name=table_name)
        self.metastore._oprot.trans.close()
        return table

    def get_tables(self, db, pattern='*'):
        """
        Get a metastore table object
        """
        self.metastore._oprot.trans.open()
        tables = self.metastore.get_tables(db_name=db, pattern=pattern)
        objs = self.metastore.get_table_objects_by_name(db, tables)
        self.metastore._oprot.trans.close()
        return objs

    def get_databases(self, pattern='*'):
        """
        Get a metastore table object
        """
        self.metastore._oprot.trans.open()
        dbs = self.metastore.get_databases(pattern)
        self.metastore._oprot.trans.close()
        return dbs

    def get_partitions(
            self, schema, table_name, filter=None):
        """
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
        """
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
        """
        Returns the maximum value for all partitions in a table. Works only
        for tables that have a single partition key. For subpartitioned
        table, we recommend using signal tables.

        >>> hh = HiveMetastoreHook()
        >>> t = 'static_babynames_partitioned'
        >>> hh.max_partition(schema='airflow', table_name=t)
        '2015-01-01'
        """
        parts = self.get_partitions(schema, table_name, filter)
        if not parts:
            return None
        elif len(parts[0]) == 1:
            field = list(parts[0].keys())[0]
        elif not field:
            raise AirflowException(
                "Please specify the field you want the max "
                "value for")

        return max([p[field] for p in parts])

    def table_exists(self, table_name, db='default'):
        """
        Check if table exists

        >>> hh = HiveMetastoreHook()
        >>> hh.table_exists(db='airflow', table_name='static_babynames')
        True
        >>> hh.table_exists(db='airflow', table_name='does_not_exist')
        False
        """
        try:
            t = self.get_table(table_name, db)
            return True
        except Exception as e:
            return False


class HiveServer2Hook(BaseHook):
    """
    Wrapper around the impyla library

    Note that the default authMechanism is PLAIN, to override it you
    can specify it in the ``extra`` of your connection in the UI as in
    """
    def __init__(self, hiveserver2_conn_id='hiveserver2_default'):
        self.hiveserver2_conn_id = hiveserver2_conn_id

    def get_conn(self, schema=None):
        db = self.get_connection(self.hiveserver2_conn_id)
        auth_mechanism = db.extra_dejson.get('authMechanism', 'PLAIN')
        kerberos_service_name = None
        if configuration.get('core', 'security') == 'kerberos':
            auth_mechanism = db.extra_dejson.get('authMechanism', 'GSSAPI')
            kerberos_service_name = db.extra_dejson.get('kerberos_service_name', 'hive')

        # impyla uses GSSAPI instead of KERBEROS as a auth_mechanism identifier
        if auth_mechanism == 'KERBEROS':
            logging.warning("Detected deprecated 'KERBEROS' for authMechanism for %s. Please use 'GSSAPI' instead",
                            self.hiveserver2_conn_id)
            auth_mechanism = 'GSSAPI'

        from impala.dbapi import connect
        return connect(
            host=db.host,
            port=db.port,
            auth_mechanism=auth_mechanism,
            kerberos_service_name=kerberos_service_name,
            user=db.login,
            database=schema or db.schema or 'default')

    def get_results(self, hql, schema='default', arraysize=1000):
        from impala.error import ProgrammingError
        with self.get_conn(schema) as conn:
            if isinstance(hql, basestring):
                hql = [hql]
            results = {
                'data': [],
                'header': [],
            }
            cur = conn.cursor()
            for statement in hql:
                cur.execute(statement)
                records = []
                try:
                    # impala Lib raises when no results are returned
                    # we're silencing here as some statements in the list
                    # may be `SET` or DDL
                    records = cur.fetchall()
                except ProgrammingError:
                    logging.debug("get_results returned no records")
                if records:
                    results = {
                        'data': records,
                        'header': cur.description,
                    }
            return results

    def to_csv(
            self,
            hql,
            csv_filepath,
            schema='default',
            delimiter=',',
            lineterminator='\r\n',
            output_header=True,
            fetch_size=1000):
        schema = schema or 'default'
        with self.get_conn(schema) as conn:
            with conn.cursor() as cur:
                logging.info("Running query: " + hql)
                cur.execute(hql)
                schema = cur.description
                with open(csv_filepath, 'wb') as f:
                    writer = csv.writer(f,
                                        delimiter=delimiter,
                                        lineterminator=lineterminator,
                                        encoding='utf-8')
                    if output_header:
                        writer.writerow([c[0] for c in cur.description])
                    i = 0
                    while True:
                        rows = [row for row in cur.fetchmany(fetch_size) if row]
                        if not rows:
                            break

                        writer.writerows(rows)
                        i += len(rows)
                        logging.info("Written {0} rows so far.".format(i))
                    logging.info("Done. Loaded a total of {0} rows.".format(i))

    def get_records(self, hql, schema='default'):
        """
        Get a set of records from a Hive query.

        >>> hh = HiveServer2Hook()
        >>> sql = "SELECT * FROM airflow.static_babynames LIMIT 100"
        >>> len(hh.get_records(sql))
        100
        """
        return self.get_results(hql, schema=schema)['data']

    def get_pandas_df(self, hql, schema='default'):
        """
        Get a pandas dataframe from a Hive query

        >>> hh = HiveServer2Hook()
        >>> sql = "SELECT * FROM airflow.static_babynames LIMIT 100"
        >>> df = hh.get_pandas_df(sql)
        >>> len(df.index)
        100
        """
        import pandas as pd
        res = self.get_results(hql, schema=schema)
        df = pd.DataFrame(res['data'])
        df.columns = [c[0] for c in res['header']]
        return df
