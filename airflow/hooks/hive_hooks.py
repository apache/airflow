# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import print_function, unicode_literals

import contextlib
import os
import re
import subprocess
import time
from collections import OrderedDict
from tempfile import NamedTemporaryFile

import hmsclient
import six
import unicodecsv as csv
from past.builtins import basestring
from past.builtins import unicode
from six.moves import zip

from airflow import configuration
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.security import utils
from airflow.utils.file import TemporaryDirectory
from airflow.utils.helpers import as_flattened_list
from airflow.utils.operator_helpers import AIRFLOW_VAR_NAME_FORMAT_MAPPING

HIVE_QUEUE_PRIORITIES = ['VERY_HIGH', 'HIGH', 'NORMAL', 'LOW', 'VERY_LOW']


def get_context_from_env_var():
    """
    Extract context from env variable, e.g. dag_id, task_id and execution_date,
    so that they can be used inside BashOperator and PythonOperator.
    :return: The context of interest.
    """
    return {format_map['default']: os.environ.get(format_map['env_var_format'], '')
            for format_map in AIRFLOW_VAR_NAME_FORMAT_MAPPING.values()}


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
    :type  mapred_queue: str
    :param mapred_queue_priority: priority within the job queue.
        Possible settings include: VERY_HIGH, HIGH, NORMAL, LOW, VERY_LOW
    :type  mapred_queue_priority: str
    :param mapred_job_name: This name will appear in the jobtracker.
        This can make monitoring easier.
    :type  mapred_job_name: str
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

        self.mapred_queue = mapred_queue or configuration.get('hive',
                                                              'default_hive_mapred_queue')
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
            if configuration.conf.get('core', 'security') == 'kerberos':
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
            jdbc_url = '"{}"'.format(jdbc_url)

            cmd_extra += ['-u', jdbc_url]
            if conn.login:
                cmd_extra += ['-n', conn.login]
            if conn.password:
                cmd_extra += ['-p', conn.password]

        hive_params_list = self.hive_cli_params.split()

        return [hive_bin] + cmd_extra + hive_params_list

    @staticmethod
    def _prepare_hiveconf(d):
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
            zip(["-hiveconf"] * len(d),
                ["{}={}".format(k, v) for k, v in d.items()])
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
                hql = hql + '\n'
                f.write(hql.encode('UTF-8'))
                f.flush()
                hive_cmd = self._prepare_cli_cmd()
                env_context = get_context_from_env_var()
                # Only extend the hive_conf if it is defined.
                if hive_conf:
                    env_context.update(hive_conf)
                hive_conf_params = self._prepare_hiveconf(env_context)
                if self.mapred_queue:
                    hive_conf_params.extend(
                        ['-hiveconf',
                         'mapreduce.job.queuename={}'
                         .format(self.mapred_queue),
                         '-hiveconf',
                         'mapred.job.queue.name={}'
                         .format(self.mapred_queue),
                         '-hiveconf',
                         'tez.job.queue.name={}'
                         .format(self.mapred_queue)
                         ])

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
                    self.log.info(" ".join(hive_cmd))
                sp = subprocess.Popen(
                    hive_cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    cwd=tmp_dir,
                    close_fds=True)
                self.sp = sp
                stdout = ''
                while True:
                    line = sp.stdout.readline()
                    if not line:
                        break
                    stdout += line.decode('UTF-8')
                    if verbose:
                        self.log.info(line.decode('UTF-8').strip())
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
                self.log.info("Testing HQL [%s (...)]", query_preview)
                if query_set == insert:
                    query = other + '; explain ' + query
                else:
                    query = 'explain ' + query
                try:
                    self.run_cli(query, verbose=False)
                except AirflowException as e:
                    message = e.args[0].split('\n')[-2]
                    self.log.info(message)
                    error_loc = re.search('(\d+):(\d+)', message)
                    if error_loc and error_loc.group(1).isdigit():
                        lst = int(error_loc.group(1))
                        begin = max(lst - 2, 0)
                        end = min(lst + 3, len(query.split('\n')))
                        context = '\n'.join(query.split('\n')[begin:end])
                        self.log.info("Context :\n %s", context)
                else:
                    self.log.info("SUCCESS")

    def load_df(
            self,
            df,
            table,
            field_dict=None,
            delimiter=',',
            encoding='utf8',
            pandas_kwargs=None, **kwargs):
        """
        Loads a pandas DataFrame into hive.

        Hive data types will be inferred if not passed but column names will
        not be sanitized.

        :param df: DataFrame to load into a Hive table
        :type df: DataFrame
        :param table: target Hive table, use dot notation to target a
            specific database
        :type table: str
        :param field_dict: mapping from column name to hive data type.
            Note that it must be OrderedDict so as to keep columns' order.
        :type field_dict: OrderedDict
        :param delimiter: field delimiter in the file
        :type delimiter: str
        :param encoding: str encoding to use when writing DataFrame to file
        :type encoding: str
        :param pandas_kwargs: passed to DataFrame.to_csv
        :type pandas_kwargs: dict
        :param kwargs: passed to self.load_file
        """

        def _infer_field_types_from_df(df):
            DTYPE_KIND_HIVE_TYPE = {
                'b': 'BOOLEAN',    # boolean
                'i': 'BIGINT',     # signed integer
                'u': 'BIGINT',     # unsigned integer
                'f': 'DOUBLE',     # floating-point
                'c': 'STRING',     # complex floating-point
                'M': 'TIMESTAMP',  # datetime
                'O': 'STRING',     # object
                'S': 'STRING',     # (byte-)string
                'U': 'STRING',     # Unicode
                'V': 'STRING'      # void
            }

            d = OrderedDict()
            for col, dtype in df.dtypes.iteritems():
                d[col] = DTYPE_KIND_HIVE_TYPE[dtype.kind]
            return d

        if pandas_kwargs is None:
            pandas_kwargs = {}

        with TemporaryDirectory(prefix='airflow_hiveop_') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, mode="w") as f:

                if field_dict is None:
                    field_dict = _infer_field_types_from_df(df)

                df.to_csv(path_or_buf=f,
                          sep=(delimiter.encode(encoding)
                               if six.PY2 and isinstance(delimiter, unicode)
                               else delimiter),
                          header=False,
                          index=False,
                          encoding=encoding,
                          date_format="%Y-%m-%d %H:%M:%S",
                          **pandas_kwargs)
                f.flush()

                return self.load_file(filepath=f.name,
                                      table=table,
                                      delimiter=delimiter,
                                      field_dict=field_dict,
                                      **kwargs)

    def load_file(
            self,
            filepath,
            table,
            delimiter=",",
            field_dict=None,
            create=True,
            overwrite=True,
            partition=None,
            recreate=False,
            tblproperties=None):
        """
        Loads a local file into Hive

        Note that the table generated in Hive uses ``STORED AS textfile``
        which isn't the most efficient serialization format. If a
        large amount of data is loaded and/or if the tables gets
        queried considerably, you may want to use this operator only to
        stage the data into a temporary table before loading it into its
        final destination using a ``HiveOperator``.

        :param filepath: local filepath of the file to load
        :type filepath: str
        :param table: target Hive table, use dot notation to target a
            specific database
        :type table: str
        :param delimiter: field delimiter in the file
        :type delimiter: str
        :param field_dict: A dictionary of the fields name in the file
            as keys and their Hive types as values.
            Note that it must be OrderedDict so as to keep columns' order.
        :type field_dict: OrderedDict
        :param create: whether to create the table if it doesn't exist
        :type create: bool
        :param overwrite: whether to overwrite the data in table or partition
        :type overwrite: bool
        :param partition: target partition as a dict of partition columns
            and values
        :type partition: dict
        :param recreate: whether to drop and recreate the table at every
            execution
        :type recreate: bool
        :param tblproperties: TBLPROPERTIES of the hive table being created
        :type tblproperties: dict
        """
        hql = ''
        if recreate:
            hql += "DROP TABLE IF EXISTS {table};\n"
        if create or recreate:
            if field_dict is None:
                raise ValueError("Must provide a field dict when creating a table")
            fields = ",\n    ".join(
                [k + ' ' + v for k, v in field_dict.items()])
            hql += "CREATE TABLE IF NOT EXISTS {table} (\n{fields})\n"
            if partition:
                pfields = ",\n    ".join(
                    [p + " STRING" for p in partition])
                hql += "PARTITIONED BY ({pfields})\n"
            hql += "ROW FORMAT DELIMITED\n"
            hql += "FIELDS TERMINATED BY '{delimiter}'\n"
            hql += "STORED AS textfile\n"
            if tblproperties is not None:
                tprops = ", ".join(
                    ["'{0}'='{1}'".format(k, v) for k, v in tblproperties.items()])
                hql += "TBLPROPERTIES({tprops})\n"
        hql += ";"
        hql = hql.format(**locals())
        self.log.info(hql)
        self.run_cli(hql)
        hql = "LOAD DATA LOCAL INPATH '{filepath}' "
        if overwrite:
            hql += "OVERWRITE "
        hql += "INTO TABLE {table} "
        if partition:
            pvals = ", ".join(
                ["{0}='{1}'".format(k, v) for k, v in partition.items()])
            hql += "PARTITION ({pvals});"

        # As a workaround for HIVE-10541, add a newline character
        # at the end of hql (AIRFLOW-2412).
        hql += '\n'

        hql = hql.format(**locals())
        self.log.info(hql)
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

    # java short max val
    MAX_PART_COUNT = 32767

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
        ms = self.metastore_conn
        auth_mechanism = ms.extra_dejson.get('authMechanism', 'NOSASL')
        if configuration.conf.get('core', 'security') == 'kerberos':
            auth_mechanism = ms.extra_dejson.get('authMechanism', 'GSSAPI')
            kerberos_service_name = ms.extra_dejson.get('kerberos_service_name', 'hive')

        socket = TSocket.TSocket(ms.host, ms.port)
        if configuration.conf.get('core', 'security') == 'kerberos' \
                and auth_mechanism == 'GSSAPI':
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

        return hmsclient.HMSClient(iprot=protocol)

    def get_conn(self):
        return self.metastore

    def check_for_partition(self, schema, table, partition):
        """
        Checks whether a partition exists

        :param schema: Name of hive schema (database) @table belongs to
        :type schema: str
        :param table: Name of hive table @partition belongs to
        :type schema: str
        :partition: Expression that matches the partitions to check for
            (eg `a = 'b' AND c = 'd'`)
        :type schema: str
        :rtype: bool

        >>> hh = HiveMetastoreHook()
        >>> t = 'static_babynames_partitioned'
        >>> hh.check_for_partition('airflow', t, "ds='2015-01-01'")
        True
        """
        with self.metastore as client:
            partitions = client.get_partitions_by_filter(
                schema, table, partition, 1)

        if partitions:
            return True
        else:
            return False

    def check_for_named_partition(self, schema, table, partition_name):
        """
        Checks whether a partition with a given name exists

        :param schema: Name of hive schema (database) @table belongs to
        :type schema: str
        :param table: Name of hive table @partition belongs to
        :type schema: str
        :partition: Name of the partitions to check for (eg `a=b/c=d`)
        :type schema: str
        :rtype: bool

        >>> hh = HiveMetastoreHook()
        >>> t = 'static_babynames_partitioned'
        >>> hh.check_for_named_partition('airflow', t, "ds=2015-01-01")
        True
        >>> hh.check_for_named_partition('airflow', t, "ds=xxx")
        False
        """
        with self.metastore as client:
            return client.check_for_named_partition(schema, table, partition_name)

    def get_table(self, table_name, db='default'):
        """Get a metastore table object

        >>> hh = HiveMetastoreHook()
        >>> t = hh.get_table(db='airflow', table_name='static_babynames')
        >>> t.tableName
        'static_babynames'
        >>> [col.name for col in t.sd.cols]
        ['state', 'year', 'name', 'gender', 'num']
        """
        if db == 'default' and '.' in table_name:
            db, table_name = table_name.split('.')[:2]
        with self.metastore as client:
            return client.get_table(dbname=db, tbl_name=table_name)

    def get_tables(self, db, pattern='*'):
        """
        Get a metastore table object
        """
        with self.metastore as client:
            tables = client.get_tables(db_name=db, pattern=pattern)
            return client.get_table_objects_by_name(db, tables)

    def get_databases(self, pattern='*'):
        """
        Get a metastore table object
        """
        with self.metastore as client:
            return client.get_databases(pattern)

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
        with self.metastore as client:
            table = client.get_table(dbname=schema, tbl_name=table_name)
            if len(table.partitionKeys) == 0:
                raise AirflowException("The table isn't partitioned")
            else:
                if filter:
                    parts = client.get_partitions_by_filter(
                        db_name=schema, tbl_name=table_name,
                        filter=filter, max_parts=HiveMetastoreHook.MAX_PART_COUNT)
                else:
                    parts = client.get_partitions(
                        db_name=schema, tbl_name=table_name,
                        max_parts=HiveMetastoreHook.MAX_PART_COUNT)

                pnames = [p.name for p in table.partitionKeys]
                return [dict(zip(pnames, p.values)) for p in parts]

    @staticmethod
    def _get_max_partition_from_part_specs(part_specs, partition_key, filter_map):
        """
        Helper method to get max partition of partitions with partition_key
        from part specs. key:value pair in filter_map will be used to
        filter out partitions.

        :param part_specs: list of partition specs.
        :type part_specs: list
        :param partition_key: partition key name.
        :type partition_key: str
        :param filter_map: partition_key:partition_value map used for partition filtering,
                           e.g. {'key1': 'value1', 'key2': 'value2'}.
                           Only partitions matching all partition_key:partition_value
                           pairs will be considered as candidates of max partition.
        :type filter_map: map
        :return: Max partition or None if part_specs is empty.
        """
        if not part_specs:
            return None

        # Assuming all specs have the same keys.
        if partition_key not in part_specs[0].keys():
            raise AirflowException("Provided partition_key {} "
                                   "is not in part_specs.".format(partition_key))
        if filter_map:
            is_subset = set(filter_map.keys()).issubset(set(part_specs[0].keys()))
        if filter_map and not is_subset:
            raise AirflowException("Keys in provided filter_map {} "
                                   "are not subset of part_spec keys: {}"
                                   .format(', '.join(filter_map.keys()),
                                           ', '.join(part_specs[0].keys())))

        candidates = [p_dict[partition_key] for p_dict in part_specs
                      if filter_map is None or
                      all(item in p_dict.items() for item in filter_map.items())]

        if not candidates:
            return None
        else:
            return max(candidates).encode('utf-8')

    def max_partition(self, schema, table_name, field=None, filter_map=None):
        """
        Returns the maximum value for all partitions with given field in a table.
        If only one partition key exist in the table, the key will be used as field.
        filter_map should be a partition_key:partition_value map and will be used to
        filter out partitions.

        :param schema: schema name.
        :type schema: str
        :param table_name: table name.
        :type table_name: str
        :param field: partition key to get max partition from.
        :type field: str
        :param filter_map: partition_key:partition_value map used for partition filtering.
        :type filter_map: map

        >>> hh = HiveMetastoreHook()
        >>> filter_map = {'ds': '2015-01-01', 'ds': '2014-01-01'}
        >>> t = 'static_babynames_partitioned'
        >>> hh.max_partition(schema='airflow',\
        ... table_name=t, field='ds', filter_map=filter_map)
        '2015-01-01'
        """
        with self.metastore as client:
            table = client.get_table(dbname=schema, tbl_name=table_name)
            key_name_set = set(key.name for key in table.partitionKeys)
            if len(table.partitionKeys) == 1:
                field = table.partitionKeys[0].name
            elif not field:
                raise AirflowException("Please specify the field you want the max "
                                       "value for.")
            elif field not in key_name_set:
                raise AirflowException("Provided field is not a partition key.")

            if filter_map and not set(filter_map.keys()).issubset(key_name_set):
                raise AirflowException("Provided filter_map contains keys "
                                       "that are not partition key.")

            part_names = \
                client.get_partition_names(schema,
                                           table_name,
                                           max_parts=HiveMetastoreHook.MAX_PART_COUNT)
            part_specs = [client.partition_name_to_spec(part_name)
                          for part_name in part_names]

        return HiveMetastoreHook._get_max_partition_from_part_specs(part_specs,
                                                                    field,
                                                                    filter_map)

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
            self.get_table(table_name, db)
            return True
        except Exception:
            return False


class HiveServer2Hook(BaseHook):
    """
    Wrapper around the pyhive library

    Note that the default authMechanism is PLAIN, to override it you
    can specify it in the ``extra`` of your connection in the UI as in
    """
    def __init__(self, hiveserver2_conn_id='hiveserver2_default'):
        self.hiveserver2_conn_id = hiveserver2_conn_id

    def get_conn(self, schema=None):
        db = self.get_connection(self.hiveserver2_conn_id)
        auth_mechanism = db.extra_dejson.get('authMechanism', 'NONE')
        if auth_mechanism == 'NONE' and db.login is None:
            # we need to give a username
            username = 'airflow'
        kerberos_service_name = None
        if configuration.conf.get('core', 'security') == 'kerberos':
            auth_mechanism = db.extra_dejson.get('authMechanism', 'KERBEROS')
            kerberos_service_name = db.extra_dejson.get('kerberos_service_name', 'hive')

        # pyhive uses GSSAPI instead of KERBEROS as a auth_mechanism identifier
        if auth_mechanism == 'GSSAPI':
            self.log.warning(
                "Detected deprecated 'GSSAPI' for authMechanism "
                "for %s. Please use 'KERBEROS' instead",
                self.hiveserver2_conn_id
            )
            auth_mechanism = 'KERBEROS'

        from pyhive.hive import connect
        return connect(
            host=db.host,
            port=db.port,
            auth=auth_mechanism,
            kerberos_service_name=kerberos_service_name,
            username=db.login or username,
            database=schema or db.schema or 'default')

    def _get_results(self, hql, schema='default', fetch_size=None, hive_conf=None):
        from pyhive.exc import ProgrammingError
        if isinstance(hql, basestring):
            hql = [hql]
        previous_description = None
        with contextlib.closing(self.get_conn(schema)) as conn, \
                contextlib.closing(conn.cursor()) as cur:
            cur.arraysize = fetch_size or 1000

            env_context = get_context_from_env_var()
            if hive_conf:
                env_context.update(hive_conf)
            for k, v in env_context.items():
                cur.execute("set {}={}".format(k, v))

            for statement in hql:
                cur.execute(statement)
                # we only get results of statements that returns
                lowered_statement = statement.lower().strip()
                if (lowered_statement.startswith('select') or
                    lowered_statement.startswith('with') or
                    (lowered_statement.startswith('set') and
                     '=' not in lowered_statement)):
                    description = [c for c in cur.description]
                    if previous_description and previous_description != description:
                        message = '''The statements are producing different descriptions:
                                     Current: {}
                                     Previous: {}'''.format(repr(description),
                                                            repr(previous_description))
                        raise ValueError(message)
                    elif not previous_description:
                        previous_description = description
                        yield description
                    try:
                        # DB API 2 raises when no results are returned
                        # we're silencing here as some statements in the list
                        # may be `SET` or DDL
                        for row in cur:
                            yield row
                    except ProgrammingError:
                        self.log.debug("get_results returned no records")

    def get_results(self, hql, schema='default', fetch_size=None, hive_conf=None):
        """
        Get results of the provided hql in target schema.
        :param hql: hql to be executed.
        :param schema: target schema, default to 'default'.
        :param fetch_size max size of result to fetch.
        :param hive_conf: hive_conf to execute alone with the hql.
        :return: results of hql execution.
        """
        results_iter = self._get_results(hql, schema,
                                         fetch_size=fetch_size, hive_conf=hive_conf)
        header = next(results_iter)
        results = {
            'data': list(results_iter),
            'header': header
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
            fetch_size=1000,
            hive_conf=None):
        """
        Execute hql in target schema and write results to a csv file.
        :param hql: hql to be executed.
        :param csv_filepath: filepath of csv to write results into.
        :param schema: target schema, , default to 'default'.
        :param delimiter: delimiter of the csv file.
        :param lineterminator: lineterminator of the csv file.
        :param output_header: header of the csv file.
        :param fetch_size: number of result rows to write into the csv file.
        :param hive_conf: hive_conf to execute alone with the hql.
        :return:
        """

        results_iter = self._get_results(hql, schema,
                                         fetch_size=fetch_size, hive_conf=hive_conf)
        header = next(results_iter)
        message = None

        with open(csv_filepath, 'wb') as f:
            writer = csv.writer(f,
                                delimiter=delimiter,
                                lineterminator=lineterminator,
                                encoding='utf-8')
            try:
                if output_header:
                    self.log.debug('Cursor description is %s', header)
                    writer.writerow([c[0] for c in header])

                for i, row in enumerate(results_iter):
                    writer.writerow(row)
                    if i % fetch_size == 0:
                        self.log.info("Written %s rows so far.", i)
            except ValueError as exception:
                message = str(exception)

        if message:
            # need to clean up the file first
            os.remove(csv_filepath)
            raise ValueError(message)

        self.log.info("Done. Loaded a total of %s rows.", i)

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
