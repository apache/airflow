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

try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO
import logging

from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


COPY_SQL = ("COPY {schema}.{table} {column_names}"
            "FROM 's3://{s3_bucket}/{s3_key}' "
            "CREDENTIALS 'aws_access_key_id={aws_access_key};"
            "aws_secret_access_key={aws_secret_access_key}' {options}")


class S3ToRedshiftCopy(BaseOperator):
    """Runs a COPY command to load data from S3 to Redshift

    :param s3_bucket: S3 bucket in which the files are stored
    :type s3_bucket: string
    :param s3_key: S3 key where the CSV is stored
    :type s3_key: string
    :param redshift_conn_id: Redshift connection ID
    :type redshift_conn_id: string
    :param schema: Schema in Redshift where the data will be loaded
    :type schema: string
    :param table: Table in redshift where the data will be loaded
    :type table: string
    :param s3_conn_id: S3 connection id
    :type s3_conn_id: string
    :param infer_column_names: If set to true, it is assumed that the data
        file is a CSV and the first row of the CSV will be used verbatim as
        the column names for the COPY command. Defaults to False.
    :type infer_column_names: boolean
    :param column_names: Column ordering to provide to the COPY command.
        Note: If column_names is provided, infer_column_names must
        be False.
    :type column_names: list of strings
    :param copy_options: Extra options to pass to the COPY command, for example
        DELIMITER, CSV, BLANKSASNULL, etc.
    :type copy_options: A dictionary of options where the key is the option
        name. If the option is a flag, pass either an empty string or None
        as the value.
    :param autocommit: Whether or not to autocommit the transaction. Defaults
        to True.
    :type autocommit: boolean
    """

    template_fields = ()
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            s3_bucket,
            s3_key,
            schema,
            table,
            redshift_conn_id='redshift_default',
            s3_conn_id='s3_default',
            infer_column_names=False,
            copy_options=None,
            column_names=None,
            autocommit=True,
            *args,
            **kwargs):
        super(S3ToRedshiftCopy, self).__init__(*args, **kwargs)
        self.autocommit = autocommit
        self.column_names = column_names
        self.copy_options = {} if copy_options is None else copy_options
        self.infer_column_names = infer_column_names
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_conn_id = s3_conn_id
        self.s3_key = s3_key
        self.schema = schema
        self.table = table

        if self.column_names is not None and self.infer_column_names:
            raise AirflowException(
                'column_names and infer_column_names are mutually exclusive '
                'options.')

    def execute(self, context):
        pg_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        s3_hook = S3Hook(s3_conn_id=self.s3_conn_id)
        aws_access_key, aws_secret_access_key = s3_hook.get_credentials()

        if not s3_hook.check_for_key(self.s3_key, bucket_name=self.s3_bucket):
            raise AirflowException(
                'Key %s does not exist in bucket %s', self.s3_key,
                self.s3_bucket)

        # Get column names if available
        column_names = None
        if self.infer_column_names:
            logging.info(
                'Reading source S3 file %s from bucket %s to infer column '
                'names', self.s3_key, self.s3_bucket)
            key = s3_hook.get_key(self.s3_key, bucket_name=self.s3_bucket)
            buf = StringIO()
            key.get_contents_to_file(buf)
            buf.seek(0)
            column_names = buf.readline()
            # We can assume that since we're inferring column names from the
            # first row, we should ignore the header when copying data
            if self.copy_options.get('IGNOREHEADER') is None:
                self.copy_options['IGNOREHEADER'] = 1
        elif self.column_names is not None:
            column_names = ','.join(self.column_names)

        options = []
        for option, value in self.copy_options.items():
            if value is None or value == '':
                options.append(option)
            else:
                options.append("{} {}".format(option, value))
        options_string = ' '.join(options)

        copy_params = {
            'aws_access_key': aws_access_key,
            'aws_secret_access_key': aws_secret_access_key,
            'column_names': '({}) '.format(column_names)
                            if column_names is not None else '',
            'options': options_string,
            's3_bucket': self.s3_bucket,
            's3_key': self.s3_key,
            'schema': self.schema,
            'table': self.table,
        }
        logging.info(
            'Staring COPY from s3://%s/%s to redshift table %s.%s',
            self.s3_bucket, self.s3_key, self.schema, self.table)
        pg_hook.run(COPY_SQL.format(**copy_params), self.autocommit)
        logging.info(
            'Done executing COPY from s3://%s/%s to redshift table %s.%s',
            self.s3_bucket, self.s3_key, self.schema, self.table)
