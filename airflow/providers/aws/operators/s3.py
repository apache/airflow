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

import bz2
import gzip
import json
import os
import subprocess
import sys
import tempfile
import warnings
from tempfile import NamedTemporaryFile
from typing import Dict, Iterable, List, Optional, Union
from urllib.parse import urlparse

from bson import json_util

from airflow.contrib.hooks.imap_hook import ImapHook
from airflow.contrib.hooks.mongo_hook import MongoHook
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.exceptions import AirflowException
from airflow.gcp.hooks.gcs import GoogleCloudStorageHook, _parse_gcs_url
from airflow.hooks.hive_hooks import HiveCliHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.providers.aws.hooks.s3 import S3Hook
from airflow.utils.compression import uncompress_file
from airflow.utils.decorators import apply_defaults
from airflow.utils.file import TemporaryDirectory


class S3CopyObjectOperator(BaseOperator):
    """
    Creates a copy of an object that is already stored in S3.

    Note: the S3 connection used here needs to have access to both
    source and destination bucket/key.

    :param source_bucket_key: The key of the source object. (templated)

        It can be either full s3:// style url or relative path from root level.

        When it's specified as a full s3:// url, please omit source_bucket_name.
    :type source_bucket_key: str
    :param dest_bucket_key: The key of the object to copy to. (templated)

        The convention to specify `dest_bucket_key` is the same as `source_bucket_key`.
    :type dest_bucket_key: str
    :param source_bucket_name: Name of the S3 bucket where the source object is in. (templated)

        It should be omitted when `source_bucket_key` is provided as a full s3:// url.
    :type source_bucket_name: str
    :param dest_bucket_name: Name of the S3 bucket to where the object is copied. (templated)

        It should be omitted when `dest_bucket_key` is provided as a full s3:// url.
    :type dest_bucket_name: str
    :param source_version_id: Version ID of the source object (OPTIONAL)
    :type source_version_id: str
    :param aws_conn_id: Connection id of the S3 connection to use
    :type aws_conn_id: str
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.

        You can provide the following values:

        - False: do not validate SSL certificates. SSL will still be used,
                 but SSL certificates will not be
                 verified.
        - path/to/cert/bundle.pem: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :type verify: bool or str
    """

    template_fields = ('source_bucket_key', 'dest_bucket_key',
                       'source_bucket_name', 'dest_bucket_name')

    @apply_defaults
    def __init__(
            self,
            source_bucket_key,
            dest_bucket_key,
            source_bucket_name=None,
            dest_bucket_name=None,
            source_version_id=None,
            aws_conn_id='aws_default',
            verify=None,
            *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.source_bucket_key = source_bucket_key
        self.dest_bucket_key = dest_bucket_key
        self.source_bucket_name = source_bucket_name
        self.dest_bucket_name = dest_bucket_name
        self.source_version_id = source_version_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        s3_hook.copy_object(self.source_bucket_key, self.dest_bucket_key,
                            self.source_bucket_name, self.dest_bucket_name,
                            self.source_version_id)


class S3FileTransformOperator(BaseOperator):
    """
    Copies data from a source S3 location to a temporary location on the
    local filesystem. Runs a transformation on this file as specified by
    the transformation script and uploads the output to a destination S3
    location.

    The locations of the source and the destination files in the local
    filesystem is provided as an first and second arguments to the
    transformation script. The transformation script is expected to read the
    data from source, transform it and write the output to the local
    destination file. The operator then takes over control and uploads the
    local destination file to S3.

    S3 Select is also available to filter the source contents. Users can
    omit the transformation script if S3 Select expression is specified.

    :param source_s3_key: The key to be retrieved from S3. (templated)
    :type source_s3_key: str
    :param dest_s3_key: The key to be written from S3. (templated)
    :type dest_s3_key: str
    :param transform_script: location of the executable transformation script
    :type transform_script: str
    :param select_expression: S3 Select expression
    :type select_expression: str
    :param source_aws_conn_id: source s3 connection
    :type source_aws_conn_id: str
    :param source_verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
             (unless use_ssl is False), but SSL certificates will not be
             verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
             You can specify this argument if you want to use a different
             CA cert bundle than the one used by botocore.

        This is also applicable to ``dest_verify``.
    :type source_verify: bool or str
    :param dest_aws_conn_id: destination s3 connection
    :type dest_aws_conn_id: str
    :param dest_verify: Whether or not to verify SSL certificates for S3 connection.
        See: ``source_verify``
    :type dest_verify: bool or str
    :param replace: Replace dest S3 key if it already exists
    :type replace: bool
    """

    template_fields = ('source_s3_key', 'dest_s3_key')
    template_ext = ()
    ui_color = '#f9c915'

    @apply_defaults
    def __init__(
            self,
            source_s3_key: str,
            dest_s3_key: str,
            transform_script: Optional[str] = None,
            select_expression=None,
            source_aws_conn_id: str = 'aws_default',
            source_verify: Optional[Union[bool, str]] = None,
            dest_aws_conn_id: str = 'aws_default',
            dest_verify: Optional[Union[bool, str]] = None,
            replace: bool = False,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.source_s3_key = source_s3_key
        self.source_aws_conn_id = source_aws_conn_id
        self.source_verify = source_verify
        self.dest_s3_key = dest_s3_key
        self.dest_aws_conn_id = dest_aws_conn_id
        self.dest_verify = dest_verify
        self.replace = replace
        self.transform_script = transform_script
        self.select_expression = select_expression
        self.output_encoding = sys.getdefaultencoding()

    def execute(self, context):
        if self.transform_script is None and self.select_expression is None:
            raise AirflowException(
                "Either transform_script or select_expression must be specified")

        source_s3 = S3Hook(aws_conn_id=self.source_aws_conn_id,
                           verify=self.source_verify)
        dest_s3 = S3Hook(aws_conn_id=self.dest_aws_conn_id,
                         verify=self.dest_verify)

        self.log.info("Downloading source S3 file %s", self.source_s3_key)
        if not source_s3.check_for_key(self.source_s3_key):
            raise AirflowException(
                "The source key {0} does not exist".format(self.source_s3_key))
        source_s3_key_object = source_s3.get_key(self.source_s3_key)

        with NamedTemporaryFile("wb") as f_source, NamedTemporaryFile("wb") as f_dest:
            self.log.info(
                "Dumping S3 file %s contents to local file %s",
                self.source_s3_key, f_source.name
            )

            if self.select_expression is not None:
                content = source_s3.select_key(
                    key=self.source_s3_key,
                    expression=self.select_expression
                )
                f_source.write(content.encode("utf-8"))
            else:
                source_s3_key_object.download_fileobj(Fileobj=f_source)
            f_source.flush()

            if self.transform_script is not None:
                process = subprocess.Popen(
                    [self.transform_script, f_source.name, f_dest.name],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    close_fds=True
                )

                self.log.info("Output:")
                for line in iter(process.stdout.readline, b''):
                    self.log.info(line.decode(self.output_encoding).rstrip())

                process.wait()

                if process.returncode > 0:
                    raise AirflowException(
                        "Transform script failed: {0}".format(process.returncode)
                    )
                else:
                    self.log.info(
                        "Transform script successful. Output temporarily located at %s",
                        f_dest.name
                    )

            self.log.info("Uploading transformed file to S3")
            f_dest.flush()
            dest_s3.load_file(
                filename=f_dest.name,
                key=self.dest_s3_key,
                replace=self.replace
            )
            self.log.info("Upload successful")


class S3ToHiveTransfer(BaseOperator):
    """
    Moves data from S3 to Hive. The operator downloads a file from S3,
    stores the file locally before loading it into a Hive table.
    If the ``create`` or ``recreate`` arguments are set to ``True``,
    a ``CREATE TABLE`` and ``DROP TABLE`` statements are generated.
    Hive data types are inferred from the cursor's metadata from.

    Note that the table generated in Hive uses ``STORED AS textfile``
    which isn't the most efficient serialization format. If a
    large amount of data is loaded and/or if the tables gets
    queried considerably, you may want to use this operator only to
    stage the data into a temporary table before loading it into its
    final destination using a ``HiveOperator``.

    :param s3_key: The key to be retrieved from S3. (templated)
    :type s3_key: str
    :param field_dict: A dictionary of the fields name in the file
        as keys and their Hive types as values
    :type field_dict: dict
    :param hive_table: target Hive table, use dot notation to target a
        specific database. (templated)
    :type hive_table: str
    :param delimiter: field delimiter in the file
    :type delimiter: str
    :param create: whether to create the table if it doesn't exist
    :type create: bool
    :param recreate: whether to drop and recreate the table at every
        execution
    :type recreate: bool
    :param partition: target partition as a dict of partition columns
        and values. (templated)
    :type partition: dict
    :param headers: whether the file contains column names on the first
        line
    :type headers: bool
    :param check_headers: whether the column names on the first line should be
        checked against the keys of field_dict
    :type check_headers: bool
    :param wildcard_match: whether the s3_key should be interpreted as a Unix
        wildcard pattern
    :type wildcard_match: bool
    :param aws_conn_id: source s3 connection
    :type aws_conn_id: str
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                 (unless use_ssl is False), but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :type verify: bool or str
    :param hive_cli_conn_id: destination hive connection
    :type hive_cli_conn_id: str
    :param input_compressed: Boolean to determine if file decompression is
        required to process headers
    :type input_compressed: bool
    :param tblproperties: TBLPROPERTIES of the hive table being created
    :type tblproperties: dict
    :param select_expression: S3 Select expression
    :type select_expression: str
    """

    template_fields = ('s3_key', 'partition', 'hive_table')
    template_ext = ()
    ui_color = '#a0e08c'

    @apply_defaults
    def __init__(
            self,
            s3_key: str,
            field_dict: Dict,
            hive_table: str,
            delimiter: str = ',',
            create: bool = True,
            recreate: bool = False,
            partition: Optional[Dict] = None,
            headers: bool = False,
            check_headers: bool = False,
            wildcard_match: bool = False,
            aws_conn_id: str = 'aws_default',
            verify: Optional[Union[bool, str]] = None,
            hive_cli_conn_id: str = 'hive_cli_default',
            input_compressed: bool = False,
            tblproperties: Optional[Dict] = None,
            select_expression: Optional[str] = None,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.s3_key = s3_key
        self.field_dict = field_dict
        self.hive_table = hive_table
        self.delimiter = delimiter
        self.create = create
        self.recreate = recreate
        self.partition = partition
        self.headers = headers
        self.check_headers = check_headers
        self.wildcard_match = wildcard_match
        self.hive_cli_conn_id = hive_cli_conn_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.input_compressed = input_compressed
        self.tblproperties = tblproperties
        self.select_expression = select_expression

        if (self.check_headers and
                not (self.field_dict is not None and self.headers)):
            raise AirflowException("To check_headers provide " +
                                   "field_dict and headers")

    def execute(self, context):
        # Downloading file from S3
        self.s3 = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        self.hive = HiveCliHook(hive_cli_conn_id=self.hive_cli_conn_id)
        self.log.info("Downloading S3 file")

        if self.wildcard_match:
            if not self.s3.check_for_wildcard_key(self.s3_key):
                raise AirflowException("No key matches {0}"
                                       .format(self.s3_key))
            s3_key_object = self.s3.get_wildcard_key(self.s3_key)
        else:
            if not self.s3.check_for_key(self.s3_key):
                raise AirflowException(
                    "The key {0} does not exists".format(self.s3_key))
            s3_key_object = self.s3.get_key(self.s3_key)

        _, file_ext = os.path.splitext(s3_key_object.key)
        if (self.select_expression and self.input_compressed and
                file_ext.lower() != '.gz'):
            raise AirflowException("GZIP is the only compression " +
                                   "format Amazon S3 Select supports")

        with TemporaryDirectory(prefix='tmps32hive_') as tmp_dir,\
                NamedTemporaryFile(mode="wb",
                                   dir=tmp_dir,
                                   suffix=file_ext) as f:
            self.log.info(
                "Dumping S3 key %s contents to local file %s", s3_key_object.key, f.name
            )
            if self.select_expression:
                option = {}
                if self.headers:
                    option['FileHeaderInfo'] = 'USE'
                if self.delimiter:
                    option['FieldDelimiter'] = self.delimiter

                input_serialization = {'CSV': option}
                if self.input_compressed:
                    input_serialization['CompressionType'] = 'GZIP'

                content = self.s3.select_key(
                    bucket_name=s3_key_object.bucket_name,
                    key=s3_key_object.key,
                    expression=self.select_expression,
                    input_serialization=input_serialization
                )
                f.write(content.encode("utf-8"))
            else:
                s3_key_object.download_fileobj(f)
            f.flush()

            if self.select_expression or not self.headers:
                self.log.info("Loading file %s into Hive", f.name)
                self.hive.load_file(
                    f.name,
                    self.hive_table,
                    field_dict=self.field_dict,
                    create=self.create,
                    partition=self.partition,
                    delimiter=self.delimiter,
                    recreate=self.recreate,
                    tblproperties=self.tblproperties)
            else:
                # Decompressing file
                if self.input_compressed:
                    self.log.info("Uncompressing file %s", f.name)
                    fn_uncompressed = uncompress_file(f.name,
                                                      file_ext,
                                                      tmp_dir)
                    self.log.info("Uncompressed to %s", fn_uncompressed)
                    # uncompressed file available now so deleting
                    # compressed file to save disk space
                    f.close()
                else:
                    fn_uncompressed = f.name

                # Testing if header matches field_dict
                if self.check_headers:
                    self.log.info("Matching file header against field_dict")
                    header_list = self._get_top_row_as_list(fn_uncompressed)
                    if not self._match_headers(header_list):
                        raise AirflowException("Header check failed")

                # Deleting top header row
                self.log.info("Removing header from file %s", fn_uncompressed)
                headless_file = (
                    self._delete_top_row_and_compress(fn_uncompressed,
                                                      file_ext,
                                                      tmp_dir))
                self.log.info("Headless file %s", headless_file)
                self.log.info("Loading file %s into Hive", headless_file)
                self.hive.load_file(headless_file,
                                    self.hive_table,
                                    field_dict=self.field_dict,
                                    create=self.create,
                                    partition=self.partition,
                                    delimiter=self.delimiter,
                                    recreate=self.recreate,
                                    tblproperties=self.tblproperties)

    def _get_top_row_as_list(self, file_name):
        with open(file_name, 'rt') as file:
            header_line = file.readline().strip()
            header_list = header_line.split(self.delimiter)
            return header_list

    def _match_headers(self, header_list):
        if not header_list:
            raise AirflowException("Unable to retrieve header row from file")
        field_names = self.field_dict.keys()
        if len(field_names) != len(header_list):
            self.log.warning(
                "Headers count mismatch File headers:\n %s\nField names: \n %s\n", header_list, field_names
            )
            return False
        test_field_match = [h1.lower() == h2.lower()
                            for h1, h2 in zip(header_list, field_names)]
        if not all(test_field_match):
            self.log.warning(
                "Headers do not match field names File headers:\n %s\nField names: \n %s\n",
                header_list, field_names
            )
            return False
        else:
            return True

    @staticmethod
    def _delete_top_row_and_compress(
            input_file_name,
            output_file_ext,
            dest_dir):
        # When output_file_ext is not defined, file is not compressed
        open_fn = open
        if output_file_ext.lower() == '.gz':
            open_fn = gzip.GzipFile
        elif output_file_ext.lower() == '.bz2':
            open_fn = bz2.BZ2File

        _, fn_output = tempfile.mkstemp(suffix=output_file_ext, dir=dest_dir)
        with open(input_file_name, 'rb') as f_in, open_fn(fn_output, 'wb') as f_out:
            f_in.seek(0)
            next(f_in)
            for line in f_in:
                f_out.write(line)
        return fn_output


class S3ToRedshiftTransfer(BaseOperator):
    """
    Executes an COPY command to load files from s3 to Redshift

    :param schema: reference to a specific schema in redshift database
    :type schema: str
    :param table: reference to a specific table in redshift database
    :type table: str
    :param s3_bucket: reference to a specific S3 bucket
    :type s3_bucket: str
    :param s3_key: reference to a specific S3 key
    :type s3_key: str
    :param redshift_conn_id: reference to a specific redshift database
    :type redshift_conn_id: str
    :param aws_conn_id: reference to a specific S3 connection
    :type aws_conn_id: str
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                 (unless use_ssl is False), but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :type verify: bool or str
    :param copy_options: reference to a list of COPY options
    :type copy_options: list
    """

    template_fields = ()
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            schema: str,
            table: str,
            s3_bucket: str,
            s3_key: str,
            redshift_conn_id: str = 'redshift_default',
            aws_conn_id: str = 'aws_default',
            verify: Optional[Union[bool, str]] = None,
            copy_options: Optional[List] = None,
            autocommit: bool = False,
            *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.copy_options = copy_options or []
        self.autocommit = autocommit

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.s3 = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        credentials = self.s3.get_credentials()
        copy_options = '\n\t\t\t'.join(self.copy_options)

        copy_query = """
            COPY {schema}.{table}
            FROM 's3://{s3_bucket}/{s3_key}/{table}'
            with credentials
            'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
            {copy_options};
        """.format(schema=self.schema,
                   table=self.table,
                   s3_bucket=self.s3_bucket,
                   s3_key=self.s3_key,
                   access_key=credentials.access_key,
                   secret_key=credentials.secret_key,
                   copy_options=copy_options)

        self.log.info('Executing COPY command...')
        self.hook.run(copy_query, self.autocommit)
        self.log.info("COPY command complete...")


class ImapAttachmentToS3Operator(BaseOperator):
    """
    Transfers a mail attachment from a mail server into s3 bucket.

    :param imap_attachment_name: The file name of the mail attachment that you want to transfer.
    :type imap_attachment_name: str
    :param s3_key: The destination file name in the s3 bucket for the attachment.
    :type s3_key: str
    :param imap_check_regex: If set checks the `imap_attachment_name` for a regular expression.
    :type imap_check_regex: bool
    :param imap_mail_folder: The folder on the mail server to look for the attachment.
    :type imap_mail_folder: str
    :param imap_mail_filter: If set other than 'All' only specific mails will be checked.
        See :py:meth:`imaplib.IMAP4.search` for details.
    :type imap_mail_filter: str
    :param s3_overwrite: If set overwrites the s3 key if already exists.
    :type s3_overwrite: bool
    :param imap_conn_id: The reference to the connection details of the mail server.
    :type imap_conn_id: str
    :param s3_conn_id: The reference to the s3 connection details.
    :type s3_conn_id: str
    """
    template_fields = ('imap_attachment_name', 's3_key', 'imap_mail_filter')

    @apply_defaults
    def __init__(self,
                 imap_attachment_name,
                 s3_key,
                 imap_check_regex=False,
                 imap_mail_folder='INBOX',
                 imap_mail_filter='All',
                 s3_overwrite=False,
                 imap_conn_id='imap_default',
                 s3_conn_id='aws_default',
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.imap_attachment_name = imap_attachment_name
        self.s3_key = s3_key
        self.imap_check_regex = imap_check_regex
        self.imap_mail_folder = imap_mail_folder
        self.imap_mail_filter = imap_mail_filter
        self.s3_overwrite = s3_overwrite
        self.imap_conn_id = imap_conn_id
        self.s3_conn_id = s3_conn_id

    def execute(self, context):
        """
        This function executes the transfer from the email server (via imap) into s3.

        :param context: The context while executing.
        :type context: dict
        """
        self.log.info(
            'Transferring mail attachment %s from mail server via imap to s3 key %s...',
            self.imap_attachment_name, self.s3_key
        )

        with ImapHook(imap_conn_id=self.imap_conn_id) as imap_hook:
            imap_mail_attachments = imap_hook.retrieve_mail_attachments(
                name=self.imap_attachment_name,
                check_regex=self.imap_check_regex,
                latest_only=True,
                mail_folder=self.imap_mail_folder,
                mail_filter=self.imap_mail_filter,
            )

        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)
        s3_hook.load_bytes(bytes_data=imap_mail_attachments[0][1],
                           key=self.s3_key,
                           replace=self.s3_overwrite)


class MongoToS3Operator(BaseOperator):
    """
    Mongo -> S3
        A more specific baseOperator meant to move data
        from mongo via pymongo to s3 via boto

        things to note
                .execute() is written to depend on .transform()
                .transform() is meant to be extended by child classes
                to perform transformations unique to those operators needs
    """

    template_fields = ['s3_key', 'mongo_query']
    # pylint: disable=too-many-instance-attributes

    @apply_defaults
    def __init__(self,
                 mongo_conn_id,
                 s3_conn_id,
                 mongo_collection,
                 mongo_query,
                 s3_bucket,
                 s3_key,
                 mongo_db=None,
                 replace=False,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Conn Ids
        self.mongo_conn_id = mongo_conn_id
        self.s3_conn_id = s3_conn_id
        # Mongo Query Settings
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        # Grab query and determine if we need to run an aggregate pipeline
        self.mongo_query = mongo_query
        self.is_pipeline = True if isinstance(
            self.mongo_query, list) else False

        # S3 Settings
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.replace = replace

    def execute(self, context):
        """
        Executed by task_instance at runtime
        """
        s3_conn = S3Hook(self.s3_conn_id)

        # Grab collection and execute query according to whether or not it is a pipeline
        if self.is_pipeline:
            results = MongoHook(self.mongo_conn_id).aggregate(
                mongo_collection=self.mongo_collection,
                aggregate_query=self.mongo_query,
                mongo_db=self.mongo_db
            )

        else:
            results = MongoHook(self.mongo_conn_id).find(
                mongo_collection=self.mongo_collection,
                query=self.mongo_query,
                mongo_db=self.mongo_db
            )

        # Performs transform then stringifies the docs results into json format
        docs_str = self._stringify(self.transform(results))

        # Load Into S3
        s3_conn.load_string(
            string_data=docs_str,
            key=self.s3_key,
            bucket_name=self.s3_bucket,
            replace=self.replace
        )

        return True

    @staticmethod
    def _stringify(iterable, joinable='\n'):
        """
        Takes an iterable (pymongo Cursor or Array) containing dictionaries and
        returns a stringified version using python join
        """
        return joinable.join(
            [json.dumps(doc, default=json_util.default) for doc in iterable]
        )

    @staticmethod
    def transform(docs):
        """
        Processes pyMongo cursor and returns an iterable with each element being
                a JSON serializable dictionary

        Base transform() assumes no processing is needed
        ie. docs is a pyMongo cursor of documents and cursor just
        needs to be passed through

        Override this method for custom transformations
        """
        return docs


class S3DeleteObjectsOperator(BaseOperator):
    """
    To enable users to delete single object or multiple objects from
    a bucket using a single HTTP request.

    Users may specify up to 1000 keys to delete.

    :param bucket: Name of the bucket in which you are going to delete object(s). (templated)
    :type bucket: str
    :param keys: The key(s) to delete from S3 bucket. (templated)

        When ``keys`` is a string, it's supposed to be the key name of
        the single object to delete.

        When ``keys`` is a list, it's supposed to be the list of the
        keys to delete.

        You may specify up to 1000 keys.
    :type keys: str or list
    :param aws_conn_id: Connection id of the S3 connection to use
    :type aws_conn_id: str
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.

        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used,
                 but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :type verify: bool or str
    """

    template_fields = ('keys', 'bucket')

    @apply_defaults
    def __init__(
            self,
            bucket,
            keys,
            aws_conn_id='aws_default',
            verify=None,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket = bucket
        self.keys = keys
        self.aws_conn_id = aws_conn_id
        self.verify = verify

    def execute(self, context):
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)

        response = s3_hook.delete_objects(bucket=self.bucket, keys=self.keys)

        deleted_keys = [x['Key'] for x in response.get("Deleted", [])]
        self.log.info("Deleted: %s", deleted_keys)

        if "Errors" in response:
            errors_keys = [x['Key'] for x in response.get("Errors", [])]
            raise AirflowException("Errors when deleting: {}".format(errors_keys))


class S3ListOperator(BaseOperator):
    """
    List all objects from the bucket with the given string prefix in name.

    This operator returns a python list with the name of objects which can be
    used by `xcom` in the downstream task.

    :param bucket: The S3 bucket where to find the objects. (templated)
    :type bucket: str
    :param prefix: Prefix string to filters the objects whose name begin with
        such prefix. (templated)
    :type prefix: str
    :param delimiter: the delimiter marks key hierarchy. (templated)
    :type delimiter: str
    :param aws_conn_id: The connection ID to use when connecting to S3 storage.
    :type aws_conn_id: str
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                 (unless use_ssl is False), but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :type verify: bool or str


    **Example**:
        The following operator would list all the files
        (excluding subfolders) from the S3
        ``customers/2018/04/`` key in the ``data`` bucket. ::

            s3_file = S3ListOperator(
                task_id='list_3s_files',
                bucket='data',
                prefix='customers/2018/04/',
                delimiter='/',
                aws_conn_id='aws_customers_conn'
            )
    """
    template_fields = ('bucket', 'prefix', 'delimiter')  # type: Iterable[str]
    ui_color = '#ffd700'

    @apply_defaults
    def __init__(self,
                 bucket,
                 prefix='',
                 delimiter='',
                 aws_conn_id='aws_default',
                 verify=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket = bucket
        self.prefix = prefix
        self.delimiter = delimiter
        self.aws_conn_id = aws_conn_id
        self.verify = verify

    def execute(self, context):
        hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)

        self.log.info(
            'Getting the list of files from bucket: %s in prefix: %s (Delimiter {%s)',
            self.bucket, self.prefix, self.delimiter
        )

        return hook.list_keys(
            bucket_name=self.bucket,
            prefix=self.prefix,
            delimiter=self.delimiter)


class S3ToSFTPOperator(BaseOperator):
    """
    This operator enables the transferring of files from S3 to a SFTP server.

    :param sftp_conn_id: The sftp connection id. The name or identifier for
        establishing a connection to the SFTP server.
    :type sftp_conn_id: str
    :param sftp_path: The sftp remote path. This is the specified file path for
        uploading file to the SFTP server.
    :type sftp_path: str
    :param s3_conn_id: The s3 connection id. The name or identifier for
        establishing a connection to S3
    :type s3_conn_id: str
    :param s3_bucket: The targeted s3 bucket. This is the S3 bucket from
        where the file is downloaded.
    :type s3_bucket: str
    :param s3_key: The targeted s3 key. This is the specified file path for
        downloading the file from S3.
    :type s3_key: str
    """

    template_fields = ('s3_key', 'sftp_path')

    @apply_defaults
    def __init__(self,
                 s3_bucket,
                 s3_key,
                 sftp_path,
                 sftp_conn_id='ssh_default',
                 s3_conn_id='aws_default',
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.sftp_conn_id = sftp_conn_id
        self.sftp_path = sftp_path
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_conn_id = s3_conn_id

    @staticmethod
    def get_s3_key(s3_key):
        """This parses the correct format for S3 keys
            regardless of how the S3 url is passed."""

        parsed_s3_key = urlparse(s3_key)
        return parsed_s3_key.path.lstrip('/')

    def execute(self, context):
        self.s3_key = self.get_s3_key(self.s3_key)
        ssh_hook = SSHHook(ssh_conn_id=self.sftp_conn_id)
        s3_hook = S3Hook(self.s3_conn_id)

        s3_client = s3_hook.get_conn()
        sftp_client = ssh_hook.get_conn().open_sftp()

        with NamedTemporaryFile("w") as f:
            s3_client.download_file(self.s3_bucket, self.s3_key, f.name)
            sftp_client.put(f.name, self.sftp_path)


class SFTPToS3Operator(BaseOperator):
    """
    This operator enables the transferring of files from a SFTP server to
    Amazon S3.

    :param sftp_conn_id: The sftp connection id. The name or identifier for
        establishing a connection to the SFTP server.
    :type sftp_conn_id: str
    :param sftp_path: The sftp remote path. This is the specified file path
        for downloading the file from the SFTP server.
    :type sftp_path: str
    :param s3_conn_id: The s3 connection id. The name or identifier for
        establishing a connection to S3
    :type s3_conn_id: str
    :param s3_bucket: The targeted s3 bucket. This is the S3 bucket to where
        the file is uploaded.
    :type s3_bucket: str
    :param s3_key: The targeted s3 key. This is the specified path for
        uploading the file to S3.
    :type s3_key: str
    """

    template_fields = ('s3_key', 'sftp_path')

    @apply_defaults
    def __init__(self,
                 s3_bucket,
                 s3_key,
                 sftp_path,
                 sftp_conn_id='ssh_default',
                 s3_conn_id='aws_default',
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.sftp_conn_id = sftp_conn_id
        self.sftp_path = sftp_path
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_conn_id = s3_conn_id

    @staticmethod
    def get_s3_key(s3_key):
        """This parses the correct format for S3 keys
            regardless of how the S3 url is passed."""

        parsed_s3_key = urlparse(s3_key)
        return parsed_s3_key.path.lstrip('/')

    def execute(self, context):
        self.s3_key = self.get_s3_key(self.s3_key)
        ssh_hook = SSHHook(ssh_conn_id=self.sftp_conn_id)
        s3_hook = S3Hook(self.s3_conn_id)

        sftp_client = ssh_hook.get_conn().open_sftp()

        with NamedTemporaryFile("w") as f:
            sftp_client.get(self.sftp_path, f.name)

            s3_hook.load_file(
                filename=f.name,
                key=self.s3_key,
                bucket_name=self.s3_bucket,
                replace=True
            )


class S3ToGoogleCloudStorageOperator(S3ListOperator):
    """
    Synchronizes an S3 key, possibly a prefix, with a Google Cloud Storage
    destination path.

    :param bucket: The S3 bucket where to find the objects. (templated)
    :type bucket: str
    :param prefix: Prefix string which filters objects whose name begin with
        such prefix. (templated)
    :type prefix: str
    :param delimiter: the delimiter marks key hierarchy. (templated)
    :type delimiter: str
    :param aws_conn_id: The source S3 connection
    :type aws_conn_id: str
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                 (unless use_ssl is False), but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :type verify: bool or str
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param dest_gcs_conn_id: (Deprecated) The connection ID used to connect to Google Cloud
        Platform. This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type dest_gcs_conn_id: str
    :param dest_gcs: The destination Google Cloud Storage bucket and prefix
        where you want to store the files. (templated)
    :type dest_gcs: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param replace: Whether you want to replace existing destination files
        or not.
    :type replace: bool
    :param gzip: Option to compress file for upload
    :type gzip: bool


    **Example**:

    .. code-block:: python

       s3_to_gcs_op = S3ToGoogleCloudStorageOperator(
            task_id='s3_to_gcs_example',
            bucket='my-s3-bucket',
            prefix='data/customers-201804',
            dest_gcs_conn_id='google_cloud_default',
            dest_gcs='gs://my.gcs.bucket/some/customers/',
            replace=False,
            gzip=True,
            dag=my-dag)

    Note that ``bucket``, ``prefix``, ``delimiter`` and ``dest_gcs`` are
    templated, so you can use variables in them if you wish.
    """

    template_fields = ('bucket', 'prefix', 'delimiter', 'dest_gcs')
    ui_color = '#e09411'

    @apply_defaults
    def __init__(self,
                 bucket,
                 prefix='',
                 delimiter='',
                 aws_conn_id='aws_default',
                 verify=None,
                 gcp_conn_id='google_cloud_default',
                 dest_gcs_conn_id=None,
                 dest_gcs=None,
                 delegate_to=None,
                 replace=False,
                 gzip=False,
                 *args,
                 **kwargs):

        super().__init__(
            bucket=bucket,
            prefix=prefix,
            delimiter=delimiter,
            aws_conn_id=aws_conn_id,
            *args,
            **kwargs)

        if dest_gcs_conn_id:
            warnings.warn(
                "The dest_gcs_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.", DeprecationWarning, stacklevel=3)
            gcp_conn_id = dest_gcs_conn_id

        self.gcp_conn_id = gcp_conn_id
        self.dest_gcs = dest_gcs
        self.delegate_to = delegate_to
        self.replace = replace
        self.verify = verify
        self.gzip = gzip

        if dest_gcs and not self._gcs_object_is_directory(self.dest_gcs):
            self.log.info(
                'Destination Google Cloud Storage path is not a valid '
                '"directory", define a path that ends with a slash "/" or '
                'leave it empty for the root of the bucket.')
            raise AirflowException('The destination Google Cloud Storage path '
                                   'must end with a slash "/" or be empty.')

    def execute(self, context):
        # use the super method to list all the files in an S3 bucket/key
        files = super().execute(context)

        gcs_hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to)

        if not self.replace:
            # if we are not replacing -> list all files in the GCS bucket
            # and only keep those files which are present in
            # S3 and not in Google Cloud Storage
            bucket_name, object_prefix = _parse_gcs_url(self.dest_gcs)
            existing_files_prefixed = gcs_hook.list(
                bucket_name, prefix=object_prefix)

            existing_files = []

            if existing_files_prefixed:
                # Remove the object prefix itself, an empty directory was found
                if object_prefix in existing_files_prefixed:
                    existing_files_prefixed.remove(object_prefix)

                # Remove the object prefix from all object string paths
                for f in existing_files_prefixed:
                    if f.startswith(object_prefix):
                        existing_files.append(f[len(object_prefix):])
                    else:
                        existing_files.append(f)

            files = list(set(files) - set(existing_files))
            if len(files) > 0:
                self.log.info(
                    '%s files are going to be synced: %s.', len(files), files
                )
            else:
                self.log.info(
                    'There are no new files to sync. Have a nice day!')

        if files:
            hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)

            for file in files:
                # GCS hook builds its own in-memory file so we have to create
                # and pass the path
                file_object = hook.get_key(file, self.bucket)
                with NamedTemporaryFile(mode='wb', delete=True) as f:
                    file_object.download_fileobj(f)
                    f.flush()

                    dest_gcs_bucket, dest_gcs_object_prefix = _parse_gcs_url(
                        self.dest_gcs)
                    # There will always be a '/' before file because it is
                    # enforced at instantiation time
                    dest_gcs_object = dest_gcs_object_prefix + file

                    # Sync is sequential and the hook already logs too much
                    # so skip this for now
                    # self.log.info(
                    #     'Saving file {0} from S3 bucket {1} in GCS bucket {2}'
                    #     ' as object {3}'.format(file, self.bucket,
                    #                             dest_gcs_bucket,
                    #                             dest_gcs_object))

                    gcs_hook.upload(dest_gcs_bucket, dest_gcs_object, f.name, gzip=self.gzip)

            self.log.info(
                "All done, uploaded %d files to Google Cloud Storage",
                len(files))
        else:
            self.log.info(
                'In sync, no files needed to be uploaded to Google Cloud'
                'Storage')

        return files

    # Following functionality may be better suited in
    # airflow/contrib/hooks/gcs.py
    @staticmethod
    def _gcs_object_is_directory(object):
        _, blob = _parse_gcs_url(object)

        return len(blob) == 0 or blob.endswith('/')
