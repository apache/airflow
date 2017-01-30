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


import logging
import os
import tempfile
import zipfile
import csv

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.S3_hook import S3Hook

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

class SqlToS3(BaseOperator):
    """
    Moves data querying from a connection to s3 csv file

    :param db_conn_id: source connection id
    :type db_conn_id: str
    :param sql: SQL query to execute against the source database
    :type sql: str
    :param s3_bucket: bucket name in s3
    :type s3_bucket: str
    :param s3_file_key: file key to refeer file in s3. See s3_replace_file.
    :type s3_file_key: str
    :param s3_replace_file: A flag to decide whether or not to overwrite the key
            if it already exists. If replace is False and the key exists, an
            error will be raised.
    :type s3_replace_file: bool
    :param s3_zip_file: A flag to decide if the csv file will be zipped.
    :type s3_zip_file: bool
    :param s3_conn_id: s3 connection id
    :type s3_conn_id: str
    """
    template_fields = tuple()
    ui_color = '#e8f7e4'

    @apply_defaults
    def __init__(
            self,
            db_conn_id,
            sql,
            s3_bucket,
            s3_file_key,
            s3_conn_id,
            s3_replace_file=False,
            s3_zip_file=False,
            *args, **kwargs):

        super(SqlToS3, self).__init__(*args, **kwargs)
        self.db_conn_id=db_conn_id
        self.sql=sql
        self.s3_bucket=s3_bucket
        self.s3_file_key=s3_file_key
        self.s3_conn_id=s3_conn_id
        self.s3_replace_file=s3_replace_file
        self.s3_zip_file=s3_zip_file
        self.source_hook=self.get_source_hook()
        self.s3_hook=self.get_s3_hook()

    def execute(self, context):
        """
        Creates files into a temporary directory
        which will be cleaned afterwards
        """
        logging.info("Extracting data from {}".format(self.db_conn_id))
        logging.info("Executing: \n" + self.sql)
        cursor =self.source_hook.get_conn().cursor()
        cursor.execute(self.sql)

        tmpdir = tempfile.mkdtemp()
        filename = self.s3_file_key + '.csv'

        # Ensure the file is read/write by the creator only
        saved_umask = os.umask(int('077', 8))

        path = os.path.join(tmpdir, filename)
        logging.info("Writting on csv... %s."% (path))
        with open(path, "w") as tmp:
                csv_writer =  csv.writer(tmp)
                csv_writer.writerow([i[0] for i in cursor.description])
                csv_writer.writerows(cursor)

        if self.s3_zip_file:
            zippath = os.path.join(tmpdir, self.s3_file_key+'.zip')
            logging.info("Zipping CSV file.... %s."% (zippath))
            with zipfile.ZipFile(zippath, 'w', zipfile.ZIP_DEFLATED) as archive:
                    archive.write(path, filename)

        finalpath = zippath if self.s3_zip_file else path
        logging.info("Sending to S3 bucket %s  current file %s"%(self.s3_bucket, finalpath))
        self.s3_hook.load_file(finalpath, self.s3_file_key , self.s3_bucket, self.s3_replace_file)
        logging.info("File is stored in S3 with %s key."%(self.s3_file_key))
        self.s3_hook.connection.close()


    def get_source_hook(self):
        return BaseHook.get_hook(self.db_conn_id)


    def get_s3_hook(self):
        return S3Hook(s3_conn_id=self.s3_conn_id)