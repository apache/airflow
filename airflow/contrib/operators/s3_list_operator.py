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

from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class S3ListOperator(BaseOperator):
    """
    List all objects from the bucket with the given string prefix and delimiter
    in name.

    This operator returns a python list with the name of objects which can be
    used by `xcom` in the downstream task.

    :param bucket: The S3 bucket where to find the objects.
    :type bucket: string
    :param prefix: Prefix string to filters the objects whose name begin with
        such prefix
    :type prefix: string
    :param delimiter: The delimiter by which you want to filter the objects.
        For e.g to lists the CSV files from in a directory in S3 you would use
        delimiter='.csv'.
    :type delimiter: string
    :param aws_conn_id: The connection ID to use when connecting to S3 storage.
    :type aws_conn_id: string

    **Example**:
        The following operator would list all the CSV files from the S3
        ``customers/2018/04/`` key in the ``data`` bucket. ::

            s3_file = S3ListOperator(
                task_id='list_3s_files',
                bucket='data',
                prefix='customers/2018/04/',
                delimiter='.csv',
                aws_conn_id='aws_customers_conn'
            )
    """
    template_fields = ('bucket', 'prefix', 'delimiter')
    ui_color = '#ffd700'

    @apply_defaults
    def __init__(self,
                 bucket,
                 prefix='',
                 delimiter='',
                 aws_conn_id='aws_default',
                 *args,
                 **kwargs):
        super(S3ListOperator, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.prefix = prefix
        self.delimiter = delimiter
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        hook = S3Hook(aws_conn_id=self.aws_conn_id)

        self.log.info(
            'Getting the list of files from bucket: {0} in prefix: {1} (Delimiter {2})'.
            format(self.bucket, self.prefix, self.delimiter))

        return hook.list_keys(
            bucket_name=self.bucket,
            prefix=self.prefix,
            delimiter=self.delimiter)
