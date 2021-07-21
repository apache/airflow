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

from typing import Iterable, Optional, Union

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class S3ListPrefixesOperator(BaseOperator):
    """
    List all subfolders from the bucket with the given string prefix in name.

    This operator returns a python list with the name of all subfolders which
    can be used by `xcom` in the downstream task.

    :param bucket: The S3 bucket where to find the subfolders. (templated)
    :type bucket: str
    :param prefix: Prefix string to filter the subfolders whose name begin with
        such prefix. (templated)
    :type prefix: str
    :param delimiter: the delimiter marks subfolder hierarchy. (templated)
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
        The following operator would list all the subfolders
        from the S3 ``customers/2018/04/`` prefix in the ``data`` bucket. ::

            s3_file = S3ListPrefixesOperator(
                task_id='list_s3_prefixes',
                bucket='data',
                prefix='customers/2018/04/',
                delimiter='/',
                aws_conn_id='aws_customers_conn'
            )
    """

    template_fields: Iterable[str] = ('bucket', 'prefix', 'delimiter')
    ui_color = '#ffd700'

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str = '',
        delimiter: str = '',
        aws_conn_id: str = 'aws_default',
        verify: Optional[Union[str, bool]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.bucket = bucket
        self.prefix = prefix
        self.delimiter = delimiter
        self.aws_conn_id = aws_conn_id
        self.verify = verify

    def execute(self, context):
        hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)

        self.log.info(
            'Getting the list of subfolders from bucket: %s in prefix: %s (Delimiter {%s)',
            self.bucket,
            self.prefix,
            self.delimiter,
        )

        return hook.list_prefixes(bucket_name=self.bucket, prefix=self.prefix, delimiter=self.delimiter)
