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


from urllib.parse import urlparse

from airflow.exceptions import AirflowException
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class S3KeySensor(BaseSensorOperator):
    """
    Waits for a key (a file-like instance on S3) to be present in a S3 bucket.
    S3 being a key/value it does not support folders. The path is just a key
    a resource.

    :param bucket_key: The key being waited on. Supports full s3:// style url
        or relative path from root level.
    :type bucket_key: str
    :param bucket_name: Name of the S3 bucket
    :type bucket_name: str
    :param wildcard_match: whether the bucket_key should be interpreted as a
        Unix wildcard pattern
    :type wildcard_match: bool
    :param aws_conn_id: a reference to the s3 connection
    :type aws_conn_id: str
    """
    template_fields = ('bucket_key', 'bucket_name')

    @apply_defaults
    def __init__(self,
                 bucket_key,
                 bucket_name=None,
                 wildcard_match=False,
                 aws_conn_id='aws_default',
                 *args,
                 **kwargs):
        super(S3KeySensor, self).__init__(*args, **kwargs)
        # Parse
        if bucket_name is None:
            parsed_url = urlparse(bucket_key)
            if parsed_url.netloc == '':
                raise AirflowException('Please provide a bucket_name')
            else:
                bucket_name = parsed_url.netloc
                if parsed_url.path[0] == '/':
                    bucket_key = parsed_url.path[1:]
                else:
                    bucket_key = parsed_url.path
        self.bucket_name = bucket_name
        self.bucket_key = bucket_key
        self.wildcard_match = wildcard_match
        self.aws_conn_id = aws_conn_id

    def poke(self, context):
        from airflow.hooks.S3_hook import S3Hook
        hook = S3Hook(aws_conn_id=self.aws_conn_id)
        full_url = "s3://" + self.bucket_name + "/" + self.bucket_key
        self.log.info('Poking for key : {full_url}'.format(**locals()))
        if self.wildcard_match:
            return hook.check_for_wildcard_key(self.bucket_key,
                                               self.bucket_name)
        else:
            return hook.check_for_key(self.bucket_key, self.bucket_name)
