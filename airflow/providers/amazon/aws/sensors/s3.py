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
#

import os
import re
import sys
from datetime import datetime
from typing import TYPE_CHECKING, Callable, List, Optional, Sequence, Set, Union
from urllib.parse import urlparse

if TYPE_CHECKING:
    from airflow.utils.context import Context


if sys.version_info >= (3, 8):
    from functools import cached_property
else:
    from cached_property import cached_property

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.base import BaseSensorOperator, poke_mode_only


class S3KeySensor(BaseSensorOperator):
    """
    Waits for a key (a file-like instance on S3) to be present in a S3 bucket.
    S3 being a key/value it does not support folders. The path is just a key
    a resource.

    :param bucket_key: The key being waited on. Supports full s3:// style url
        or relative path from root level. When it's specified as a full s3://
        url, please leave bucket_name as `None`.
    :param bucket_name: Name of the S3 bucket. Only needed when ``bucket_key``
        is not provided as a full s3:// url.
    :param wildcard_match: whether the bucket_key should be interpreted as a
        Unix wildcard pattern
    :param aws_conn_id: a reference to the s3 connection
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                 (unless use_ssl is False), but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    """

    template_fields: Sequence[str] = ('bucket_key', 'bucket_name')

    def __init__(
        self,
        *,
        bucket_key: str,
        bucket_name: Optional[str] = None,
        wildcard_match: bool = False,
        aws_conn_id: str = 'aws_default',
        verify: Optional[Union[str, bool]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.bucket_name = bucket_name
        self.bucket_key = bucket_key
        self.wildcard_match = wildcard_match
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.hook: Optional[S3Hook] = None

    def poke(self, context: 'Context'):

        if self.bucket_name is None:
            parsed_url = urlparse(self.bucket_key)
            if parsed_url.netloc == '':
                raise AirflowException('If key is a relative path from root, please provide a bucket_name')
            self.bucket_name = parsed_url.netloc
            self.bucket_key = parsed_url.path.lstrip('/')
        else:
            parsed_url = urlparse(self.bucket_key)
            if parsed_url.scheme != '' or parsed_url.netloc != '':
                raise AirflowException(
                    'If bucket_name is provided, bucket_key'
                    ' should be relative path from root'
                    ' level, rather than a full s3:// url'
                )

        self.log.info('Poking for key : s3://%s/%s', self.bucket_name, self.bucket_key)
        if self.wildcard_match:
            return self.get_hook().check_for_wildcard_key(self.bucket_key, self.bucket_name)
        return self.get_hook().check_for_key(self.bucket_key, self.bucket_name)

    def get_hook(self) -> S3Hook:
        """Create and return an S3Hook"""
        if self.hook:
            return self.hook

        self.hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        return self.hook


class S3KeySizeSensor(S3KeySensor):
    """
    Waits for a key (a file-like instance on S3) to be present and be more than
    some size in a S3 bucket.
    S3 being a key/value it does not support folders. The path is just a key
    a resource.

    :param bucket_key: The key being waited on. Supports full s3:// style url
        or relative path from root level. When it's specified as a full s3://
        url, please leave bucket_name as `None`.
    :param bucket_name: Name of the S3 bucket. Only needed when ``bucket_key``
        is not provided as a full s3:// url.
    :param wildcard_match: whether the bucket_key should be interpreted as a
        Unix wildcard pattern
    :param aws_conn_id: a reference to the s3 connection
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                 (unless use_ssl is False), but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :param check_fn: Function that receives the list of the S3 objects,
        and returns the boolean:
        - ``True``: a certain criteria is met
        - ``False``: the criteria isn't met
        **Example**: Wait for any S3 object size more than 1 megabyte  ::

            def check_fn(self, data: List) -> bool:
                return any(f.get('Size', 0) > 1048576 for f in data if isinstance(f, dict))
    """

    def __init__(
        self,
        *,
        check_fn: Optional[Callable[..., bool]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.check_fn_user = check_fn

    def poke(self, context: 'Context'):
        if super().poke(context=context) is False:
            return False

        s3_objects = self.get_files(s3_hook=self.get_hook())
        if not s3_objects:
            return False
        if self.check_fn_user is not None:
            return self.check_fn_user(s3_objects)
        return self.check_fn(s3_objects)

    def get_files(self, s3_hook: S3Hook, delimiter: Optional[str] = '/') -> List:
        """Gets a list of files in the bucket"""
        prefix = self.bucket_key
        config = {
            'PageSize': None,
            'MaxItems': None,
        }
        if self.wildcard_match:
            prefix = re.split(r'[\[\*\?]', self.bucket_key, 1)[0]

        paginator = s3_hook.get_conn().get_paginator('list_objects_v2')
        response = paginator.paginate(
            Bucket=self.bucket_name, Prefix=prefix, Delimiter=delimiter, PaginationConfig=config
        )
        keys: List = []
        for page in response:
            if 'Contents' in page:
                _temp = [k for k in page['Contents'] if isinstance(k.get('Size', None), (int, float))]
                keys = keys + _temp
        return keys

    def check_fn(self, data: List, object_min_size: Optional[Union[int, float]] = 0) -> bool:
        """Default function for checking that S3 Objects have size more than 0

        :param data: List of the objects in S3 bucket.
        :param object_min_size: Checks if the objects sizes are greater then this value.
        """
        return all(f.get('Size', 0) > object_min_size for f in data if isinstance(f, dict))


@poke_mode_only
class S3KeysUnchangedSensor(BaseSensorOperator):
    """
    Checks for changes in the number of objects at prefix in AWS S3
    bucket and returns True if the inactivity period has passed with no
    increase in the number of objects. Note, this sensor will not behave correctly
    in reschedule mode, as the state of the listed objects in the S3 bucket will
    be lost between rescheduled invocations.

    :param bucket_name: Name of the S3 bucket
    :param prefix: The prefix being waited on. Relative path from bucket root level.
    :param aws_conn_id: a reference to the s3 connection
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                 (unless use_ssl is False), but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :param inactivity_period: The total seconds of inactivity to designate
        keys unchanged. Note, this mechanism is not real time and
        this operator may not return until a poke_interval after this period
        has passed with no additional objects sensed.
    :param min_objects: The minimum number of objects needed for keys unchanged
        sensor to be considered valid.
    :param previous_objects: The set of object ids found during the last poke.
    :param allow_delete: Should this sensor consider objects being deleted
        between pokes valid behavior. If true a warning message will be logged
        when this happens. If false an error will be raised.
    """

    template_fields: Sequence[str] = ('bucket_name', 'prefix')

    def __init__(
        self,
        *,
        bucket_name: str,
        prefix: str,
        aws_conn_id: str = 'aws_default',
        verify: Optional[Union[bool, str]] = None,
        inactivity_period: float = 60 * 60,
        min_objects: int = 1,
        previous_objects: Optional[Set[str]] = None,
        allow_delete: bool = True,
        **kwargs,
    ) -> None:

        super().__init__(**kwargs)

        self.bucket_name = bucket_name
        self.prefix = prefix
        if inactivity_period < 0:
            raise ValueError("inactivity_period must be non-negative")
        self.inactivity_period = inactivity_period
        self.min_objects = min_objects
        self.previous_objects = previous_objects or set()
        self.inactivity_seconds = 0
        self.allow_delete = allow_delete
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.last_activity_time: Optional[datetime] = None

    @cached_property
    def hook(self):
        """Returns S3Hook."""
        return S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)

    def is_keys_unchanged(self, current_objects: Set[str]) -> bool:
        """
        Checks whether new objects have been uploaded and the inactivity_period
        has passed and updates the state of the sensor accordingly.

        :param current_objects: set of object ids in bucket during last poke.
        """
        current_num_objects = len(current_objects)
        if current_objects > self.previous_objects:
            # When new objects arrived, reset the inactivity_seconds
            # and update previous_objects for the next poke.
            self.log.info(
                "New objects found at %s, resetting last_activity_time.",
                os.path.join(self.bucket_name, self.prefix),
            )
            self.log.debug("New objects: %s", current_objects - self.previous_objects)
            self.last_activity_time = datetime.now()
            self.inactivity_seconds = 0
            self.previous_objects = current_objects
            return False

        if self.previous_objects - current_objects:
            # During the last poke interval objects were deleted.
            if self.allow_delete:
                deleted_objects = self.previous_objects - current_objects
                self.previous_objects = current_objects
                self.last_activity_time = datetime.now()
                self.log.info(
                    "Objects were deleted during the last poke interval. Updating the "
                    "file counter and resetting last_activity_time:\n%s",
                    deleted_objects,
                )
                return False

            raise AirflowException(
                f"Illegal behavior: objects were deleted in"
                f" {os.path.join(self.bucket_name, self.prefix)} between pokes."
            )

        if self.last_activity_time:
            self.inactivity_seconds = int((datetime.now() - self.last_activity_time).total_seconds())
        else:
            # Handles the first poke where last inactivity time is None.
            self.last_activity_time = datetime.now()
            self.inactivity_seconds = 0

        if self.inactivity_seconds >= self.inactivity_period:
            path = os.path.join(self.bucket_name, self.prefix)

            if current_num_objects >= self.min_objects:
                self.log.info(
                    "SUCCESS: \nSensor found %s objects at %s.\n"
                    "Waited at least %s seconds, with no new objects uploaded.",
                    current_num_objects,
                    path,
                    self.inactivity_period,
                )
                return True

            self.log.error("FAILURE: Inactivity Period passed, not enough objects found in %s", path)

            return False
        return False

    def poke(self, context: 'Context'):
        return self.is_keys_unchanged(set(self.hook.list_keys(self.bucket_name, prefix=self.prefix)))


class S3PrefixSensor(BaseSensorOperator):
    """
    Waits for a prefix or all prefixes to exist. A prefix is the first part of a key,
    thus enabling checking of constructs similar to glob ``airfl*`` or
    SQL LIKE ``'airfl%'``. There is the possibility to precise a delimiter to
    indicate the hierarchy or keys, meaning that the match will stop at that
    delimiter. Current code accepts sane delimiters, i.e. characters that
    are NOT special characters in the Python regex engine.

    :param bucket_name: Name of the S3 bucket
    :param prefix: The prefix being waited on. Relative path from bucket root level.
    :param delimiter: The delimiter intended to show hierarchy.
        Defaults to '/'.
    :param aws_conn_id: a reference to the s3 connection
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:

        - ``False``: do not validate SSL certificates. SSL will still be used
                 (unless use_ssl is False), but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    """

    template_fields: Sequence[str] = ('prefix', 'bucket_name')

    def __init__(
        self,
        *,
        bucket_name: str,
        prefix: Union[str, List[str]],
        delimiter: str = '/',
        aws_conn_id: str = 'aws_default',
        verify: Optional[Union[str, bool]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        # Parse
        self.bucket_name = bucket_name
        self.prefix = [prefix] if isinstance(prefix, str) else prefix
        self.delimiter = delimiter
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.hook: Optional[S3Hook] = None

    def poke(self, context: 'Context'):
        self.log.info('Poking for prefix : %s in bucket s3://%s', self.prefix, self.bucket_name)
        return all(self._check_for_prefix(prefix) for prefix in self.prefix)

    def get_hook(self) -> S3Hook:
        """Create and return an S3Hook"""
        if self.hook:
            return self.hook

        self.hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        return self.hook

    def _check_for_prefix(self, prefix: str) -> bool:
        return self.get_hook().check_for_prefix(
            prefix=prefix, delimiter=self.delimiter, bucket_name=self.bucket_name
        )
