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
from __future__ import annotations

import fnmatch
import os
import re
from datetime import datetime
from typing import TYPE_CHECKING, Callable, Sequence

if TYPE_CHECKING:
    from airflow.utils.context import Context

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.base import BaseSensorOperator, poke_mode_only


class S3KeySensor(BaseSensorOperator):
    """
    Waits for one or multiple keys (a file-like instance on S3) to be present in a S3 bucket.
    The path is just a key/value pointer to a resource for the given S3 path.
    Note: S3 does not support folders directly, and only provides key/value pairs.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:S3KeySensor`

    :param bucket_key: The key(s) being waited on. Supports full s3:// style url
        or relative path from root level. When it's specified as a full s3://
        url, please leave bucket_name as `None`
    :param bucket_name: Name of the S3 bucket. Only needed when ``bucket_key``
        is not provided as a full s3:// url. When specified, all the keys passed to ``bucket_key``
        refers to this bucket
    :param wildcard_match: whether the bucket_key should be interpreted as a
        Unix wildcard pattern
    :param check_fn: Function that receives the list of the S3 objects,
        and returns a boolean:
        - ``True``: the criteria is met
        - ``False``: the criteria isn't met
        **Example**: Wait for any S3 object size more than 1 megabyte  ::

            def check_fn(files: List) -> bool:
                return any(f.get('Size', 0) > 1048576 for f in files)
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

    template_fields: Sequence[str] = ("bucket_key", "bucket_name")

    def __init__(
        self,
        *,
        bucket_key: str | list[str],
        bucket_name: str | None = None,
        wildcard_match: bool = False,
        check_fn: Callable[..., bool] | None = None,
        aws_conn_id: str = "aws_default",
        verify: str | bool | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.bucket_key = bucket_key
        self.wildcard_match = wildcard_match
        self.check_fn = check_fn
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.hook: S3Hook | None = None

    def _check_key(self, key):
        bucket_name, key = S3Hook.get_s3_bucket_key(self.bucket_name, key, "bucket_name", "bucket_key")
        self.log.info("Poking for key : s3://%s/%s", bucket_name, key)

        """
        Set variable `files` which contains a list of dict which contains only the size
        If needed we might want to add other attributes later
        Format: [{
            'Size': int
        }]
        """
        if self.wildcard_match:
            prefix = re.split(r"[\[\*\?]", key, 1)[0]
            keys = self.get_hook().get_file_metadata(prefix, bucket_name)
            key_matches = [k for k in keys if fnmatch.fnmatch(k["Key"], key)]
            if len(key_matches) == 0:
                return False

            # Reduce the set of metadata to size only
            files = list(map(lambda f: {"Size": f["Size"]}, key_matches))
        else:
            obj = self.get_hook().head_object(key, bucket_name)
            if obj is None:
                return False
            files = [{"Size": obj["ContentLength"]}]

        if self.check_fn is not None:
            return self.check_fn(files)

        return True

    def poke(self, context: Context):
        if isinstance(self.bucket_key, str):
            return self._check_key(self.bucket_key)
        else:
            return all(self._check_key(key) for key in self.bucket_key)

    def get_hook(self) -> S3Hook:
        """Create and return an S3Hook"""
        if self.hook:
            return self.hook

        self.hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        return self.hook


@poke_mode_only
class S3KeysUnchangedSensor(BaseSensorOperator):
    """
    Checks for changes in the number of objects at prefix in AWS S3
    bucket and returns True if the inactivity period has passed with no
    increase in the number of objects. Note, this sensor will not behave correctly
    in reschedule mode, as the state of the listed objects in the S3 bucket will
    be lost between rescheduled invocations.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:S3KeysUnchangedSensor`

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

    template_fields: Sequence[str] = ("bucket_name", "prefix")

    def __init__(
        self,
        *,
        bucket_name: str,
        prefix: str,
        aws_conn_id: str = "aws_default",
        verify: bool | str | None = None,
        inactivity_period: float = 60 * 60,
        min_objects: int = 1,
        previous_objects: set[str] | None = None,
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
        self.last_activity_time: datetime | None = None

    @cached_property
    def hook(self):
        """Returns S3Hook."""
        return S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)

    def is_keys_unchanged(self, current_objects: set[str]) -> bool:
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

    def poke(self, context: Context):
        return self.is_keys_unchanged(set(self.hook.list_keys(self.bucket_name, prefix=self.prefix)))
