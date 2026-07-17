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
import inspect
import os
import re
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor
from airflow.providers.amazon.aws.triggers.s3 import S3KeysUnchangedTrigger, S3KeyTrigger
from airflow.providers.amazon.aws.utils import validate_execute_complete_event
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.providers.common.compat.sdk import AirflowException, conf, poke_mode_only

if TYPE_CHECKING:
    from airflow.sdk import Context


class S3KeySensor(AwsBaseSensor[S3Hook]):
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
        is not provided as a full ``s3://`` url. When specified, all the keys passed to ``bucket_key``
        refers to this bucket
    :param wildcard_match: whether the bucket_key should be interpreted as a
        Unix wildcard pattern
    :param check_fn: Function that receives the list of the S3 objects with the context values,
        and returns a boolean:
        - ``True``: the criteria is met
        - ``False``: the criteria isn't met
        **Example**: Wait for any S3 object size more than 1 megabyte  ::

            def check_fn(files: List, **kwargs) -> bool:
                return any(f.get('Size', 0) > 1048576 for f in files)
    :param deferrable: Run operator in the deferrable mode
    :param use_regex: whether to use regex to check bucket
    :param metadata_keys: List of head_object attributes to gather and send to ``check_fn``.
        Acceptable values: Any top level attribute returned by s3.head_object. Specify * to return
        all available attributes.
        Default value: "Size".
        If the requested attribute is not found, the key is still included and the value is None.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    """

    template_fields: Sequence[str] = aws_template_fields("bucket_key", "bucket_name")
    aws_hook_class = S3Hook

    def __init__(
        self,
        *,
        bucket_key: str | list[str],
        bucket_name: str | None = None,
        wildcard_match: bool = False,
        check_fn: Callable[..., bool] | None = None,
        verify: str | bool | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        use_regex: bool = False,
        metadata_keys: list[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.bucket_key = bucket_key
        self.wildcard_match = wildcard_match
        self.check_fn = check_fn
        self.verify = verify
        self.deferrable = deferrable
        self.use_regex = use_regex
        self.metadata_keys = metadata_keys if metadata_keys else ["Size", "Key"]

    def _check_key(self, key, context: Context):
        bucket_name, key = self.hook.get_s3_bucket_key(self.bucket_name, key, "bucket_name", "bucket_key")
        self.log.info("Poking for key : s3://%s/%s", bucket_name, key)

        """
        Set variable `files` which contains a list of dict which contains attributes defined by the user
        Format: [{
            'Size': int,
            'Key': str,
        }]
        """
        if self.wildcard_match:
            prefix = re.split(r"[\[*?]", key, 1)[0]

            key_matches: list[str] = []

            # Is check_fn is None, then we can return True without having to iterate through each value in
            # yielded by iter_file_metadata. Otherwise, we'll check for a match, and add all matches to the
            # key_matches list
            for k in self.hook.iter_file_metadata(prefix, bucket_name):
                if fnmatch.fnmatch(k["Key"], key):
                    if self.check_fn is None:
                        # This will only wait for a single match, and will immediately return
                        return True
                    key_matches.append(k)

            if not key_matches:
                return False

            # Reduce the set of metadata to requested attributes
            files = []
            for f in key_matches:
                metadata = {}
                if "*" in self.metadata_keys:
                    metadata = self.hook.head_object(f["Key"], bucket_name)  # type: ignore[index]
                else:
                    for mk in self.metadata_keys:
                        try:
                            metadata[mk] = f[mk]  # type: ignore[index]
                        except KeyError:
                            # supplied key might be from head_object response
                            self.log.info("Key %s not found in response, performing head_object", mk)
                            metadata[mk] = self.hook.head_object(f["Key"], bucket_name).get(mk, None)  # type: ignore[index]
                files.append(metadata)

        elif self.use_regex:
            for k in self.hook.iter_file_metadata("", bucket_name):
                if re.match(pattern=key, string=k["Key"]):
                    return True
            return False

        else:
            obj = self.hook.head_object(key, bucket_name)
            if obj is None:
                return False
            metadata = {}
            if "*" in self.metadata_keys:
                metadata = self.hook.head_object(key, bucket_name)

            else:
                for key in self.metadata_keys:
                    # backwards compatibility with original implementation
                    if key == "Size":
                        metadata[key] = obj.get("ContentLength")
                    else:
                        metadata[key] = obj.get(key, None)
            files = [metadata]

        if self.check_fn is not None:
            # For backwards compatibility, check if the function takes a context argument
            signature = inspect.signature(self.check_fn)
            if any(param.kind == inspect.Parameter.VAR_KEYWORD for param in signature.parameters.values()):
                return self.check_fn(files, **context)
            # Otherwise, just pass the files
            return self.check_fn(files)

        return True

    def poke(self, context: Context):
        if isinstance(self.bucket_key, str):
            return self._check_key(self.bucket_key, context=context)
        return all(self._check_key(key, context=context) for key in self.bucket_key)

    def execute(self, context: Context) -> None:
        """Airflow runs this method on the worker and defers using the trigger."""
        if not self.deferrable:
            super().execute(context)
        else:
            if not self.poke(context=context):
                self._defer()

    def _defer(self) -> None:
        """Check for a keys in s3 and defers using the triggerer."""
        self.defer(
            timeout=timedelta(seconds=self.timeout),
            trigger=S3KeyTrigger(
                bucket_name=cast("str", self.bucket_name),
                bucket_key=self.bucket_key,
                wildcard_match=self.wildcard_match,
                aws_conn_id=self.aws_conn_id,
                region_name=self.region_name,
                verify=self.verify,
                botocore_config=self.botocore_config,
                poke_interval=self.poke_interval,
                should_check_fn=bool(self.check_fn),
                use_regex=self.use_regex,
                metadata_keys=self.metadata_keys,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        """
        Execute when the trigger fires - returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        if event["status"] == "running":
            found_keys = self.check_fn(event["files"], **context)  # type: ignore[misc]
            if not found_keys:
                self._defer()
        elif event["status"] == "error":
            raise AirflowException(event["message"])


@poke_mode_only
class S3KeysUnchangedSensor(AwsBaseSensor[S3Hook]):
    """
    Return True if inactivity_period has passed with no increase in the number of objects matching prefix.

    Note, this sensor will not behave correctly in reschedule mode, as the state of the listed
    objects in the S3 bucket will be lost between rescheduled invocations.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:S3KeysUnchangedSensor`

    :param bucket_name: Name of the S3 bucket
    :param prefix: The prefix being waited on. Relative path from bucket root level.
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
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
    :param deferrable: Run sensor in the deferrable mode
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    """

    template_fields: Sequence[str] = aws_template_fields("bucket_name", "prefix")
    aws_hook_class = S3Hook

    def __init__(
        self,
        *,
        bucket_name: str,
        prefix: str,
        verify: bool | str | None = None,
        inactivity_period: float = 60 * 60,
        min_objects: int = 1,
        previous_objects: set[str] | None = None,
        allow_delete: bool = True,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
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
        self.deferrable = deferrable
        self.verify = verify
        self.last_activity_time: datetime | None = None

    def is_keys_unchanged(self, current_objects: set[str]) -> bool:
        """
        Check for new objects after the inactivity_period and update the sensor state accordingly.

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
                f"Illegal behavior: objects were deleted in {os.path.join(self.bucket_name, self.prefix)} between pokes."
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

    def execute(self, context: Context) -> None:
        """Airflow runs this method on the worker and defers using the trigger if deferrable is True."""
        if not self.deferrable:
            super().execute(context)
        else:
            if not self.poke(context):
                self.defer(
                    timeout=timedelta(seconds=self.timeout),
                    trigger=S3KeysUnchangedTrigger(
                        bucket_name=self.bucket_name,
                        prefix=self.prefix,
                        inactivity_period=self.inactivity_period,
                        min_objects=self.min_objects,
                        previous_objects=self.previous_objects,
                        inactivity_seconds=self.inactivity_seconds,
                        allow_delete=self.allow_delete,
                        aws_conn_id=self.aws_conn_id,
                        region_name=self.region_name,
                        verify=self.verify,
                        botocore_config=self.botocore_config,
                        last_activity_time=self.last_activity_time,
                    ),
                    method_name="execute_complete",
                )

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> None:
        """
        Execute when the trigger fires - returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        event = validate_execute_complete_event(event)

        if event and event["status"] == "error":
            raise AirflowException(event["message"])
        return None
