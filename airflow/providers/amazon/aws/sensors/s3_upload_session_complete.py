
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

import os
from datetime import datetime
from typing import Optional, Set

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.base_sensor_operator import BaseSensorOperator, poke_mode_only
from airflow.utils.decorators import apply_defaults


@poke_mode_only
class S3UploadSessionCompleteSensor(BaseSensorOperator):
    """
    Checks for changes in the number of objects at prefix in AWS S3
    bucket and returns True if the inactivity period has passed with no
    increase in the number of objects. Note, this sensor will no behave correctly
    in reschedule mode, as the state of the listed objects in the S3 bucket will
    be lost between rescheduled invocations.

    :param bucket_name: Name of the S3 bucket
    :type bucket_name: str
    :param prefix: The prefix being waited on. Relative path from bucket root level.
    :type prefix: str
    :param aws_conn_id: a reference to the s3 connection
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
    :param inactivity_period: The total seconds of inactivity to designate
        an upload session is over. Note, this mechanism is not real time and
        this operator may not return until a poke_interval after this period
        has passed with no additional objects sensed.
    :type inactivity_period: float
    :param min_objects: The minimum number of objects needed for upload session
        to be considered valid.
    :type min_objects: int
    :param previous_objects: The set of object ids found during the last poke.
    :type previous_objects: set[str]
    :param allow_delete: Should this sensor consider objects being deleted
        between pokes valid behavior. If true a warning message will be logged
        when this happens. If false an error will be raised.
    :type allow_delete: bool
    """

    template_fields = ('bucket_name', 'prefix')

    @apply_defaults
    def __init__(self,
                 bucket_name: str,
                 prefix: str,
                 inactivity_period: float = 60 * 60,
                 min_objects: int = 1,
                 previous_objects: Optional[Set[str]] = None,
                 allow_delete: bool = True,
                 aws_conn_id: str = 'google_cloud_default',
                 delegate_to: Optional[str] = None,
                 *args, **kwargs) -> None:

        super().__init__(*args, **kwargs)

        self.bucket = bucket_name
        self.prefix = prefix
        if inactivity_period < 0:
            raise ValueError("inactivity_period must be non-negative")
        self.inactivity_period = inactivity_period
        self.min_objects = min_objects
        self.previous_objects = previous_objects if previous_objects else set()
        self.inactivity_seconds = 0
        self.allow_delete = allow_delete
        self.aws_conn_id = aws_conn_id
        self.delegate_to = delegate_to
        self.last_activity_time = None
        self.hook = None

    def _get_aws_hook(self):
        if not self.hook:
            self.hook = S3Hook()
        return self.hook

    def is_bucket_updated(self, current_objects: Set[str]) -> bool:
        """
        Checks whether new objects have been uploaded and the inactivity_period
        has passed and updates the state of the sensor accordingly.

        :param current_objects: set of object ids in bucket during last poke.
        :type current_objects: set[str]
        """
        current_num_objects = len(current_objects)
        if current_objects > self.previous_objects:
            # When new objects arrived, reset the inactivity_seconds
            # and update previous_objects for the next poke.
            self.log.info("New objects found at %s resetting last_activity_time.",
                          os.path.join(self.bucket, self.prefix))
            self.log.debug("New objects: %s",
                           "\n".join(current_objects - self.previous_objects))
            self.last_activity_time = get_time()
            self.inactivity_seconds = 0
            self.previous_objects = current_objects
            return False

        if self.previous_objects - current_objects:
            # During the last poke interval objects were deleted.
            if self.allow_delete:
                self.previous_objects = current_objects
                self.last_activity_time = get_time()
                self.log.warning(
                    """
                    Objects were deleted during the last
                    poke interval. Updating the file counter and
                    resetting last_activity_time.
                    %s
                    """, self.previous_objects - current_objects
                )
                return False

            raise AirflowException(
                """
                Illegal behavior: objects were deleted in {} between pokes.
                """.format(os.path.join(self.bucket, self.prefix))
            )

        if self.last_activity_time:
            self.inactivity_seconds = (get_time() - self.last_activity_time).total_seconds()
        else:
            # Handles the first poke where last inactivity time is None.
            self.last_activity_time = get_time()
            self.inactivity_seconds = 0

        if self.inactivity_seconds >= self.inactivity_period:
            path = os.path.join(self.bucket, self.prefix)

            if current_num_objects >= self.min_objects:
                self.log.info("""SUCCESS:
                    Sensor found %s objects at %s.
                    Waited at least %s seconds, with no new objects dropped.
                    """, current_num_objects, path, self.inactivity_period)
                return True

            self.log.error("FAILURE: Inactivity Period passed, not enough objects found in %s", path)

            return False
        return False

    def poke(self, context):
        return self.is_bucket_updated(set(self._get_aws_hook().list_keys(self.bucket, prefix=self.prefix)))


def get_time():
    """
    This is just a wrapper of datetime.datetime.now to simplify mocking in the
    unittests.
    """
    return datetime.now()
