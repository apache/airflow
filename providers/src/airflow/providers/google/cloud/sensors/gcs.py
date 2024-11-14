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
"""This module contains Google Cloud Storage sensors."""

from __future__ import annotations

import os
import textwrap
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Callable, Sequence

from google.cloud.storage.retry import DEFAULT_RETRY

from airflow.configuration import conf
from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.triggers.gcs import (
    GCSBlobTrigger,
    GCSCheckBlobUpdateTimeTrigger,
    GCSPrefixBlobTrigger,
    GCSUploadSessionTrigger,
)
from airflow.providers.google.common.deprecated import deprecated
from airflow.sensors.base import BaseSensorOperator, poke_mode_only

if TYPE_CHECKING:
    from google.api_core.retry import Retry

    from airflow.utils.context import Context


class GCSObjectExistenceSensor(BaseSensorOperator):
    """
    Checks for the existence of a file in Google Cloud Storage.

    :param bucket: The Google Cloud Storage bucket where the object is.
    :param object: The name of the object to check in the Google cloud
        storage bucket.
    :param use_glob: When set to True the object parameter is interpreted as glob
    :param google_cloud_conn_id: The connection ID to use when
        connecting to Google Cloud Storage.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param retry: (Optional) How to retry the RPC
    """

    template_fields: Sequence[str] = (
        "bucket",
        "object",
        "impersonation_chain",
    )
    ui_color = "#f0eee4"

    def __init__(
        self,
        *,
        bucket: str,
        object: str,
        use_glob: bool = False,
        google_cloud_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        retry: Retry = DEFAULT_RETRY,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket = bucket
        self.object = object
        self.use_glob = use_glob
        self.google_cloud_conn_id = google_cloud_conn_id
        self._matches: bool = False
        self.impersonation_chain = impersonation_chain
        self.retry = retry

        self.deferrable = deferrable

    def poke(self, context: Context) -> bool:
        self.log.info("Sensor checks existence of : %s, %s", self.bucket, self.object)
        hook = GCSHook(
            gcp_conn_id=self.google_cloud_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self._matches = (
            bool(hook.list(self.bucket, match_glob=self.object))
            if self.use_glob
            else hook.exists(self.bucket, self.object, self.retry)
        )
        return self._matches

    def execute(self, context: Context):
        """Airflow runs this method on the worker and defers using the trigger."""
        if self.deferrable:
            if not self.poke(context=context):
                self.defer(
                    timeout=timedelta(seconds=self.timeout),
                    trigger=GCSBlobTrigger(
                        bucket=self.bucket,
                        object_name=self.object,
                        use_glob=self.use_glob,
                        poke_interval=self.poke_interval,
                        google_cloud_conn_id=self.google_cloud_conn_id,
                        hook_params={
                            "impersonation_chain": self.impersonation_chain,
                        },
                    ),
                    method_name="execute_complete",
                )
        else:
            super().execute(context)
        return self._matches

    def execute_complete(self, context: Context, event: dict[str, str]) -> bool:
        """
        Act as a callback for when the trigger fires - returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info("File %s was found in bucket %s.", self.object, self.bucket)
        return True


@deprecated(
    planned_removal_date="November 01, 2024",
    use_instead="GCSObjectExistenceSensor",
    instructions="Please use GCSObjectExistenceSensor and set deferrable attribute to True.",
    category=AirflowProviderDeprecationWarning,
)
class GCSObjectExistenceAsyncSensor(GCSObjectExistenceSensor):
    """
    Checks for the existence of a file in Google Cloud Storage.

    This class is deprecated and will be removed in a future release.

    Please use :class:`airflow.providers.google.cloud.sensors.gcs.GCSObjectExistenceSensor`
    and set *deferrable* attribute to *True* instead.

    :param bucket: The Google Cloud Storage bucket where the object is.
    :param object: The name of the object to check in the Google cloud storage bucket.
    :param google_cloud_conn_id: The connection ID to use when connecting to Google Cloud Storage.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(deferrable=True, **kwargs)


def ts_function(context):
    """
    Act as a default callback for the GoogleCloudStorageObjectUpdatedSensor.

    The default behaviour is check for the object being updated after the data
    interval's end.
    """
    return context["data_interval_end"]


class GCSObjectUpdateSensor(BaseSensorOperator):
    """
    Checks if an object is updated in Google Cloud Storage.

    :param bucket: The Google Cloud Storage bucket where the object is.
    :param object: The name of the object to download in the Google cloud
        storage bucket.
    :param ts_func: Callback for defining the update condition. The default callback
        returns execution_date + schedule_interval. The callback takes the context
        as parameter.
    :param google_cloud_conn_id: The connection ID to use when
        connecting to Google Cloud Storage.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param deferrable: Run sensor in deferrable mode
    """

    template_fields: Sequence[str] = (
        "bucket",
        "object",
        "impersonation_chain",
    )
    ui_color = "#f0eee4"

    def __init__(
        self,
        bucket: str,
        object: str,
        ts_func: Callable = ts_function,
        google_cloud_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket = bucket
        self.object = object
        self.ts_func = ts_func
        self.google_cloud_conn_id = google_cloud_conn_id
        self.impersonation_chain = impersonation_chain
        self.deferrable = deferrable

    def poke(self, context: Context) -> bool:
        self.log.info("Sensor checks existence of : %s, %s", self.bucket, self.object)
        hook = GCSHook(
            gcp_conn_id=self.google_cloud_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        return hook.is_updated_after(self.bucket, self.object, self.ts_func(context))

    def execute(self, context: Context) -> None:
        """Airflow runs this method on the worker and defers using the trigger."""
        if self.deferrable is False:
            super().execute(context)
        else:
            if not self.poke(context=context):
                self.defer(
                    timeout=timedelta(seconds=self.timeout),
                    trigger=GCSCheckBlobUpdateTimeTrigger(
                        bucket=self.bucket,
                        object_name=self.object,
                        target_date=self.ts_func(context),
                        poke_interval=self.poke_interval,
                        google_cloud_conn_id=self.google_cloud_conn_id,
                        hook_params={
                            "impersonation_chain": self.impersonation_chain,
                        },
                    ),
                    method_name="execute_complete",
                )

    def execute_complete(self, context: dict[str, Any], event: dict[str, str] | None = None) -> str:
        """Return immediately and rely on trigger to throw a success event. Callback for the trigger."""
        if event:
            if event["status"] == "success":
                self.log.info(
                    "Checking last updated time for object %s in bucket : %s", self.object, self.bucket
                )
                return event["message"]
            raise AirflowException(event["message"])

        message = "No event received in trigger callback"
        raise AirflowException(message)


class GCSObjectsWithPrefixExistenceSensor(BaseSensorOperator):
    """
    Checks for the existence of GCS objects at a given prefix, passing matches via XCom.

    When files matching the given prefix are found, the poke method's criteria will be
    fulfilled and the matching objects will be returned from the operator and passed
    through XCom for downstream tasks.

    :param bucket: The Google Cloud Storage bucket where the object is.
    :param prefix: The name of the prefix to check in the Google cloud
        storage bucket.
    :param google_cloud_conn_id: The connection ID to use when
        connecting to Google Cloud Storage.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param deferrable: Run sensor in deferrable mode
    """

    template_fields: Sequence[str] = (
        "bucket",
        "prefix",
        "impersonation_chain",
    )
    ui_color = "#f0eee4"

    def __init__(
        self,
        bucket: str,
        prefix: str,
        google_cloud_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket = bucket
        self.prefix = prefix
        self.google_cloud_conn_id = google_cloud_conn_id
        self._matches: list[str] = []
        self.impersonation_chain = impersonation_chain
        self.deferrable = deferrable

    def poke(self, context: Context) -> bool:
        self.log.info("Checking for existence of object: %s, %s", self.bucket, self.prefix)
        hook = GCSHook(
            gcp_conn_id=self.google_cloud_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        self._matches = hook.list(self.bucket, prefix=self.prefix)
        return bool(self._matches)

    def execute(self, context: Context):
        """Overridden to allow matches to be passed."""
        self.log.info("Checking for existence of object: %s, %s", self.bucket, self.prefix)
        if not self.deferrable:
            super().execute(context)
            return self._matches
        else:
            if not self.poke(context=context):
                self.defer(
                    timeout=timedelta(seconds=self.timeout),
                    trigger=GCSPrefixBlobTrigger(
                        bucket=self.bucket,
                        prefix=self.prefix,
                        poke_interval=self.poke_interval,
                        google_cloud_conn_id=self.google_cloud_conn_id,
                        hook_params={
                            "impersonation_chain": self.impersonation_chain,
                        },
                    ),
                    method_name="execute_complete",
                )
            else:
                return self._matches

    def execute_complete(self, context: dict[str, Any], event: dict[str, str | list[str]]) -> str | list[str]:
        """Return immediately and rely on trigger to throw a success event. Callback for the trigger."""
        self.log.info("Resuming from trigger and checking status")
        if event["status"] == "success":
            return event["matches"]
        raise AirflowException(event["message"])


def get_time():
    """Act as a wrapper of datetime.datetime.now to simplify mocking in the unittests."""
    return datetime.now()


@poke_mode_only
class GCSUploadSessionCompleteSensor(BaseSensorOperator):
    """
    Return True if the inactivity period has passed with no increase in the number of objects in the bucket.

    Checks for changes in the number of objects at prefix in Google Cloud Storage
    bucket and returns True if the inactivity period has passed with no
    increase in the number of objects. Note, this sensor will not behave correctly
    in reschedule mode, as the state of the listed objects in the GCS bucket will
    be lost between rescheduled invocations.

    :param bucket: The Google Cloud Storage bucket where the objects are.
        expected.
    :param prefix: The name of the prefix to check in the Google cloud
        storage bucket.
    :param inactivity_period: The total seconds of inactivity to designate
        an upload session is over. Note, this mechanism is not real time and
        this operator may not return until a poke_interval after this period
        has passed with no additional objects sensed.
    :param min_objects: The minimum number of objects needed for upload session
        to be considered valid.
    :param previous_objects: The set of object ids found during the last poke.
    :param allow_delete: Should this sensor consider objects being deleted
        between pokes valid behavior. If true a warning message will be logged
        when this happens. If false an error will be raised.
    :param google_cloud_conn_id: The connection ID to use when connecting
        to Google Cloud Storage.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param deferrable: Run sensor in deferrable mode
    """

    template_fields: Sequence[str] = (
        "bucket",
        "prefix",
        "impersonation_chain",
    )
    ui_color = "#f0eee4"

    def __init__(
        self,
        bucket: str,
        prefix: str,
        inactivity_period: float = 60 * 60,
        min_objects: int = 1,
        previous_objects: set[str] | None = None,
        allow_delete: bool = True,
        google_cloud_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.bucket = bucket
        self.prefix = prefix
        if inactivity_period < 0:
            raise ValueError("inactivity_period must be non-negative")
        self.inactivity_period = inactivity_period
        self.min_objects = min_objects
        self.previous_objects = previous_objects or set()
        self.inactivity_seconds = 0
        self.allow_delete = allow_delete
        self.google_cloud_conn_id = google_cloud_conn_id
        self.last_activity_time = None
        self.impersonation_chain = impersonation_chain
        self.hook: GCSHook | None = None
        self.deferrable = deferrable

    def _get_gcs_hook(self) -> GCSHook | None:
        if not self.hook:
            self.hook = GCSHook(
                gcp_conn_id=self.google_cloud_conn_id,
                impersonation_chain=self.impersonation_chain,
            )
        return self.hook

    def is_bucket_updated(self, current_objects: set[str]) -> bool:
        """
        Check whether new objects have been added and the inactivity_period has passed, and update the state.

        :param current_objects: set of object ids in bucket during last poke.
        """
        current_num_objects = len(current_objects)
        if current_objects > self.previous_objects:
            # When new objects arrived, reset the inactivity_seconds
            # and update previous_objects for the next poke.
            self.log.info(
                "New objects found at %s resetting last_activity_time.",
                os.path.join(self.bucket, self.prefix),
            )
            self.log.debug("New objects: %s", "\n".join(current_objects - self.previous_objects))
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
                    textwrap.dedent(
                        """\
                    Objects were deleted during the last
                    poke interval. Updating the file counter and
                    resetting last_activity_time.
                    %s\
                    """
                    ),
                    self.previous_objects - current_objects,
                )
                return False

            message = (
                "Illegal behavior: objects were deleted in "
                f"{os.path.join(self.bucket, self.prefix)} between pokes."
            )
            raise AirflowException(message)

        if self.last_activity_time:
            self.inactivity_seconds = (get_time() - self.last_activity_time).total_seconds()
        else:
            # Handles the first poke where last inactivity time is None.
            self.last_activity_time = get_time()
            self.inactivity_seconds = 0

        if self.inactivity_seconds >= self.inactivity_period:
            path = os.path.join(self.bucket, self.prefix)

            if current_num_objects >= self.min_objects:
                self.log.info(
                    textwrap.dedent(
                        """\
                        SUCCESS:
                        Sensor found %s objects at %s.
                        Waited at least %s seconds, with no new objects dropped.
                        """
                    ),
                    current_num_objects,
                    path,
                    self.inactivity_period,
                )
                return True

            self.log.error("FAILURE: Inactivity Period passed, not enough objects found in %s", path)

            return False
        return False

    def poke(self, context: Context) -> bool:
        return self.is_bucket_updated(
            set(self._get_gcs_hook().list(self.bucket, prefix=self.prefix))  # type: ignore[union-attr]
        )

    def execute(self, context: Context) -> None:
        """Airflow runs this method on the worker and defers using the trigger."""
        hook_params = {"impersonation_chain": self.impersonation_chain}

        if not self.deferrable:
            return super().execute(context)

        if not self.poke(context=context):
            self.defer(
                timeout=timedelta(seconds=self.timeout),
                trigger=GCSUploadSessionTrigger(
                    bucket=self.bucket,
                    prefix=self.prefix,
                    poke_interval=self.poke_interval,
                    google_cloud_conn_id=self.google_cloud_conn_id,
                    inactivity_period=self.inactivity_period,
                    min_objects=self.min_objects,
                    previous_objects=self.previous_objects,
                    allow_delete=self.allow_delete,
                    hook_params=hook_params,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context: dict[str, Any], event: dict[str, str] | None = None) -> str:
        """
        Rely on trigger to throw an exception, otherwise it assumes execution was successful.

        Callback for when the trigger fires - returns immediately.

        """
        if event:
            if event["status"] == "success":
                return event["message"]
            raise AirflowException(event["message"])

        message = "No event received in trigger callback"
        raise AirflowException(message)
