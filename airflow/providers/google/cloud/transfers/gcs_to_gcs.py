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
"""This module contains a Google Cloud Storage operator."""
from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

WILDCARD = "*"

if TYPE_CHECKING:
    from airflow.utils.context import Context


class GCSToGCSOperator(BaseOperator):
    """
    Copies objects from a bucket to another, with renaming if requested.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GCSToGCSOperator`

    :param source_bucket: The source Google Cloud Storage bucket where the
         object is. (templated)
    :param source_object: The source name of the object to copy in the Google cloud
        storage bucket. (templated)
        You can use only one wildcard for objects (filenames) within your
        bucket. The wildcard can appear inside the object name or at the
        end of the object name. Appending a wildcard to the bucket name is
        unsupported.
    :param source_objects: A list of source name of the objects to copy in the Google cloud
        storage bucket. (templated)
    :param destination_bucket: The destination Google Cloud Storage bucket
        where the object should be. If the destination_bucket is None, it defaults
        to source_bucket. (templated)
    :param destination_object: The destination name of the object in the
        destination Google Cloud Storage bucket. (templated)
        If a wildcard is supplied in the source_object argument, this is the
        prefix that will be prepended to the final destination objects' paths.
        Note that the source path's part before the wildcard will be removed;
        if it needs to be retained it should be appended to destination_object.
        For example, with prefix ``foo/*`` and destination_object ``blah/``, the
        file ``foo/baz`` will be copied to ``blah/baz``; to retain the prefix write
        the destination_object as e.g. ``blah/foo``, in which case the copied file
        will be named ``blah/foo/baz``.
        The same thing applies to source objects inside source_objects.
    :param move_object: When move object is True, the object is moved instead
        of copied to the new location. This is the equivalent of a mv command
        as opposed to a cp command.
    :param replace: Whether you want to replace existing destination files or not.
    :param delimiter: This is used to restrict the result to only the 'files' in a given 'folder'.
        If source_objects = ['foo/bah/'] and delimiter = '.avro', then only the 'files' in the
        folder 'foo/bah/' with '.avro' delimiter will be copied to the destination object.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param last_modified_time: When specified, the objects will be copied or moved,
        only if they were modified after last_modified_time.
        If tzinfo has not been set, UTC will be assumed.
    :param maximum_modified_time: When specified, the objects will be copied or moved,
        only if they were modified before maximum_modified_time.
        If tzinfo has not been set, UTC will be assumed.
    :param is_older_than: When specified, the objects will be copied if they are older
        than the specified time in seconds.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param source_object_required: Whether you want to raise an exception when the source object
        doesn't exist. It doesn't have any effect when the source objects are folders or patterns.
    :param exact_match: When specified, only exact match of the source object (filename) will be
        copied.

    :Example:

    The following Operator would copy a single file named
    ``sales/sales-2017/january.avro`` in the ``data`` bucket to the file named
    ``copied_sales/2017/january-backup.avro`` in the ``data_backup`` bucket ::

        copy_single_file = GCSToGCSOperator(
            task_id='copy_single_file',
            source_bucket='data',
            source_objects=['sales/sales-2017/january.avro'],
            destination_bucket='data_backup',
            destination_object='copied_sales/2017/january-backup.avro',
            gcp_conn_id=google_cloud_conn_id
        )

    The following Operator would copy all the Avro files from ``sales/sales-2017``
    folder (i.e. with names starting with that prefix) in ``data`` bucket to the
    ``copied_sales/2017`` folder in the ``data_backup`` bucket. ::

        copy_files = GCSToGCSOperator(
            task_id='copy_files',
            source_bucket='data',
            source_objects=['sales/sales-2017'],
            destination_bucket='data_backup',
            destination_object='copied_sales/2017/',
            delimiter='.avro'
            gcp_conn_id=google_cloud_conn_id
        )

        Or ::

        copy_files = GCSToGCSOperator(
            task_id='copy_files',
            source_bucket='data',
            source_object='sales/sales-2017/*.avro',
            destination_bucket='data_backup',
            destination_object='copied_sales/2017/',
            gcp_conn_id=google_cloud_conn_id
        )

    The following Operator would move all the Avro files from ``sales/sales-2017``
    folder (i.e. with names starting with that prefix) in ``data`` bucket to the
    same folder in the ``data_backup`` bucket, deleting the original files in the
    process. ::

        move_files = GCSToGCSOperator(
            task_id='move_files',
            source_bucket='data',
            source_object='sales/sales-2017/*.avro',
            destination_bucket='data_backup',
            move_object=True,
            gcp_conn_id=google_cloud_conn_id
        )

    The following Operator would move all the Avro files from ``sales/sales-2019``
     and ``sales/sales-2020` folder in ``data`` bucket to the same folder in the
     ``data_backup`` bucket, deleting the original files in the process. ::

        move_files = GCSToGCSOperator(
            task_id='move_files',
            source_bucket='data',
            source_objects=['sales/sales-2019/*.avro', 'sales/sales-2020'],
            destination_bucket='data_backup',
            delimiter='.avro',
            move_object=True,
            gcp_conn_id=google_cloud_conn_id
        )

    """

    template_fields: Sequence[str] = (
        "source_bucket",
        "source_object",
        "source_objects",
        "destination_bucket",
        "destination_object",
        "delimiter",
        "impersonation_chain",
    )
    ui_color = "#f0eee4"

    def __init__(
        self,
        *,
        source_bucket,
        source_object=None,
        source_objects=None,
        destination_bucket=None,
        destination_object=None,
        delimiter=None,
        move_object=False,
        replace=True,
        gcp_conn_id="google_cloud_default",
        delegate_to=None,
        last_modified_time=None,
        maximum_modified_time=None,
        is_older_than=None,
        impersonation_chain: str | Sequence[str] | None = None,
        source_object_required=False,
        exact_match=False,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.source_bucket = source_bucket
        self.source_object = source_object
        self.source_objects = source_objects
        self.destination_bucket = destination_bucket
        self.destination_object = destination_object
        self.delimiter = delimiter
        self.move_object = move_object
        self.replace = replace
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.last_modified_time = last_modified_time
        self.maximum_modified_time = maximum_modified_time
        self.is_older_than = is_older_than
        self.impersonation_chain = impersonation_chain
        self.source_object_required = source_object_required
        self.exact_match = exact_match

    def execute(self, context: Context):

        hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        if self.source_objects and self.source_object:
            error_msg = (
                f"You can either set source_object parameter or source_objects parameter but not both. "
                f"Found source_object={self.source_object} and source_objects={self.source_objects}"
            )
            raise AirflowException(error_msg)

        if not self.source_object and not self.source_objects:
            error_msg = "You must set source_object parameter or source_objects parameter. None set"
            raise AirflowException(error_msg)

        if self.source_objects and not all(isinstance(item, str) for item in self.source_objects):
            raise AirflowException("At least, one of the `objects` in the `source_objects` is not a string")

        # If source_object is set, default it to source_objects
        if self.source_object:
            self.source_objects = [self.source_object]

        if self.destination_bucket is None:
            self.log.warning(
                "destination_bucket is None. Defaulting it to source_bucket (%s)", self.source_bucket
            )
            self.destination_bucket = self.source_bucket

        # An empty source_object means to copy all files
        if len(self.source_objects) == 0:
            self.source_objects = [""]
        # Raise exception if empty string `''` is used twice in source_object, this is to avoid double copy
        if self.source_objects.count("") > 1:
            raise AirflowException("You can't have two empty strings inside source_object")

        # Iterate over the source_objects and do the copy
        for prefix in self.source_objects:
            # Check if prefix contains wildcard
            if WILDCARD in prefix:
                self._copy_source_with_wildcard(hook=hook, prefix=prefix)
            # Now search with prefix using provided delimiter if any
            else:
                self._copy_source_without_wildcard(hook=hook, prefix=prefix)

    def _ignore_existing_files(self, hook, prefix, **kwargs):
        # list all files in the Destination GCS bucket
        # and only keep those files which are present in
        # Source GCS bucket and not in Destination GCS bucket
        delimiter = kwargs.get("delimiter")
        objects = kwargs.get("objects")
        if self.destination_object is None:
            existing_objects = hook.list(self.destination_bucket, prefix=prefix, delimiter=delimiter)
        else:
            self.log.info("Replaced destination_object with source_object prefix.")
            destination_objects = hook.list(
                self.destination_bucket,
                prefix=self.destination_object,
                delimiter=delimiter,
            )
            existing_objects = [
                dest_object.replace(self.destination_object, prefix, 1) for dest_object in destination_objects
            ]

        objects = set(objects) - set(existing_objects)
        if len(objects) > 0:
            self.log.info("%s files are going to be synced: %s.", len(objects), objects)
        else:
            self.log.info("There are no new files to sync. Have a nice day!")
        return objects

    def _copy_source_without_wildcard(self, hook, prefix):
        """
        For source_objects with no wildcard, this operator would first list
        all files in source_objects, using provided delimiter if any. Then copy
        files from source_objects to destination_object and rename each source
        file.

        Example 1:


        The following Operator would copy all the files from ``a/``folder
        (i.e a/a.csv, a/b.csv, a/c.csv)in ``data`` bucket to the ``b/`` folder in
        the ``data_backup`` bucket (b/a.csv, b/b.csv, b/c.csv) ::

            copy_files = GCSToGCSOperator(
                task_id='copy_files_without_wildcard',
                source_bucket='data',
                source_objects=['a/'],
                destination_bucket='data_backup',
                destination_object='b/',
                gcp_conn_id=google_cloud_conn_id
            )

        Example 2:


        The following Operator would copy all avro files from ``a/``folder
        (i.e a/a.avro, a/b.avro, a/c.avro)in ``data`` bucket to the ``b/`` folder in
        the ``data_backup`` bucket (b/a.avro, b/b.avro, b/c.avro) ::

            copy_files = GCSToGCSOperator(
                task_id='copy_files_without_wildcard',
                source_bucket='data',
                source_objects=['a/'],
                destination_bucket='data_backup',
                destination_object='b/',
                delimiter='.avro',
                gcp_conn_id=google_cloud_conn_id
            )
        """
        objects = hook.list(self.source_bucket, prefix=prefix, delimiter=self.delimiter)

        if not self.replace:
            # If we are not replacing, ignore files already existing in source buckets
            objects = self._ignore_existing_files(hook, prefix, objects=objects, delimiter=self.delimiter)

        # If objects is empty and we have prefix, let's check if prefix is a blob
        # and copy directly
        if len(objects) == 0 and prefix:
            if hook.exists(self.source_bucket, prefix):
                self._copy_single_object(
                    hook=hook, source_object=prefix, destination_object=self.destination_object
                )
            elif self.source_object_required:
                msg = f"{prefix} does not exist in bucket {self.source_bucket}"
                self.log.warning(msg)
                raise AirflowException(msg)

        for source_obj in objects:
            if self.exact_match and (source_obj != prefix or not source_obj.endswith(prefix)):
                continue
            if self.destination_object is None:
                destination_object = source_obj
            else:
                destination_object = source_obj.replace(prefix, self.destination_object, 1)
            self._copy_single_object(
                hook=hook, source_object=source_obj, destination_object=destination_object
            )

    def _copy_source_with_wildcard(self, hook, prefix):
        total_wildcards = prefix.count(WILDCARD)
        if total_wildcards > 1:
            error_msg = (
                "Only one wildcard '*' is allowed in source_object parameter. "
                f"Found {total_wildcards} in {prefix}."
            )

            raise AirflowException(error_msg)
        self.log.info("Delimiter ignored because wildcard is in prefix")
        prefix_, delimiter = prefix.split(WILDCARD, 1)
        objects = hook.list(self.source_bucket, prefix=prefix_, delimiter=delimiter)
        if not self.replace:
            # If we are not replacing, list all files in the Destination GCS bucket
            # and only keep those files which are present in
            # Source GCS bucket and not in Destination GCS bucket
            objects = self._ignore_existing_files(hook, prefix_, delimiter=delimiter, objects=objects)

        for source_object in objects:
            if self.destination_object is None:
                destination_object = source_object
            else:
                destination_object = source_object.replace(prefix_, self.destination_object, 1)

            self._copy_single_object(
                hook=hook, source_object=source_object, destination_object=destination_object
            )

    def _copy_single_object(self, hook, source_object, destination_object):
        if self.is_older_than:
            # Here we check if the given object is older than the given time
            # If given, last_modified_time and maximum_modified_time is ignored
            if hook.is_older_than(self.source_bucket, source_object, self.is_older_than):
                self.log.info("Object is older than %s seconds ago", self.is_older_than)
            else:
                self.log.debug("Object is not older than %s seconds ago", self.is_older_than)
                return
        elif self.last_modified_time and self.maximum_modified_time:
            # check to see if object was modified between last_modified_time and
            # maximum_modified_time
            if hook.is_updated_between(
                self.source_bucket, source_object, self.last_modified_time, self.maximum_modified_time
            ):
                self.log.info(
                    "Object has been modified between %s and %s",
                    self.last_modified_time,
                    self.maximum_modified_time,
                )
            else:
                self.log.debug(
                    "Object was not modified between %s and %s",
                    self.last_modified_time,
                    self.maximum_modified_time,
                )
                return

        elif self.last_modified_time is not None:
            # Check to see if object was modified after last_modified_time
            if hook.is_updated_after(self.source_bucket, source_object, self.last_modified_time):
                self.log.info("Object has been modified after %s ", self.last_modified_time)
            else:
                self.log.debug("Object was not modified after %s ", self.last_modified_time)
                return
        elif self.maximum_modified_time is not None:
            # Check to see if object was modified before maximum_modified_time
            if hook.is_updated_before(self.source_bucket, source_object, self.maximum_modified_time):
                self.log.info("Object has been modified before %s ", self.maximum_modified_time)
            else:
                self.log.debug("Object was not modified before %s ", self.maximum_modified_time)
                return

        self.log.info(
            "Executing copy of gs://%s/%s to gs://%s/%s",
            self.source_bucket,
            source_object,
            self.destination_bucket,
            destination_object,
        )

        hook.rewrite(self.source_bucket, source_object, self.destination_bucket, destination_object)

        if self.move_object:
            hook.delete(self.source_bucket, source_object)
