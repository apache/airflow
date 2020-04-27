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

"""Operators that integrates with Google Cloud Build service."""

from typing import Dict, Optional, Sequence, Tuple, Union

from google.api_core.retry import Retry
from google.cloud.devtools.cloudbuild_v1.types import Build, BuildTrigger, RepoSource
from google.protobuf.json_format import MessageToDict

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.cloud_build import CloudBuildHook  # noqa
from airflow.utils.decorators import apply_defaults


class CloudBuildCancelBuildOperator(BaseOperator):
    """
    Cancels a build in progress.

    :param id_: The ID of the build.
    :type id_: str
    :param project_id: Optional, Google Cloud Project project_id where the function belongs.
        If set to None or missing, the default project_id from the GCP connection is used.
    :type project_id: Optional[str]
    :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :type retry: Optional[Retry]
    :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :type timeout: Optional[float]
    :param metadata: Optional, additional metadata that is provided to the method.
    :type metadata: Optional[Sequence[Tuple[str, str]]]
    :param gcp_conn_id: Optional, the connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: Optional[str]

    :rtype: dict
    """

    template_fields = ("project_id", "id_", "gcp_conn_id")

    @apply_defaults
    def __init__(
        self,
        id_: str,
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.id_ = id_
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = CloudBuildHook(gcp_conn_id=self.gcp_conn_id)
        result = hook.cancel_build(
            id_=self.id_,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return MessageToDict(result)


class CloudBuildCreateBuildOperator(BaseOperator):
    """
    Starts a build with the specified configuration.

    :param build: The build resource to create. If a dict is provided, it must be of the same form
        as the protobuf message `google.cloud.devtools.cloudbuild_v1.types.Build`
    :type build: Union[dict, `google.cloud.devtools.cloudbuild_v1.types.Build`]
    :param project_id: Optional, Google Cloud Project project_id where the function belongs.
        If set to None or missing, the default project_id from the GCP connection is used.
    :type project_id: Optional[str]
    :param wait: Optional, wait for operation to finish.
    :type wait: Optional[bool]
    :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :type retry: Optional[Retry]
    :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :type timeout: Optional[float]
    :param metadata: Optional, additional metadata that is provided to the method.
    :type metadata: Optional[Sequence[Tuple[str, str]]]
    :param gcp_conn_id: Optional, the connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: Optional[str]

    :rtype: dict
    """

    template_fields = ("project_id", "build", "gcp_conn_id")

    @apply_defaults
    def __init__(
        self,
        build: Union[Dict, Build],
        project_id: Optional[str] = None,
        wait: bool = True,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.build = build
        self.project_id = project_id
        self.wait = wait
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = CloudBuildHook(gcp_conn_id=self.gcp_conn_id)
        result = hook.create_build(
            build=self.build,
            project_id=self.project_id,
            wait=self.wait,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return MessageToDict(result)


class CloudBuildCreateBuildTriggerOperator(BaseOperator):
    """
    Creates a new BuildTrigger.

    :param trigger: The BuildTrigger to create. If a dict is provided, it must be of the same form
        as the protobuf message `google.cloud.devtools.cloudbuild_v1.types.BuildTrigger`
    :type trigger: Union[dict, `google.cloud.devtools.cloudbuild_v1.types.BuildTrigger`]
    :param project_id: Optional, Google Cloud Project project_id where the function belongs.
        If set to None or missing, the default project_id from the GCP connection is used.
    :type project_id: Optional[str]
    :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :type retry: Optional[Retry]
    :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :type timeout: Optional[float]
    :param metadata: Optional, additional metadata that is provided to the method.
    :type metadata: Optional[Sequence[Tuple[str, str]]]
    :param gcp_conn_id: Optional, the connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: Optional[str]

    :rtype: dict
    """

    template_fields = ("project_id", "trigger", "gcp_conn_id")

    @apply_defaults
    def __init__(
        self,
        trigger: Union[dict, BuildTrigger],
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.trigger = trigger
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = CloudBuildHook(gcp_conn_id=self.gcp_conn_id)
        result = hook.create_build_trigger(
            trigger=self.trigger,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return MessageToDict(result)


class CloudBuildDeleteBuildTriggerOperator(BaseOperator):
    """
    Deletes a BuildTrigger by its project ID and trigger ID.

    :param trigger_id: The ID of the BuildTrigger to delete.
    :type trigger_id: str
    :param project_id: Optional, Google Cloud Project project_id where the function belongs.
        If set to None or missing, the default project_id from the GCP connection is used.
    :type project_id: Optional[str]
    :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :type retry: Optional[Retry]
    :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :type timeout: Optional[float]
    :param metadata: Optional, additional metadata that is provided to the method.
    :type metadata: Optional[Sequence[Tuple[str, str]]]
    :param gcp_conn_id: Optional, the connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: Optional[str]
    """

    template_fields = ("project_id", "trigger_id", "gcp_conn_id")

    @apply_defaults
    def __init__(
        self,
        trigger_id: str,
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.trigger_id = trigger_id
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = CloudBuildHook(gcp_conn_id=self.gcp_conn_id)
        hook.delete_build_trigger(
            trigger_id=self.trigger_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class CloudBuildGetBuildOperator(BaseOperator):
    """
    Returns information about a previously requested build.

    :param id_: The ID of the build.
    :type id_: str
    :param project_id: Optional, Google Cloud Project project_id where the function belongs.
        If set to None or missing, the default project_id from the GCP connection is used.
    :type project_id: Optional[str]
    :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :type retry: Optional[Retry]
    :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :type timeout: Optional[float]
    :param metadata: Optional, additional metadata that is provided to the method.
    :type metadata: Optional[Sequence[Tuple[str, str]]]
    :param gcp_conn_id: Optional, the connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: Optional[str]

    :rtype: dict
    """
    template_fields = ("project_id", "id_", "gcp_conn_id")

    @apply_defaults
    def __init__(
        self,
        id_: str,
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.id_ = id_
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = CloudBuildHook(gcp_conn_id=self.gcp_conn_id)
        result = hook.get_build(
            id_=self.id_,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return MessageToDict(result)


class CloudBuildGetBuildTriggerOperator(BaseOperator):
    """
    Returns information about a BuildTrigger.

    :param trigger_id: The ID of the BuildTrigger to get.
    :type trigger_id: str
    :param project_id: Optional, Google Cloud Project project_id where the function belongs.
        If set to None or missing, the default project_id from the GCP connection is used.
    :type project_id: Optional[str]
    :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :type retry: Optional[Retry]
    :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :type timeout: Optional[float]
    :param metadata: Optional, additional metadata that is provided to the method.
    :type metadata: Optional[Sequence[Tuple[str, str]]]
    :param gcp_conn_id: Optional, the connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: Optional[str]

    :rtype: dict
    """

    template_fields = ("project_id", "trigger_id", "gcp_conn_id")

    @apply_defaults
    def __init__(
        self,
        trigger_id: str,
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.trigger_id = trigger_id
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = CloudBuildHook(gcp_conn_id=self.gcp_conn_id)
        result = hook.get_build_trigger(
            trigger_id=self.trigger_id,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return MessageToDict(result)


class CloudBuildListBuildTriggersOperator(BaseOperator):
    """
    Lists existing BuildTriggers.

    :param project_id: Optional, Google Cloud Project project_id where the function belongs.
        If set to None or missing, the default project_id from the GCP connection is used.
    :type project_id: Optional[str]
    :param page_size: Optional, number of results to return in the list.
    :type page_size: Optional[int]
    :param page_token: Optional, token to provide to skip to a particular spot in the list.
    :type page_token: Optional[str]
    :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :type retry: Optional[Retry]
    :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :type timeout: Optional[float]
    :param metadata: Optional, additional metadata that is provided to the method.
    :type metadata: Optional[Sequence[Tuple[str, str]]]
    :param gcp_conn_id: Optional, the connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: Optional[str]

    :rtype: dict
    """

    template_fields = ("project_id", "gcp_conn_id")

    @apply_defaults
    def __init__(
        self,
        project_id: Optional[str] = None,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.page_size = page_size
        self.page_token = page_token
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = CloudBuildHook(gcp_conn_id=self.gcp_conn_id)
        result = hook.list_build_triggers(
            project_id=self.project_id,
            page_size=self.page_size,
            page_token=self.page_token,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return MessageToDict(result)


class CloudBuildListBuildsOperator(BaseOperator):
    """
    Lists previously requested builds.

    :param project_id: Optional, Google Cloud Project project_id where the function belongs.
        If set to None or missing, the default project_id from the GCP connection is used.
    :type project_id: str
    :param page_size: Optional, number of results to return in the list.
    :type page_size: Optional[int]
    :param filter_: Optional, the raw filter text to constrain the results.
    :type filter_: Optional[str]
    :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :type retry: Optional[Retry]
    :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :type timeout: Optional[float]
    :param metadata: Optional, additional metadata that is provided to the method.
    :type metadata: Optional[Sequence[Tuple[str, str]]]
    :param gcp_conn_id: Optional, the connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: Optional[str]

    :rtype: List[dict]
    """

    template_fields = ("project_id", "gcp_conn_id")

    @apply_defaults
    def __init__(
        self,
        project_id: Optional[str] = None,
        page_size: Optional[int] = None,
        filter_: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.project_id = project_id
        self.page_size = page_size
        self.filter_ = filter_
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = CloudBuildHook(gcp_conn_id=self.gcp_conn_id)
        results = hook.list_builds(
            project_id=self.project_id,
            page_size=self.page_size,
            filter_=self.filter_,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return [MessageToDict(result) for result in results]


class CloudBuildRetryBuildOperator(BaseOperator):
    """
    Creates a new build based on the specified build. This method creates a new build
    using the original build request, which may or may not result in an identical build.

    :param id_: Build ID of the original build.
    :type id_: str
    :param project_id: Optional, Google Cloud Project project_id where the function belongs.
        If set to None or missing, the default project_id from the GCP connection is used.
    :type project_id: str
    :param wait: Optional, wait for operation to finish.
    :type wait: Optional[bool]
    :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :type retry: Optional[Retry]
    :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :type timeout: Optional[float]
    :param metadata: Optional, additional metadata that is provided to the method.
    :type metadata: Optional[Sequence[Tuple[str, str]]]
    :param gcp_conn_id: Optional, the connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: Optional[str]

    :rtype: dict
    """

    template_fields = ("project_id", "id_", "gcp_conn_id")

    @apply_defaults
    def __init__(
        self,
        id_: str,
        project_id: Optional[str] = None,
        wait: bool = True,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.id_ = id_
        self.project_id = project_id
        self.wait = wait
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = CloudBuildHook(gcp_conn_id=self.gcp_conn_id)
        result = hook.retry_build(
            id_=self.id_,
            project_id=self.project_id,
            wait=self.wait,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return MessageToDict(result)


class CloudBuildRunBuildTriggerOperator(BaseOperator):
    """
    Runs a BuildTrigger at a particular source revision.

    :param trigger_id: The ID of the trigger.
    :type trigger_id: str
    :param source: Source to build against this trigger. If a dict is provided, it must be of the same form
        as the protobuf message `google.cloud.devtools.cloudbuild_v1.types.RepoSource`
    :type source: Union[dict, `google.cloud.devtools.cloudbuild_v1.types.RepoSource`]
    :param project_id: Optional, Google Cloud Project project_id where the function belongs.
        If set to None or missing, the default project_id from the GCP connection is used.
    :type project_id: str
    :param wait: Optional, wait for operation to finish.
    :type wait: Optional[bool]
    :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :type retry: Optional[Retry]
    :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :type timeout: Optional[float]
    :param metadata: Optional, additional metadata that is provided to the method.
    :type metadata: Optional[Sequence[Tuple[str, str]]]
    :param gcp_conn_id: Optional, the connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: Optional[str]

    :rtype: dict
    """

    template_fields = ("project_id", "trigger_id", "source", "gcp_conn_id")

    @apply_defaults
    def __init__(
        self,
        trigger_id: str,
        source: Union[dict, RepoSource],
        project_id: Optional[str] = None,
        wait: bool = True,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.trigger_id = trigger_id
        self.source = source
        self.project_id = project_id
        self.wait = wait
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = CloudBuildHook(gcp_conn_id=self.gcp_conn_id)
        result = hook.run_build_trigger(
            trigger_id=self.trigger_id,
            source=self.source,
            project_id=self.project_id,
            wait=self.wait,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return MessageToDict(result)


class CloudBuildUpdateBuildTriggerOperator(BaseOperator):
    """
    Updates a BuildTrigger by its project ID and trigger ID.

    :param trigger_id: The ID of the trigger.
    :type trigger_id: str
    :param trigger: The BuildTrigger to create. If a dict is provided, it must be of the same form
        as the protobuf message `google.cloud.devtools.cloudbuild_v1.types.BuildTrigger`
    :type trigger: Union[dict, `google.cloud.devtools.cloudbuild_v1.types.BuildTrigger`]
    :param project_id: Optional, Google Cloud Project project_id where the function belongs.
        If set to None or missing, the default project_id from the GCP connection is used.
    :type project_id: Optional[str]
    :param retry: Optional, a retry object used  to retry requests. If `None` is specified, requests
        will not be retried.
    :type retry: Optional[Retry]
    :param timeout: Optional, the amount of time, in seconds, to wait for the request to complete.
        Note that if `retry` is specified, the timeout applies to each individual attempt.
    :type timeout: Optional[float]
    :param metadata: Optional, additional metadata that is provided to the method.
    :type metadata: Optional[Sequence[Tuple[str, str]]]
    :param gcp_conn_id: Optional, the connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: Optional[str]

    :rtype: dict
    """

    template_fields = ("project_id", "trigger_id", "trigger", "gcp_conn_id")

    @apply_defaults
    def __init__(
        self,
        trigger_id: str,
        trigger: Union[dict, BuildTrigger],
        project_id: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Optional[Sequence[Tuple[str, str]]] = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.trigger_id = trigger_id
        self.trigger = trigger
        self.project_id = project_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context):
        hook = CloudBuildHook(gcp_conn_id=self.gcp_conn_id)
        result = hook.update_build_trigger(
            trigger_id=self.trigger_id,
            trigger=self.trigger,
            project_id=self.project_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return MessageToDict(result)
