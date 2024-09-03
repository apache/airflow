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
"""This module contains a Google Dataprep operator."""

from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from airflow.providers.google.cloud.hooks.dataprep import GoogleDataprepHook
from airflow.providers.google.cloud.links.dataprep import DataprepFlowLink, DataprepJobGroupLink
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DataprepGetJobsForJobGroupOperator(GoogleCloudBaseOperator):
    """
    Get information about the batch jobs within a Cloud Dataprep job.

    API documentation: https://clouddataprep.com/documentation/api#section/Overview.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataprepGetJobsForJobGroupOperator`

    :param job_group_id The ID of the job group that will be requests
    """

    template_fields: Sequence[str] = ("job_group_id",)

    def __init__(
        self,
        *,
        dataprep_conn_id: str = "dataprep_default",
        job_group_id: int | str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dataprep_conn_id = dataprep_conn_id
        self.job_group_id = job_group_id

    def execute(self, context: Context) -> dict:
        self.log.info("Fetching data for job with id: %d ...", self.job_group_id)
        hook = GoogleDataprepHook(
            dataprep_conn_id=self.dataprep_conn_id,
        )
        response = hook.get_jobs_for_job_group(job_id=int(self.job_group_id))
        return response


class DataprepGetJobGroupOperator(GoogleCloudBaseOperator):
    """
    Get the specified job group.

    A job group is a job that is executed from a specific node in a flow.

    API documentation: https://clouddataprep.com/documentation/api#section/Overview.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataprepGetJobGroupOperator`

    :param job_group_id: The ID of the job group that will be requests
    :param embed: Comma-separated list of objects to pull in as part of the response
    :param include_deleted: if set to "true", will include deleted objects
    """

    template_fields: Sequence[str] = (
        "job_group_id",
        "embed",
        "project_id",
    )
    operator_extra_links = (DataprepJobGroupLink(),)

    def __init__(
        self,
        *,
        dataprep_conn_id: str = "dataprep_default",
        project_id: str = PROVIDE_PROJECT_ID,
        job_group_id: int | str,
        embed: str,
        include_deleted: bool,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dataprep_conn_id: str = dataprep_conn_id
        self.project_id = project_id
        self.job_group_id = job_group_id
        self.embed = embed
        self.include_deleted = include_deleted

    def execute(self, context: Context) -> dict:
        self.log.info("Fetching data for job with id: %d ...", self.job_group_id)

        if self.project_id:
            DataprepJobGroupLink.persist(
                context=context,
                task_instance=self,
                project_id=self.project_id,
                job_group_id=int(self.job_group_id),
            )

        hook = GoogleDataprepHook(dataprep_conn_id=self.dataprep_conn_id)
        response = hook.get_job_group(
            job_group_id=int(self.job_group_id),
            embed=self.embed,
            include_deleted=self.include_deleted,
        )
        return response


class DataprepRunJobGroupOperator(GoogleCloudBaseOperator):
    """
    Create a ``jobGroup``, which launches the specified job as the authenticated user.

    This performs the same action as clicking on the Run Job button in the application.

    To get recipe_id please follow the Dataprep API documentation:
    https://clouddataprep.com/documentation/api#operation/runJobGroup.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DataprepRunJobGroupOperator`

    :param dataprep_conn_id: The Dataprep connection ID
    :param body_request:  Passed as the body_request to GoogleDataprepHook's run_job_group,
        where it's the identifier for the recipe to run
    """

    template_fields: Sequence[str] = ("body_request",)
    operator_extra_links = (DataprepJobGroupLink(),)

    def __init__(
        self,
        *,
        project_id: str = PROVIDE_PROJECT_ID,
        dataprep_conn_id: str = "dataprep_default",
        body_request: dict,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.dataprep_conn_id = dataprep_conn_id
        self.body_request = body_request

    def execute(self, context: Context) -> dict:
        self.log.info("Creating a job...")
        hook = GoogleDataprepHook(dataprep_conn_id=self.dataprep_conn_id)
        response = hook.run_job_group(body_request=self.body_request)

        job_group_id = response.get("id")
        if self.project_id and job_group_id:
            DataprepJobGroupLink.persist(
                context=context,
                task_instance=self,
                project_id=self.project_id,
                job_group_id=int(job_group_id),
            )

        return response


class DataprepCopyFlowOperator(GoogleCloudBaseOperator):
    """
    Create a copy of the provided flow id, as well as all contained recipes.

    :param dataprep_conn_id: The Dataprep connection ID
    :param flow_id: ID of the flow to be copied
    :param name: Name for the copy of the flow
    :param description: Description of the copy of the flow
    :param copy_datasources: Bool value to define should the copy of data inputs be made or not.
    """

    template_fields: Sequence[str] = (
        "flow_id",
        "name",
        "project_id",
        "description",
    )
    operator_extra_links = (DataprepFlowLink(),)

    def __init__(
        self,
        *,
        project_id: str = PROVIDE_PROJECT_ID,
        dataprep_conn_id: str = "dataprep_default",
        flow_id: int | str,
        name: str = "",
        description: str = "",
        copy_datasources: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.dataprep_conn_id = dataprep_conn_id
        self.flow_id = flow_id
        self.name = name
        self.description = description
        self.copy_datasources = copy_datasources

    def execute(self, context: Context) -> dict:
        self.log.info("Copying flow with id %d...", self.flow_id)
        hook = GoogleDataprepHook(dataprep_conn_id=self.dataprep_conn_id)
        response = hook.copy_flow(
            flow_id=int(self.flow_id),
            name=self.name,
            description=self.description,
            copy_datasources=self.copy_datasources,
        )

        copied_flow_id = response.get("id")
        if self.project_id and copied_flow_id:
            DataprepFlowLink.persist(
                context=context,
                task_instance=self,
                project_id=self.project_id,
                flow_id=int(copied_flow_id),
            )
        return response


class DataprepDeleteFlowOperator(GoogleCloudBaseOperator):
    """
    Delete the flow with provided id.

    :param dataprep_conn_id: The Dataprep connection ID
    :param flow_id: ID of the flow to be copied
    """

    template_fields: Sequence[str] = ("flow_id",)

    def __init__(
        self,
        *,
        dataprep_conn_id: str = "dataprep_default",
        flow_id: int | str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dataprep_conn_id = dataprep_conn_id
        self.flow_id = flow_id

    def execute(self, context: Context) -> None:
        self.log.info("Start delete operation of the flow with id: %d...", self.flow_id)
        hook = GoogleDataprepHook(dataprep_conn_id=self.dataprep_conn_id)
        hook.delete_flow(flow_id=int(self.flow_id))


class DataprepRunFlowOperator(GoogleCloudBaseOperator):
    """
    Runs the flow with the provided id copy of the provided flow id.

    :param dataprep_conn_id: The Dataprep connection ID
    :param flow_id: ID of the flow to be copied
    :param body_request: Body of the POST request to be sent.
    """

    template_fields: Sequence[str] = (
        "flow_id",
        "project_id",
    )
    operator_extra_links = (DataprepJobGroupLink(),)

    def __init__(
        self,
        *,
        project_id: str = PROVIDE_PROJECT_ID,
        flow_id: int | str,
        body_request: dict,
        dataprep_conn_id: str = "dataprep_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.flow_id = flow_id
        self.body_request = body_request
        self.dataprep_conn_id = dataprep_conn_id

    def execute(self, context: Context) -> dict:
        self.log.info("Running the flow with id: %d...", self.flow_id)
        hooks = GoogleDataprepHook(dataprep_conn_id=self.dataprep_conn_id)
        response = hooks.run_flow(flow_id=int(self.flow_id), body_request=self.body_request)

        if self.project_id:
            job_group_id = response["data"][0]["id"]
            DataprepJobGroupLink.persist(
                context=context,
                task_instance=self,
                project_id=self.project_id,
                job_group_id=int(job_group_id),
            )

        return response
