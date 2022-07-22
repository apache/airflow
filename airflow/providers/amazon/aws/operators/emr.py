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
import ast
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Union
from uuid import uuid4

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.emr import EmrContainerHook, EmrHook
from airflow.providers.amazon.aws.links.emr import EmrClusterLink

if TYPE_CHECKING:
    from airflow.utils.context import Context


class EmrAddStepsOperator(BaseOperator):
    """
    An operator that adds steps to an existing EMR job_flow.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EmrAddStepsOperator`

    :param job_flow_id: id of the JobFlow to add steps to. (templated)
    :param job_flow_name: name of the JobFlow to add steps to. Use as an alternative to passing
        job_flow_id. will search for id of JobFlow with matching name in one of the states in
        param cluster_states. Exactly one cluster like this should exist or will fail. (templated)
    :param cluster_states: Acceptable cluster states when searching for JobFlow id by job_flow_name.
        (templated)
    :param aws_conn_id: aws connection to uses
    :param steps: boto3 style steps or reference to a steps file (must be '.json') to
        be added to the jobflow. (templated)
    :param do_xcom_push: if True, job_flow_id is pushed to XCom with key job_flow_id.
    """

    template_fields: Sequence[str] = ('job_flow_id', 'job_flow_name', 'cluster_states', 'steps')
    template_ext: Sequence[str] = ('.json',)
    template_fields_renderers = {"steps": "json"}
    ui_color = '#f9c915'
    operator_extra_links = (EmrClusterLink(),)

    def __init__(
        self,
        *,
        job_flow_id: Optional[str] = None,
        job_flow_name: Optional[str] = None,
        cluster_states: Optional[List[str]] = None,
        aws_conn_id: str = 'aws_default',
        steps: Optional[Union[List[dict], str]] = None,
        **kwargs,
    ):
        if not (job_flow_id is None) ^ (job_flow_name is None):
            raise AirflowException('Exactly one of job_flow_id or job_flow_name must be specified.')
        super().__init__(**kwargs)
        cluster_states = cluster_states or []
        steps = steps or []
        self.aws_conn_id = aws_conn_id
        self.job_flow_id = job_flow_id
        self.job_flow_name = job_flow_name
        self.cluster_states = cluster_states
        self.steps = steps

    def execute(self, context: 'Context') -> List[str]:
        emr_hook = EmrHook(aws_conn_id=self.aws_conn_id)

        emr = emr_hook.get_conn()

        job_flow_id = self.job_flow_id or emr_hook.get_cluster_id_by_name(
            str(self.job_flow_name), self.cluster_states
        )

        if not job_flow_id:
            raise AirflowException(f'No cluster found for name: {self.job_flow_name}')

        if self.do_xcom_push:
            context['ti'].xcom_push(key='job_flow_id', value=job_flow_id)

        EmrClusterLink.persist(
            context=context,
            operator=self,
            region_name=emr_hook.conn_region_name,
            aws_partition=emr_hook.conn_partition,
            job_flow_id=job_flow_id,
        )

        self.log.info('Adding steps to %s', job_flow_id)

        # steps may arrive as a string representing a list
        # e.g. if we used XCom or a file then: steps="[{ step1 }, { step2 }]"
        steps = self.steps
        if isinstance(steps, str):
            steps = ast.literal_eval(steps)

        response = emr.add_job_flow_steps(JobFlowId=job_flow_id, Steps=steps)

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException(f'Adding steps failed: {response}')
        else:
            self.log.info('Steps %s added to JobFlow', response['StepIds'])
            return response['StepIds']


class EmrContainerOperator(BaseOperator):
    """
    An operator that submits jobs to EMR on EKS virtual clusters.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EmrContainerOperator`

    :param name: The name of the job run.
    :param virtual_cluster_id: The EMR on EKS virtual cluster ID
    :param execution_role_arn: The IAM role ARN associated with the job run.
    :param release_label: The Amazon EMR release version to use for the job run.
    :param job_driver: Job configuration details, e.g. the Spark job parameters.
    :param configuration_overrides: The configuration overrides for the job run,
        specifically either application configuration or monitoring configuration.
    :param client_request_token: The client idempotency token of the job run request.
        Use this if you want to specify a unique ID to prevent two jobs from getting started.
        If no token is provided, a UUIDv4 token will be generated for you.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param wait_for_completion: Whether or not to wait in the operator for the job to complete.
    :param poll_interval: Time (in seconds) to wait between two consecutive calls to check query status on EMR
    :param max_tries: Maximum number of times to wait for the job run to finish.
        Defaults to None, which will poll until the job is *not* in a pending, submitted, or running state.
    :param tags: The tags assigned to job runs.
        Defaults to None
    """

    template_fields: Sequence[str] = (
        "name",
        "virtual_cluster_id",
        "execution_role_arn",
        "release_label",
        "job_driver",
    )
    ui_color = "#f9c915"

    def __init__(
        self,
        *,
        name: str,
        virtual_cluster_id: str,
        execution_role_arn: str,
        release_label: str,
        job_driver: dict,
        configuration_overrides: Optional[dict] = None,
        client_request_token: Optional[str] = None,
        aws_conn_id: str = "aws_default",
        wait_for_completion: bool = True,
        poll_interval: int = 30,
        max_tries: Optional[int] = None,
        tags: Optional[dict] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.virtual_cluster_id = virtual_cluster_id
        self.execution_role_arn = execution_role_arn
        self.release_label = release_label
        self.job_driver = job_driver
        self.configuration_overrides = configuration_overrides or {}
        self.aws_conn_id = aws_conn_id
        self.client_request_token = client_request_token or str(uuid4())
        self.wait_for_completion = wait_for_completion
        self.poll_interval = poll_interval
        self.max_tries = max_tries
        self.tags = tags
        self.job_id: Optional[str] = None

    @cached_property
    def hook(self) -> EmrContainerHook:
        """Create and return an EmrContainerHook."""
        return EmrContainerHook(
            self.aws_conn_id,
            virtual_cluster_id=self.virtual_cluster_id,
        )

    def execute(self, context: 'Context') -> Optional[str]:
        """Run job on EMR Containers"""
        self.job_id = self.hook.submit_job(
            self.name,
            self.execution_role_arn,
            self.release_label,
            self.job_driver,
            self.configuration_overrides,
            self.client_request_token,
            self.tags,
        )
        if self.wait_for_completion:
            query_status = self.hook.poll_query_status(self.job_id, self.max_tries, self.poll_interval)

            if query_status in EmrContainerHook.FAILURE_STATES:
                error_message = self.hook.get_job_failure_reason(self.job_id)
                raise AirflowException(
                    f"EMR Containers job failed. Final state is {query_status}. "
                    f"query_execution_id is {self.job_id}. Error: {error_message}"
                )
            elif not query_status or query_status in EmrContainerHook.INTERMEDIATE_STATES:
                raise AirflowException(
                    f"Final state of EMR Containers job is {query_status}. "
                    f"Max tries of poll status exceeded, query_execution_id is {self.job_id}."
                )

        return self.job_id

    def on_kill(self) -> None:
        """Cancel the submitted job run"""
        if self.job_id:
            self.log.info("Stopping job run with jobId - %s", self.job_id)
            response = self.hook.stop_query(self.job_id)
            http_status_code = None
            try:
                http_status_code = response["ResponseMetadata"]["HTTPStatusCode"]
            except Exception as ex:
                self.log.error("Exception while cancelling query: %s", ex)
            finally:
                if http_status_code is None or http_status_code != 200:
                    self.log.error("Unable to request query cancel on EMR. Exiting")
                else:
                    self.log.info(
                        "Polling EMR for query with id %s to reach final state",
                        self.job_id,
                    )
                    self.hook.poll_query_status(self.job_id)


class EmrCreateJobFlowOperator(BaseOperator):
    """
    Creates an EMR JobFlow, reading the config from the EMR connection.
    A dictionary of JobFlow overrides can be passed that override
    the config from the connection.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EmrCreateJobFlowOperator`

    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node)
    :param emr_conn_id: emr connection to use for run_job_flow request body.
        This will be overridden by the job_flow_overrides param
    :param job_flow_overrides: boto3 style arguments or reference to an arguments file
        (must be '.json') to override emr_connection extra. (templated)
    :param region_name: Region named passed to EmrHook
    """

    template_fields: Sequence[str] = ('job_flow_overrides',)
    template_ext: Sequence[str] = ('.json',)
    template_fields_renderers = {"job_flow_overrides": "json"}
    ui_color = '#f9c915'
    operator_extra_links = (EmrClusterLink(),)

    def __init__(
        self,
        *,
        aws_conn_id: str = 'aws_default',
        emr_conn_id: str = 'emr_default',
        job_flow_overrides: Optional[Union[str, Dict[str, Any]]] = None,
        region_name: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.emr_conn_id = emr_conn_id
        if job_flow_overrides is None:
            job_flow_overrides = {}
        self.job_flow_overrides = job_flow_overrides
        self.region_name = region_name

    def execute(self, context: 'Context') -> str:
        emr = EmrHook(
            aws_conn_id=self.aws_conn_id, emr_conn_id=self.emr_conn_id, region_name=self.region_name
        )

        self.log.info(
            'Creating JobFlow using aws-conn-id: %s, emr-conn-id: %s', self.aws_conn_id, self.emr_conn_id
        )

        if isinstance(self.job_flow_overrides, str):
            job_flow_overrides: Dict[str, Any] = ast.literal_eval(self.job_flow_overrides)
            self.job_flow_overrides = job_flow_overrides
        else:
            job_flow_overrides = self.job_flow_overrides
        response = emr.create_job_flow(job_flow_overrides)

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException(f'JobFlow creation failed: {response}')
        else:
            job_flow_id = response['JobFlowId']
            self.log.info('JobFlow with id %s created', job_flow_id)
            EmrClusterLink.persist(
                context=context,
                operator=self,
                region_name=emr.conn_region_name,
                aws_partition=emr.conn_partition,
                job_flow_id=job_flow_id,
            )
            return job_flow_id


class EmrModifyClusterOperator(BaseOperator):
    """
    An operator that modifies an existing EMR cluster.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EmrModifyClusterOperator`

    :param cluster_id: cluster identifier
    :param step_concurrency_level: Concurrency of the cluster
    :param aws_conn_id: aws connection to uses
    :param do_xcom_push: if True, cluster_id is pushed to XCom with key cluster_id.
    """

    template_fields: Sequence[str] = ('cluster_id', 'step_concurrency_level')
    template_ext: Sequence[str] = ()
    ui_color = '#f9c915'
    operator_extra_links = (EmrClusterLink(),)

    def __init__(
        self, *, cluster_id: str, step_concurrency_level: int, aws_conn_id: str = 'aws_default', **kwargs
    ):
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.cluster_id = cluster_id
        self.step_concurrency_level = step_concurrency_level

    def execute(self, context: 'Context') -> int:
        emr_hook = EmrHook(aws_conn_id=self.aws_conn_id)
        emr = emr_hook.get_conn()

        if self.do_xcom_push:
            context['ti'].xcom_push(key='cluster_id', value=self.cluster_id)

        EmrClusterLink.persist(
            context=context,
            operator=self,
            region_name=emr_hook.conn_region_name,
            aws_partition=emr_hook.conn_partition,
            job_flow_id=self.cluster_id,
        )

        self.log.info('Modifying cluster %s', self.cluster_id)
        response = emr.modify_cluster(
            ClusterId=self.cluster_id, StepConcurrencyLevel=self.step_concurrency_level
        )

        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise AirflowException(f'Modify cluster failed: {response}')
        else:
            self.log.info('Steps concurrency level %d', response['StepConcurrencyLevel'])
            return response['StepConcurrencyLevel']


class EmrTerminateJobFlowOperator(BaseOperator):
    """
    Operator to terminate EMR JobFlows.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EmrTerminateJobFlowOperator`

    :param job_flow_id: id of the JobFlow to terminate. (templated)
    :param aws_conn_id: aws connection to uses
    """

    template_fields: Sequence[str] = ('job_flow_id',)
    template_ext: Sequence[str] = ()
    ui_color = '#f9c915'
    operator_extra_links = (EmrClusterLink(),)

    def __init__(self, *, job_flow_id: str, aws_conn_id: str = 'aws_default', **kwargs):
        super().__init__(**kwargs)
        self.job_flow_id = job_flow_id
        self.aws_conn_id = aws_conn_id

    def execute(self, context: 'Context') -> None:
        emr_hook = EmrHook(aws_conn_id=self.aws_conn_id)
        emr = emr_hook.get_conn()

        EmrClusterLink.persist(
            context=context,
            operator=self,
            region_name=emr_hook.conn_region_name,
            aws_partition=emr_hook.conn_partition,
            job_flow_id=self.job_flow_id,
        )

        self.log.info('Terminating JobFlow %s', self.job_flow_id)
        response = emr.terminate_job_flows(JobFlowIds=[self.job_flow_id])

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            raise AirflowException(f'JobFlow termination failed: {response}')
        else:
            self.log.info('JobFlow with id %s terminated', self.job_flow_id)
