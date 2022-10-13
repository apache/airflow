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

import ast
import warnings
from typing import TYPE_CHECKING, Any, Sequence
from uuid import uuid4

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.emr import EmrContainerHook, EmrHook, EmrServerlessHook
from airflow.providers.amazon.aws.links.emr import EmrClusterLink

if TYPE_CHECKING:
    from airflow.utils.context import Context

from airflow.compat.functools import cached_property


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
        job_flow_id: str | None = None,
        job_flow_name: str | None = None,
        cluster_states: list[str] | None = None,
        aws_conn_id: str = 'aws_default',
        steps: list[dict] | str | None = None,
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

    def execute(self, context: Context) -> list[str]:
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


class EmrEksCreateClusterOperator(BaseOperator):
    """
    An operator that creates EMR on EKS virtual clusters.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EmrEksCreateClusterOperator`

    :param virtual_cluster_name: The name of the EMR EKS virtual cluster to create.
    :param eks_cluster_name: The EKS cluster used by the EMR virtual cluster.
    :param eks_namespace: namespace used by the EKS cluster.
    :param virtual_cluster_id: The EMR on EKS virtual cluster id.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param tags: The tags assigned to created cluster.
        Defaults to None
    """

    template_fields: Sequence[str] = (
        "virtual_cluster_name",
        "eks_cluster_name",
        "eks_namespace",
    )
    ui_color = "#f9c915"

    def __init__(
        self,
        *,
        virtual_cluster_name: str,
        eks_cluster_name: str,
        eks_namespace: str,
        virtual_cluster_id: str = '',
        aws_conn_id: str = "aws_default",
        tags: dict | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.virtual_cluster_name = virtual_cluster_name
        self.eks_cluster_name = eks_cluster_name
        self.eks_namespace = eks_namespace
        self.virtual_cluster_id = virtual_cluster_id
        self.aws_conn_id = aws_conn_id
        self.tags = tags

    @cached_property
    def hook(self) -> EmrContainerHook:
        """Create and return an EmrContainerHook."""
        return EmrContainerHook(self.aws_conn_id)

    def execute(self, context: Context) -> str | None:
        """Create EMR on EKS virtual Cluster"""
        self.virtual_cluster_id = self.hook.create_emr_on_eks_cluster(
            self.virtual_cluster_name, self.eks_cluster_name, self.eks_namespace, self.tags
        )
        return self.virtual_cluster_id


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
    :param max_tries: Deprecated - use max_polling_attempts instead.
    :param max_polling_attempts: Maximum number of times to wait for the job run to finish.
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
        configuration_overrides: dict | None = None,
        client_request_token: str | None = None,
        aws_conn_id: str = "aws_default",
        wait_for_completion: bool = True,
        poll_interval: int = 30,
        max_tries: int | None = None,
        tags: dict | None = None,
        max_polling_attempts: int | None = None,
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
        self.max_polling_attempts = max_polling_attempts
        self.tags = tags
        self.job_id: str | None = None

        if max_tries:
            warnings.warn(
                f"Parameter `{self.__class__.__name__}.max_tries` is deprecated and will be removed "
                "in a future release.  Please use method `max_polling_attempts` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            if max_polling_attempts and max_polling_attempts != max_tries:
                raise Exception("max_polling_attempts must be the same value as max_tries")
            else:
                self.max_polling_attempts = max_tries

    @cached_property
    def hook(self) -> EmrContainerHook:
        """Create and return an EmrContainerHook."""
        return EmrContainerHook(
            self.aws_conn_id,
            virtual_cluster_id=self.virtual_cluster_id,
        )

    def execute(self, context: Context) -> str | None:
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
            query_status = self.hook.poll_query_status(
                self.job_id,
                max_polling_attempts=self.max_polling_attempts,
                poll_interval=self.poll_interval,
            )

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
    :param emr_conn_id: :ref:`Amazon Elastic MapReduce Connection <howto/connection:emr>`.
        Use to receive an initial Amazon EMR cluster configuration:
        ``boto3.client('emr').run_job_flow`` request body.
        If this is None or empty or the connection does not exist,
        then an empty initial configuration is used.
    :param job_flow_overrides: boto3 style arguments or reference to an arguments file
        (must be '.json') to override specific ``emr_conn_id`` extra parameters. (templated)
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
        emr_conn_id: str | None = 'emr_default',
        job_flow_overrides: str | dict[str, Any] | None = None,
        region_name: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.emr_conn_id = emr_conn_id
        self.job_flow_overrides = job_flow_overrides or {}
        self.region_name = region_name

    def execute(self, context: Context) -> str:
        emr = EmrHook(
            aws_conn_id=self.aws_conn_id, emr_conn_id=self.emr_conn_id, region_name=self.region_name
        )

        self.log.info(
            'Creating JobFlow using aws-conn-id: %s, emr-conn-id: %s', self.aws_conn_id, self.emr_conn_id
        )
        if isinstance(self.job_flow_overrides, str):
            job_flow_overrides: dict[str, Any] = ast.literal_eval(self.job_flow_overrides)
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

    def execute(self, context: Context) -> int:
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

    def execute(self, context: Context) -> None:
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


class EmrServerlessCreateApplicationOperator(BaseOperator):
    """
    Operator to create Serverless EMR Application

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EmrServerlessCreateApplicationOperator`

    :param release_label: The EMR release version associated with the application.
    :param job_type: The type of application you want to start, such as Spark or Hive.
    :param wait_for_completion: If true, wait for the Application to start before returning. Default to True
    :param client_request_token: The client idempotency token of the application to create.
      Its value must be unique for each request.
    :param config: Optional dictionary for arbitrary parameters to the boto API create_application call.
    :param aws_conn_id: AWS connection to use
    """

    def __init__(
        self,
        release_label: str,
        job_type: str,
        client_request_token: str = '',
        config: dict | None = None,
        wait_for_completion: bool = True,
        aws_conn_id: str = 'aws_default',
        **kwargs,
    ):
        self.aws_conn_id = aws_conn_id
        self.release_label = release_label
        self.job_type = job_type
        self.wait_for_completion = wait_for_completion
        self.kwargs = kwargs
        self.config = config or {}
        super().__init__(**kwargs)

        self.client_request_token = client_request_token or str(uuid4())

    @cached_property
    def hook(self) -> EmrServerlessHook:
        """Create and return an EmrServerlessHook."""
        return EmrServerlessHook(aws_conn_id=self.aws_conn_id)

    def execute(self, context: Context):
        response = self.hook.conn.create_application(
            clientToken=self.client_request_token,
            releaseLabel=self.release_label,
            type=self.job_type,
            **self.config,
        )
        application_id = response['applicationId']

        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise AirflowException(f'Application Creation failed: {response}')

        self.log.info('EMR serverless application created: %s', application_id)

        # This should be replaced with a boto waiter when available.
        self.hook.waiter(
            get_state_callable=self.hook.conn.get_application,
            get_state_args={'applicationId': application_id},
            parse_response=['application', 'state'],
            desired_state={'CREATED'},
            failure_states=EmrServerlessHook.APPLICATION_FAILURE_STATES,
            object_type='application',
            action='created',
        )

        self.log.info('Starting application %s', application_id)
        self.hook.conn.start_application(applicationId=application_id)

        if self.wait_for_completion:
            # This should be replaced with a boto waiter when available.
            self.hook.waiter(
                get_state_callable=self.hook.conn.get_application,
                get_state_args={'applicationId': application_id},
                parse_response=['application', 'state'],
                desired_state={'STARTED'},
                failure_states=EmrServerlessHook.APPLICATION_FAILURE_STATES,
                object_type='application',
                action='started',
            )

        return application_id


class EmrServerlessStartJobOperator(BaseOperator):
    """
    Operator to start EMR Serverless job.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EmrServerlessStartJobOperator`

    :param application_id: ID of the EMR Serverless application to start.
    :param execution_role_arn: ARN of role to perform action.
    :param job_driver: Driver that the job runs on.
    :param configuration_overrides: Configuration specifications to override existing configurations.
    :param client_request_token: The client idempotency token of the application to create.
      Its value must be unique for each request.
    :param config: Optional dictionary for arbitrary parameters to the boto API start_job_run call.
    :param wait_for_completion: If true, waits for the job to start before returning. Defaults to True.
    :param aws_conn_id: AWS connection to use
    """

    template_fields: Sequence[str] = (
        'application_id',
        'execution_role_arn',
        'job_driver',
        'configuration_overrides',
    )

    def __init__(
        self,
        application_id: str,
        execution_role_arn: str,
        job_driver: dict,
        configuration_overrides: dict | None,
        client_request_token: str = '',
        config: dict | None = None,
        wait_for_completion: bool = True,
        aws_conn_id: str = 'aws_default',
        **kwargs,
    ):
        self.aws_conn_id = aws_conn_id
        self.application_id = application_id
        self.execution_role_arn = execution_role_arn
        self.job_driver = job_driver
        self.configuration_overrides = configuration_overrides
        self.wait_for_completion = wait_for_completion
        self.config = config or {}
        super().__init__(**kwargs)

        self.client_request_token = client_request_token or str(uuid4())

    @cached_property
    def hook(self) -> EmrServerlessHook:
        """Create and return an EmrServerlessHook."""
        return EmrServerlessHook(aws_conn_id=self.aws_conn_id)

    def execute(self, context: Context) -> dict:
        self.log.info('Starting job on Application: %s', self.application_id)

        app_state = self.hook.conn.get_application(applicationId=self.application_id)['application']['state']
        if app_state not in EmrServerlessHook.APPLICATION_SUCCESS_STATES:
            self.hook.conn.start_application(applicationId=self.application_id)

            self.hook.waiter(
                get_state_callable=self.hook.conn.get_application,
                get_state_args={'applicationId': self.application_id},
                parse_response=['application', 'state'],
                desired_state={'STARTED'},
                failure_states=EmrServerlessHook.APPLICATION_FAILURE_STATES,
                object_type='application',
                action='started',
            )

        response = self.hook.conn.start_job_run(
            clientToken=self.client_request_token,
            applicationId=self.application_id,
            executionRoleArn=self.execution_role_arn,
            jobDriver=self.job_driver,
            configurationOverrides=self.configuration_overrides,
            **self.config,
        )

        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise AirflowException(f'EMR serverless job failed to start: {response}')

        self.log.info('EMR serverless job started: %s', response['jobRunId'])
        if self.wait_for_completion:
            # This should be replaced with a boto waiter when available.
            self.hook.waiter(
                get_state_callable=self.hook.conn.get_job_run,
                get_state_args={
                    'applicationId': self.application_id,
                    'jobRunId': response['jobRunId'],
                },
                parse_response=['jobRun', 'state'],
                desired_state=EmrServerlessHook.JOB_SUCCESS_STATES,
                failure_states=EmrServerlessHook.JOB_FAILURE_STATES,
                object_type='job',
                action='run',
            )
        return response['jobRunId']


class EmrServerlessDeleteApplicationOperator(BaseOperator):
    """
    Operator to delete EMR Serverless application

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:EmrServerlessDeleteApplicationOperator`

    :param application_id: ID of the EMR Serverless application to delete.
    :param wait_for_completion: If true, wait for the Application to start before returning. Default to True
    :param aws_conn_id: AWS connection to use
    """

    template_fields: Sequence[str] = ('application_id',)

    def __init__(
        self,
        application_id: str,
        wait_for_completion: bool = True,
        aws_conn_id: str = 'aws_default',
        **kwargs,
    ):
        self.aws_conn_id = aws_conn_id
        self.application_id = application_id
        self.wait_for_completion = wait_for_completion
        super().__init__(**kwargs)

    @cached_property
    def hook(self) -> EmrServerlessHook:
        """Create and return an EmrServerlessHook."""
        return EmrServerlessHook(aws_conn_id=self.aws_conn_id)

    def execute(self, context: Context) -> None:
        self.log.info('Stopping application: %s', self.application_id)
        self.hook.conn.stop_application(applicationId=self.application_id)

        # This should be replaced with a boto waiter when available.
        self.hook.waiter(
            get_state_callable=self.hook.conn.get_application,
            get_state_args={
                'applicationId': self.application_id,
            },
            parse_response=['application', 'state'],
            desired_state=EmrServerlessHook.APPLICATION_FAILURE_STATES,
            failure_states=set(),
            object_type='application',
            action='stopped',
        )

        self.log.info('Deleting application: %s', self.application_id)
        response = self.hook.conn.delete_application(applicationId=self.application_id)

        if response['ResponseMetadata']['HTTPStatusCode'] != 200:
            raise AirflowException(f'Application deletion failed: {response}')

        if self.wait_for_completion:
            # This should be replaced with a boto waiter when available.
            self.hook.waiter(
                get_state_callable=self.hook.conn.get_application,
                get_state_args={'applicationId': self.application_id},
                parse_response=['application', 'state'],
                desired_state={'TERMINATED'},
                failure_states=EmrServerlessHook.APPLICATION_FAILURE_STATES,
                object_type='application',
                action='deleted',
            )

        self.log.info('EMR serverless application deleted')
