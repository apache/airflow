# -*- coding: utf-8 -*-
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
"""EmrRunJobFlows manages cluster queue by implementing an EMR sensor."""

from airflow import AirflowException
from airflow.contrib.hooks.emr_hook import EmrHook
from airflow.contrib.sensors.emr_base_sensor import EmrBaseSensor
from airflow.contrib.sensors.emr_job_flow_sensor import EmrJobFlowSensor
from airflow.utils.decorators import apply_defaults


class EmrRunJobFlows(EmrBaseSensor):
    """
    Submits batches of self-terminating EMR Job Flows and waits for their steps
    to complete. This operator submits a list of EMR clusters in batches, where
    each Job Flow is expected to be self-terminating and list all the EMR steps
    it is expected to execute. Only basic retry logic.

    Implementation Note: For each cluster, we submit all the steps at cluster
    creation time. This partially frees the cluster from the vagaries of the
    Airflow scheduler. Since we rely on EMR to terminate its clusters, any
    failed step will need to terminate the cluster and the cluster itself should
    auto-terminate as per [1]. In other words, each JobFlow must auto-terminate
    (likely via the `job_flows` parameter) by setting its Instances'
    `"KeepJobFlowAliveWhenNoSteps": False`. Additionally, consider setting each
    Step's `"ActionOnFailure": "TERMINATE_CLUSTER"` to allow failing-fast if
    your workflow allows for it.

    [1]: https://docs.aws.amazon.com/emr/latest/ManagementGuide/\
    UsingEMR_TerminationProtection.html#emr-termination-protection-steps

    TODO: The utility of the EmrBaseSensor that we extend is somewhat limited.
    Currently, it asks for the state of its JobFlow until that JobFlow reaches a
    terminal state. If the EMR JobFlow fails, the sensor will mark the task as
    failed. If custom EMR sensor logic is pursued, we could set up step-wise
    monitoring and timeouts, which would allow for context-specific retries
    using XComs, and maybe able to extend the implementation to allow for
    cross-cluster logic, such as waiting for all clusters in a batch to finish
    even when some fail.

    :param job_flows: a queue of EMR JobFlows. It's a list of dicts, each one
        mapping job_flow names to their configurations:
        [{job_flow_name: job_flow_overrides}]. Each dict in the list represents
        the job flows which should run in parallel, and every cluster in the
        preceding dict is expected to have come to a successful terminal state,
        prior to submitting the next dict. See boto3's job_flow_overrides EMR
        details in
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/\
        services/emr.html#EMR.Client.run_job_flow (templated)
    :type job_flows: list
    """

    template_fields = ['job_flows']
    template_ext = ()
    # EMR logo... ~RGB(237,165,83)
    ui_color = "#eda553"

    # Overrides for EmrBaseSensor
    NON_TERMINAL_STATES = EmrJobFlowSensor.NON_TERMINAL_STATES
    FAILED_STATE = EmrJobFlowSensor.FAILED_STATE

    @apply_defaults
    def __init__(
            self,
            job_flows,
            emr_conn_id='emr_default',
            # require_auto_termination = False,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.job_flows = job_flows
        self.emr_conn_id = emr_conn_id
        # These two fields will be filled in as clusters are requested and poked
        self.current_batch = {}
        self.statuses = []

    def execute(self, context):
        """
        See EmrBaseSensor.execute
        """
        self.log.info(
            "The clusters will be submitted across the following batches: %s",
            [set(batch.keys()) for batch in self.job_flows])
        # TODO: Verify all clusters set `"KeepJobFlowAliveWhenNoSteps": False`
        # if self.require_auto_termination
        return super().execute(context)

    def get_emr_response(self):
        """
        override for EmrBaseSensor. Queries state of all clusters in current
        batch and submits the next batch if they are all in a terminal state;
        however, if any cluster is in a failed state, its state is returned so
        that the current execution attempt fails.
        """
        emr_conn = EmrHook(emr_conn_id=self.emr_conn_id).get_conn()

        responses = []
        for name, job_flow_id in self.current_batch.items():
            self.log.debug("Poking JobFlow {%s: %s}", name, job_flow_id)
            response = emr_conn.describe_cluster(ClusterId=job_flow_id)
            responses.append(response)
            self._states()[name] = (job_flow_id, self._state_of(response))
        self.log.debug("Poked JobFlow states: %s", self._states())

        failed = next(filter(lambda r: self._state_of(r) in
                             EmrRunJobFlows.FAILED_STATE, responses), None)
        if failed:
            self.log.info("there is at least one failed JobFlow")
            return failed
        non_terminal = next(filter(lambda r: self._state_of(r) in
                                   EmrRunJobFlows.NON_TERMINAL_STATES,
                                   responses), None)
        if non_terminal:
            self.log.info("there is still at least one non-terminal JobFlow")
            return non_terminal

        # We're done with the current batch.
        if self.job_flows:
            self.log.info("Submitting next batch of clusters")
            self._request_next(self.job_flows.pop(0))
            return self.get_emr_response()
        # All batches are in a terminal state
        else:
            self.log.info("Completed poking all JobFlow batches: %s",
                          self.statuses)
            return responses[0]

    def _request_next(self, cluster_set):
        self.current_batch = {}
        self.statuses.append({})
        errors = {}
        emr_hook = EmrHook(emr_conn_id=self.emr_conn_id)
        for name, cluster_config in cluster_set.items():
            response = emr_hook.create_job_flow(cluster_config)
            if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
                errors[name] = str(response)
            else:
                job_flow_id = response["JobFlowId"]
                self.current_batch[name] = job_flow_id
                self._states()[name] = (job_flow_id, "")
        self.log.info("Requested JobFlow batch: %s", self.current_batch)

        # TODO consider returning {"statuses": statuses, "errors": errors}
        if errors:
            self.log.error("errors: %s", errors)
            raise AirflowException("JobFlow creation failed: " + str(errors))

    def _states(self):
        return self.statuses[-1] if self.statuses else {}

    @staticmethod
    def _state_of(response):
        return response.get("Cluster", {}).get("Status", {}).get("State", "")

    @staticmethod
    def state_from_response(response):
        """
        override for EmrBaseSensor. Not using _state_of(), since
        state_from_response expects an exception raised if the cluster State is
        not present.
        """
        return EmrJobFlowSensor.state_from_response(response)

    @staticmethod
    def failure_message_from_response(response):
        """
        See EmrJobFlowSensor.failure_message_from_response
        """
        return EmrJobFlowSensor.failure_message_from_response(response)
