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

from airflow.models.baseoperator import BaseOperator
from airflow.providers.sas._utils.logon import create_session_for_connection
import json
import copy
import time
import requests
from airflow.exceptions import AirflowFailException


class SASStudioFlowOperator(BaseOperator):
    """
    Executes a SAS Studio flow

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SASStudioFlowOperator`

    :param flow_path_type: valid values are content or compute
    :param flow_path: path to the flow to execute. eg /Public/myflow.flw
    :param flow_exec_log: whether or not to output the execution log
    :param flow_codegen_init_code: Whether or not to generate init code
        (default value: False)
    :param flow_codegen_wrap_code: Whether or not to generate wrapper code
        (default value: False)
    :param airflow_connection_name: name of the connection to use. The connection should be defined
        as an HTTP connection in Airflow.
    :param compute_context: (optional) Name of the compute context to use. If not provided, a
        suitable default is used.
    :param env_vars: (optional) Dictionary of environment variables to set before running the flow.
    """
    ui_color = "#CCE5FF"
    ui_fgcolor = "black"

    def __init__(self,
                 flow_path_type: str,
                 flow_path: str,
                 flow_exec_log: bool,
                 flow_codegen_init_code=False,
                 flow_codegen_wrap_code=False,
                 airflow_connection_name="SAS",
                 compute_context="SAS Studio compute context",
                 env_vars=None,
                 **kwargs) -> None:

        super().__init__(**kwargs)
        if env_vars is None:
            env_vars = {}
        self.flow_path_type = flow_path_type
        self.flow_path = flow_path
        self.flow_exec_log = flow_exec_log
        self.flow_codegen_initCode = flow_codegen_init_code
        self.flow_codegen_wrapCode = flow_codegen_wrap_code
        self.airflow_connection_name = airflow_connection_name
        self.compute_context = compute_context
        self.env_vars = env_vars

    def execute(self, context):

        print("Authenticate connection")
        session = create_session_for_connection(self.airflow_connection_name)

        print("Generate code for Studio Flow: " + str(self.flow_path))
        code = _generate_flow_code(session, self.flow_path_type,
                                   self.flow_path,
                                   self.flow_codegen_initCode,
                                   self.flow_codegen_wrapCode,
                                   None,
                                   self.compute_context)

        if self.env_vars:
            # Add environment variables to pre-code
            print(f'Adding {len(self.env_vars)} environment variables to code')
            pre_env_code = "/** Setting up environment variables **/\n"
            for env_var in self.env_vars:
                env_val = self.env_vars[env_var]
                pre_env_code = pre_env_code + f"options set={env_var}='{env_val}';\n"
            pre_env_code = pre_env_code + "/** Finished setting up environment variables **/\n\n"
            code['code'] = pre_env_code + code['code']

        # Create the job request for JES
        jr = {
            'name': f'Airflow_{self.task_id}',
            'jobDefinition': {
                'type': 'Compute',
                'code': code['code']
            },
            'arguments': {
                '_contextName': self.compute_context
            }
        }

        # Kick off the JES job
        job = _run_job_and_wait(session, jr, 1)
        job_state = job["state"]

        # display logs if needed
        if self.flow_exec_log == True:
            _dump_logs(session, job)

        # raise exception in Airflow if SAS Studio Flow ended execution with "failed" state
        if job_state == "failed":
            raise AirflowFailException(
                "SAS Studio Flow Execution completed with an error. See log for details "
                "(set flow_exec_log to True in the operator to turn on logging)")

        return 1


def _generate_flow_code(session,
                        artifact_type: str,
                        path: str,
                        init_code: bool,
                        wrap_code: bool,
                        session_id=None,
                        compute_context="SAS Studio compute context"):
    # main API URI for Code Gen
    uri_base = '/studioDevelopment/code'

    # if type == compute then Compute session should be created
    if artifact_type == 'compute':
        print('Code Generation for Studio Flow with Compute session')

        # if session id is provided
        if session_id is not None:

            print('Session ID was provided')
            uri = f'{uri_base}?sessionId={session_id}'
        else:
            print("Create or connect to session")
            compute_session = _create_or_connect_to_session(session, compute_context, "Airflow-Session")
            uri = f'{uri_base}?sessionId={compute_session["id"]}'

        req = {
            'reference': {'mediaType': 'application/vnd.sas.dataflow', 'type': artifact_type, 'path': path},
            'initCode': init_code, 'wrapperCode': wrap_code}

        response = session.post(uri, json=req)

        if response.status_code != 200:
            raise RuntimeError(f'Code generation failed: {response.text}')

        return response.json()

    # if type == content then Compute session is not needed
    elif artifact_type == 'content':
        print('Code Generation for Studio Flow without Compute session')

        req = {
            'reference': {'mediaType': 'application/vnd.sas.dataflow', 'type': artifact_type, 'path': path},
            'initCode': init_code, 'wrapperCode': wrap_code}

        uri = uri_base
        response = session.post(uri, json=req)

        if response.status_code != 200:
            raise RuntimeError(f'Code generation failed: {response.text}')

        r = response.json()
        return r

    else:
        raise RuntimeError('invalid artifact_type was supplied')


def _create_or_connect_to_session(session: requests.session, context_name: str, name: str) -> dict:
    # find session with given name
    response = session.get(f'/compute/sessions?filter=eq(name, {name})')
    if response.status_code != 200:
        raise RuntimeError(f'Find sessions failed: {response.text}')
    sessions = response.json()
    if sessions["count"] > 0:
        return sessions["items"][0]

    print(f"Compute session named '{name}' does not exist, a new one will be created")
    # find compute context
    response = session.get('/compute/contexts', params={'filter': f'eq("name","{context_name}")'})
    if response.status_code != 200:
        raise RuntimeError(f'Find context named {context_name} failed: {response.text}')
    context_resp = response.json()
    if not context_resp["count"]:
        raise RuntimeError(f"Compute context '{context_name}' was not found")
    sas_context = context_resp["items"][0]

    # create session with given context
    uri = f'/compute/contexts/{sas_context["id"]}/sessions'
    session_request = {"version": 1, "name": name}
    tmpheaders = copy.deepcopy(session.headers)
    tmpheaders["Content-Type"] = "application/vnd.sas.compute.session.request+json"

    req = json.dumps(session_request)
    response = session.post(
        uri, data=req, headers=tmpheaders)

    if response.status_code != 201:
        raise RuntimeError(f'Failed to create session: {response.text}')

    return response.json()

def _get_file_contents(session, file_uri) -> str:
    r = session.get(f'{file_uri}/content')
    if r.status_code != 200:
        raise RuntimeError(f'Failed to get file contents for {file_uri}: {r.text}')
    return r.text

def _get_uri(links, rel):
    link = next((x for x in links if x['rel'] == rel), None)
    if link is None:
        return None
    return link['uri']

JES_URI = '/jobExecution'
JOB_URI = f'{JES_URI}/jobs'
def _run_job_and_wait(session, job_request: dict, poll_interval: int) -> dict:
    uri = JOB_URI
    response = session.post(uri, json=job_request)
    # change to process non standard codes returned from API (201, 400, 415)
    # i.e. sistuation when we were not able to make API call at all
    if response.status_code != 201:
        raise RuntimeError(f'Failed to create job request: {response.text}')
    job = response.json()
    job_id = job["id"]
    state = job["state"]
    print(f'Submitted job request with id {job_id}. Waiting for completion')
    uri = f'{JOB_URI}/{job_id}'
    while state in ['pending', 'running']:
        time.sleep(poll_interval)
        response = session.get(uri)
        if response.status_code != 200:
            raise RuntimeError(f'Failed to get job: {response.text}')
        job = response.json()
        state = job["state"]
    print("Job request has completed execution with the status: " + str(state))
    return job

def _dump_logs(session, job):
    # Get the log from the job
    log_uri = _get_uri(job['links'], 'log')
    if not log_uri:
        print("Warning: failed to retrieve log uri from links. Log will not be displayed")
    else:
        log_contents = _get_file_contents(session, log_uri)
        # Parse the json log format and print each line
        jcontents = json.loads(log_contents)
        for line in jcontents["items"]:
            print(f'{line["type"]}: {line["line"]}\n')
