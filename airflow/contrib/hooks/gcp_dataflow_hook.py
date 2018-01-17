# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import json
import select
import subprocess
import time
import uuid

from apiclient.discovery import build

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.utils.log.logging_mixin import LoggingMixin


class _DataflowJob(LoggingMixin):
    def __init__(self, dataflow, project_number, name, poll_sleep=10):
        self._dataflow = dataflow
        self._project_number = project_number
        self._job_name = name
        self._job_id = None
        self._job = self._get_job()
        self._poll_sleep = poll_sleep

    def _get_job_id_from_name(self):
        jobs = self._dataflow.projects().jobs().list(
            projectId=self._project_number
        ).execute()
        for job in jobs['jobs']:
            if job['name'] == self._job_name:
                self._job_id = job['id']
                return job
        return None

    def _get_job(self):
        if self._job_id is None:
            job = self._get_job_id_from_name()
        else:
            job = self._dataflow.projects().jobs().get(projectId=self._project_number,
                                                       jobId=self._job_id).execute()
        if 'currentState' in job:
            self.log.info(
                'Google Cloud DataFlow job %s is %s',
                job['name'], job['currentState']
            )
        else:
            self.log.info(
                'Google Cloud DataFlow with job_id %s has name %s',
                self._job_id, job['name']
            )
        return job

    def wait_for_done(self):
        while True:
            if 'currentState' in self._job:
                if 'JOB_STATE_DONE' == self._job['currentState']:
                    return True
                elif 'JOB_STATE_RUNNING' == self._job['currentState'] and \
                     'JOB_TYPE_STREAMING' == self._job['type']:
                    return True
                elif 'JOB_STATE_FAILED' == self._job['currentState']:
                    raise Exception("Google Cloud Dataflow job {} has failed.".format(
                        self._job['name']))
                elif 'JOB_STATE_CANCELLED' == self._job['currentState']:
                    raise Exception("Google Cloud Dataflow job {} was cancelled.".format(
                        self._job['name']))
                elif 'JOB_STATE_RUNNING' == self._job['currentState']:
                    time.sleep(self._poll_sleep)
                elif 'JOB_STATE_PENDING' == self._job['currentState']:
                    time.sleep(15)
                else:
                    self.log.debug(str(self._job))
                    raise Exception(
                        "Google Cloud Dataflow job {} was unknown state: {}".format(
                            self._job['name'], self._job['currentState']))
            else:
                time.sleep(15)

            self._job = self._get_job()

    def get(self):
        return self._job


class _Dataflow(LoggingMixin):
    def __init__(self, cmd):
        self.log.info("Running command: %s", ' '.join(cmd))
        self._proc = subprocess.Popen(
            cmd,
            shell=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=True)

    def _line(self, fd):
        if fd == self._proc.stderr.fileno():
            lines = self._proc.stderr.readlines()
            for line in lines:
              self.log.warning(line[:-1])
            line = lines[-1][:-1]
            return line
        if fd == self._proc.stdout.fileno():
            line = self._proc.stdout.readline()
            return line

    @staticmethod
    def _extract_job(line):
        if line is not None:
            if line.startswith("Submitted job: "):
                return line[15:-1]

    def wait_for_done(self):
        reads = [self._proc.stderr.fileno(), self._proc.stdout.fileno()]
        self.log.info("Start waiting for DataFlow process to complete.")
        while self._proc.poll() is None:
            ret = select.select(reads, [], [], 5)
            if ret is not None:
                for fd in ret[0]:
                    line = self._line(fd)
                    self.log.debug(line[:-1])
            else:
                self.log.info("Waiting for DataFlow process to complete.")
        if self._proc.returncode is not 0:
            raise Exception("DataFlow failed with return code {}".format(
                self._proc.returncode))


class DataFlowHook(GoogleCloudBaseHook):

    def __init__(self,
                 gcp_conn_id='google_cloud_default',
                 delegate_to=None,
                 poll_sleep=10):
        self.poll_sleep = poll_sleep
        super(DataFlowHook, self).__init__(gcp_conn_id, delegate_to)

    def get_conn(self):
        """
        Returns a Google Cloud Storage service object.
        """
        http_authorized = self._authorize()
        return build('dataflow', 'v1b3', http=http_authorized)

    def _start_dataflow(self, task_id, variables, name,
                        command_prefix, label_formatter):
        cmd = command_prefix + self._build_cmd(task_id, variables,
                                               label_formatter)
        _Dataflow(cmd).wait_for_done()
        _DataflowJob(self.get_conn(), variables['project'],
                     name, self.poll_sleep).wait_for_done()

    def start_java_dataflow(self, task_id, variables, dataflow, job_class=None):
        name = task_id + "-" + str(uuid.uuid1())[:8]
        variables['jobName'] = name

        def label_formatter(labels_dict):
            return ['--labels={}'.format(
                    json.dumps(labels_dict).replace(' ', ''))]
        command_prefix = (["java", "-cp", dataflow, job_class] if job_class
                          else ["java", "-jar", dataflow])
        self._start_dataflow(task_id, variables, name,
                             command_prefix, label_formatter)

    def start_template_dataflow(self, task_id, variables, parameters, dataflow_template):
        name = task_id + "-" + str(uuid.uuid1())[:8]
        self._start_template_dataflow(
            name, variables, parameters, dataflow_template)

    def start_python_dataflow(self, task_id, variables, dataflow, py_options):
        name = task_id + "-" + str(uuid.uuid1())[:8]
        variables["job_name"] = name

        def label_formatter(labels_dict):
            return ['--labels={}={}'.format(key, value)
                    for key, value in labels_dict.items()]
        self._start_dataflow(task_id, variables, name,
                             ["python"] + py_options + [dataflow],
                             label_formatter)

    def _build_cmd(self, task_id, variables, label_formatter):
        command = ["--runner=DataflowRunner"]
        if variables is not None:
            for attr, value in variables.items():
                if attr == 'labels':
                    command += label_formatter(value)
                else:
                    command.append("--" + attr + "=" + value)
        return command

    def _start_template_dataflow(self, name, variables, parameters, dataflow_template):
        # Builds RuntimeEnvironment from variables dictionary
        # https://cloud.google.com/dataflow/docs/reference/rest/v1b3/RuntimeEnvironment
        environment = {}
        for key in ['maxWorkers', 'zone', 'serviceAccountEmail', 'tempLocation',
                    'bypassTempDirValidation', 'machineType']:
            if key in variables:
                environment.update({key: variables[key]})
        body = {"jobName": name,
                "parameters": parameters,
                "environment": environment}
        service = self.get_conn()
        if variables['project'] is None:
            raise Exception(
                'Project not specified')
        request = service.projects().templates().launch(projectId=variables['project'],
                                                        gcsPath=dataflow_template,
                                                        body=body)
        response = request.execute()
        _DataflowJob(
            self.get_conn(), variables['project'], name, self.poll_sleep).wait_for_done()
        return response
