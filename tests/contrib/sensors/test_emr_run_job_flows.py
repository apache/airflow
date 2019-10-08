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

import datetime
import unittest
from unittest.mock import MagicMock, patch

from dateutil.tz import tzlocal

from airflow import DAG, AirflowException
from airflow.contrib.sensors.emr_run_job_flows import EmrRunJobFlows
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2017, 1, 1)


class TestEmrRunJobFlows(unittest.TestCase):
    def setUp(self):
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
        }

        # Mock out the emr_client (moto has incorrect response)
        self.emr_client = MagicMock()
        self.boto3_session = None  # This is set in _verify_job_flow_execution
        self.emr_run_job_flows = EmrRunJobFlows(
            task_id='test_task',
            poke_interval=0,
            job_flows=self._stubbed_job_flows([
                ["cluster1"],                # first batch is just this cluster
                ["cluster2a", "cluster2b"],  # then these two run in parallel
                ["cluster3"]]),              # and finally, this third batch
            dag=DAG('test_dag_id', default_args=args)
        )
        self.states = {}
        self.clusters = []

    def _stubbed_job_flows(self, names_queue):
        job_flows = []
        for names_batch in names_queue:
            job_flows_batch = {}
            for name in names_batch:
                job_flows_batch[name] = self._cluster_config(name)
            job_flows.append(job_flows_batch)
        return job_flows

    def _cluster_config(self, name):
        return {
            'Name': name,
            'ReleaseLabel': '5.11.0',
            'Instances': {
                'KeepJobFlowAliveWhenNoSteps': False
            },
            'Steps': [{
                'Name': 'test_step',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        '/usr/lib/spark/bin/run-example',
                        '{{ macros.ds_add(ds, -1) }}',
                        '{{ ds }}'
                    ]
                }
            }]
        }

    def test_execute_calls_until_all_clusters_reach_a_terminal_state(self):
        self.clusters = ["cluster1", "cluster2a", "cluster2b", "cluster3"]
        self.states["j-cluster1"] = []
        self.states["j-cluster1"].append("STARTING")
        self.states["j-cluster1"].append("BOOTSTRAPPING")
        self.states["j-cluster1"].append("RUNNING")
        self.states["j-cluster1"].append("RUNNING")
        self.states["j-cluster1"].append("TERMINATING")
        self.states["j-cluster1"].append("TERMINATED")      # (End Of Batch)
        self.states["j-cluster2a"] = []
        self.states["j-cluster2b"] = []
        self.states["j-cluster2a"].append("STARTING")       # a
        self.states["j-cluster2b"].append("STARTING")       # b
        self.states["j-cluster2a"].append("BOOTSTRAPPING")  # a
        self.states["j-cluster2b"].append("BOOTSTRAPPING")  # b
        self.states["j-cluster2a"].append("RUNNING")        # a
        self.states["j-cluster2b"].append("RUNNING")        # b
        self.states["j-cluster2a"].append("RUNNING")        # a
        self.states["j-cluster2b"].append("RUNNING")        # b
        self.states["j-cluster2a"].append("RUNNING")        # a
        self.states["j-cluster2b"].append("TERMINATING")    # b
        self.states["j-cluster2a"].append("RUNNING")        # a
        self.states["j-cluster2b"].append("TERMINATED")     # b: terminal
        self.states["j-cluster2a"].append("TERMINATING")    # a
        self.states["j-cluster2b"].append("TERMINATED")     # b: terminal
        self.states["j-cluster2a"].append("TERMINATED")     # a (End Of Batch)
        self.states["j-cluster2b"].append("TERMINATED")     # b (End Of Batch)
        self.states["j-cluster3"] = []
        self.states["j-cluster3"].append("STARTING")
        self.states["j-cluster3"].append("BOOTSTRAPPING")
        self.states["j-cluster3"].append("RUNNING")
        self.states["j-cluster3"].append("RUNNING")
        self.states["j-cluster3"].append("TERMINATING")
        self.states["j-cluster3"].append("TERMINATED")     # (all done)

        self.emr_client.describe_cluster.side_effect = self._describe
        self.emr_client.run_job_flow.side_effect = self._create

        self._verify_job_flow_execution()

    def test_execute_stops_when_cluster_in_batch_fails(self):
        self.clusters = ["cluster1"]
        # First, cluster1 is queried until it terminates
        self.states["j-cluster1"] = []
        self.states["j-cluster1"].append("STARTING")
        self.states["j-cluster1"].append("BOOTSTRAPPING")
        self.states["j-cluster1"].append("RUNNING")
        self.states["j-cluster1"].append("RUNNING")
        self.states["j-cluster1"].append("TERMINATING")
        self.states["j-cluster1"].append("TERMINATED")
        # Then, both cluster2a and cluster2b are queried
        self.states["j-cluster2a"] = []
        self.states["j-cluster2b"] = []
        self.states["j-cluster2a"].append("STARTING")                # a
        self.states["j-cluster2b"].append("STARTING")                # b
        self.states["j-cluster2a"].append("BOOTSTRAPPING")           # a
        self.states["j-cluster2b"].append("BOOTSTRAPPING")           # b
        self.states["j-cluster2a"].append("RUNNING")                 # a
        self.states["j-cluster2b"].append("RUNNING")                 # b
        self.states["j-cluster2a"].append("RUNNING")                 # a
        self.states["j-cluster2b"].append("RUNNING")                 # b
        self.states["j-cluster2a"].append("TERMINATING")             # a
        self.states["j-cluster2b"].append("TERMINATED_WITH_ERRORS")  # b
        # We expect that no more calls are to be made, even though cluster3
        # hasn't even started and cluster2a isn't yet terminated.

        self.emr_client.describe_cluster.side_effect = self._describe
        self.emr_client.run_job_flow.side_effect = self._create

        self._verify_job_flow_execution(failure=True)

    def test_execute_stops_on_cluster_creation_failure(self):
        self.clusters = ["cluster1"]
        # Note that self.states is empty since there's nothing to poke.
        self.emr_client.run_job_flow.side_effect = self._fail_to_create

        self._verify_job_flow_execution(failure=True)

    def _verify_job_flow_execution(self, failure=False):
        # Mock out the emr_client creator
        emr_session_mock = MagicMock()
        emr_session_mock.client.return_value = self.emr_client
        self.boto3_session = MagicMock(return_value=emr_session_mock)
        with patch('boto3.session.Session', self.boto3_session):
            try:
                if failure:
                    with self.assertRaises(AirflowException):
                        self._execute_and_verify_expectations()
                else:
                    self._execute_and_verify_expectations()
            except Exception as e:
                raise e

    def _execute_and_verify_expectations(self):
        created = len(self.clusters)
        poked = sum([len(cs) for cs in self.states.values()])
        self.emr_run_job_flows.execute(None)
        self.assertEqual(self.emr_client.run_job_flow.call_count, created)
        self.assertEqual(self.emr_client.describe_cluster.call_count, poked)

    # Convenience methods for describing clusters
    def _running_cluster(self, name, state="RUNNING"):
        return {
            'Cluster': {
                'Applications': [
                    {'Name': 'Spark', 'Version': '1.6.1'}
                ],
                'AutoTerminate': True,
                'Configurations': [],
                'Ec2InstanceAttributes': {'IamInstanceProfile': 'EMR_EC2_DefaultRole'},
                'Id': name,
                'LogUri': 's3n://some-location/',
                'Name': 'PiCalc',
                'NormalizedInstanceHours': 0,
                'ReleaseLabel': 'emr-4.6.0',
                'ServiceRole': 'EMR_DefaultRole',
                'Status': {
                    'State': state,
                    'StateChangeReason': {},
                    'Timeline': {
                        'CreationDateTime': datetime.datetime(
                            2016, 6, 27, 21, 5, 2, 348000, tzinfo=tzlocal())}
                },
                'Tags': [
                    {'Key': 'app', 'Value': 'analytics'},
                    {'Key': 'environment', 'Value': 'development'}
                ],
                'TerminationProtected': False,
                'VisibleToAllUsers': True
            },
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'RequestId': 'd5456308-3caa-11e6-9d46-951401f04e0e'
            }
        }

    def _terminated_cluster(self, name):
        return {
            'Cluster': {
                'Applications': [
                    {'Name': 'Spark', 'Version': '1.6.1'}
                ],
                'AutoTerminate': True,
                'Configurations': [],
                'Ec2InstanceAttributes': {'IamInstanceProfile': 'EMR_EC2_DefaultRole'},
                'Id': name,
                'LogUri': 's3n://some-location/',
                'Name': 'PiCalc',
                'NormalizedInstanceHours': 0,
                'ReleaseLabel': 'emr-4.6.0',
                'ServiceRole': 'EMR_DefaultRole',
                'Status': {
                    'State': 'TERMINATED',
                    'StateChangeReason': {},
                    'Timeline': {
                        'CreationDateTime': datetime.datetime(
                            2016, 6, 27, 21, 5, 2, 348000, tzinfo=tzlocal())}
                },
                'Tags': [
                    {'Key': 'app', 'Value': 'analytics'},
                    {'Key': 'environment', 'Value': 'development'}
                ],
                'TerminationProtected': False,
                'VisibleToAllUsers': True
            },
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'RequestId': 'd5456308-3caa-11e6-9d46-951401f04e0e'
            }
        }

    def _failed_cluster(self, name):
        return {
            'Cluster': {
                'Applications': [
                    {'Name': 'Spark', 'Version': '1.6.1'}
                ],
                'AutoTerminate': True,
                'Configurations': [],
                'Ec2InstanceAttributes': {'IamInstanceProfile': 'EMR_EC2_DefaultRole'},
                'Id': name,
                'LogUri': 's3n://some-location/',
                'Name': 'PiCalc',
                'NormalizedInstanceHours': 0,
                'ReleaseLabel': 'emr-4.6.0',
                'ServiceRole': 'EMR_DefaultRole',
                'Status': {
                    'State': 'TERMINATED_WITH_ERRORS',
                    'StateChangeReason': {
                        'Code': 'BOOTSTRAP_FAILURE',
                        'Message': 'Master instance (i-0663047709b12345c) failed attempting to '
                                   'download bootstrap action 1 file from S3'
                    },
                    'Timeline': {
                        'CreationDateTime': datetime.datetime(
                            2016, 6, 27, 21, 5, 2, 348000, tzinfo=tzlocal())}
                },
                'Tags': [
                    {'Key': 'app', 'Value': 'analytics'},
                    {'Key': 'environment', 'Value': 'development'}
                ],
                'TerminationProtected': False,
                'VisibleToAllUsers': True
            },
            'ResponseMetadata': {
                'HTTPStatusCode': 200,
                'RequestId': 'd5456308-3caa-11e6-9d46-951401f04e0e'
            }
        }

    def _describe(self, *args, **kwargs):
        name = kwargs['ClusterId']
        state = self.states[name].pop(0)
        return {
            'TERMINATED': self._terminated_cluster(name),
            'TERMINATED_WITH_ERRORS': self._failed_cluster(name),
        }.get(state, self._running_cluster(name, state))

    def _fail_to_create(self, *args, **kwargs):
        return {
            'ResponseMetadata': {
                'HTTPStatusCode': 400
            }
        }

    def _create(self, *args, **kwargs):
        return {
            'ResponseMetadata': {
                'HTTPStatusCode': 200
            },
            'JobFlowId': 'j-' + kwargs['Name']
        }


if __name__ == '__main__':
    unittest.main()
