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

import threading
from queue import Queue
from typing import Any, Dict, Optional

from avmesos.client import MesosClient

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.executors.base_executor import BaseExecutor, CommandType
from airflow.models.taskinstance import TaskInstanceKey
from airflow.utils.session import provide_session
from airflow.utils.state import State

FRAMEWORK_CONNID_PREFIX = 'mesos_framework_'


def get_framework_name():
    """Get the mesos framework name if its set in airflow.cfg"""
    return conf.get('mesos', 'FRAMEWORK_NAME')


# pylint: disable=too-many-nested-blocks
# pylint: disable=too-many-instance-attributes
class AirflowMesosScheduler(MesosClient):
    """
    Airflow Mesos scheduler implements mesos scheduler interface
    to schedule airflow tasks on mesos
    Basically, it schedules a command like
    'airflow run <dag_id> <task_instance_id> <start_date> --local -p=<pickle>'
    to run on a mesos slave
    """

    # pylint: disable=super-init-not-called
    def __init__(self, executor, task_queue, result_queue, task_cpu: int = 1, task_mem: int = 256):
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.task_cpu = task_cpu
        self.task_mem = task_mem
        self.task_counter = 0
        self.task_key_map: Dict[str, str] = {}
        self.log = executor.log
        self.client = executor.client
        self.executor = executor
        self.driver = None

        if not conf.get('mesos', 'DOCKER_IMAGE_SLAVE'):
            self.log.error("Expecting docker image for  mesos executor")
            raise AirflowException("mesos.slave_docker_image not provided for mesos executor")

        self.mesos_slave_docker_image = conf.get('mesos', 'DOCKER_IMAGE_SLAVE').replace('"', '')
        self.mesos_docker_volume_driver = conf.get('mesos', 'DOCKER_VOLUME_DRIVER').replace('"', '')
        self.mesos_docker_volume_dag_name = conf.get('mesos', 'DOCKER_VOLUME_DAG_NAME').replace('"', '')
        self.mesos_docker_volume_dag_container_path = conf.get(
            'mesos', 'DOCKER_VOLUME_DAG_CONTAINER_PATH'
        ).replace('"', '')
        self.mesos_docker_volume_logs_name = conf.get('mesos', 'DOCKER_VOLUME_LOGS_NAME').replace('"', '')
        self.mesos_docker_volume_logs_container_path = conf.get(
            'mesos', 'DOCKER_VOLUME_LOGS_CONTAINER_PATH'
        ).replace('"', '')
        self.core_sql_alchemy_conn = conf.get('core', 'SQL_ALCHEMY_CONN')
        self.core_fernet_key = conf.get('core', 'FERNET_KEY')

    def resource_offers(self, offers):
        """If we got a offer, run a queued task"""
        self.log.debug('MESOS OFFER')
        for i, offer in enumerate(offers):
            if i == 0:
                self.run_job(offer)
            offer.decline()
            i += 1

    def run_job(self, mesos_offer):
        """Start a queued Airflow task in Mesos"""
        offer = mesos_offer.get_offer()
        tasks = []
        option = {}
        offer_cpus = 0
        offer_mem = 0
        for resource in offer['resources']:
            if resource['name'] == "cpus":
                offer_cpus += resource['scalar']['value']
            elif resource['name'] == "mem":
                offer_mem += resource['scalar']['value']

        self.log.debug(
            "Received offer %s with cpus: %s and mem: %s", offer['id']['value'], offer_cpus, offer_mem
        )

        remaining_cpus = offer_cpus
        remaining_mem = offer_mem

        while (
            (not self.task_queue.empty())
            and remaining_cpus >= self.task_cpu
            and remaining_mem >= self.task_mem
        ):

            key, cmd, executor_config = self.task_queue.get()
            self.log.debug(executor_config)
            tid = self.task_counter
            self.task_counter += 1
            self.task_key_map[str(tid)] = key

            self.log.debug("Launching task %d using offer %s", tid, offer['id']['value'])

            task = {
                'name': "AirflowTask %d" % tid,
                'task_id': {'value': str(tid)},
                'agent_id': {'value': offer['agent_id']['value']},
                'resources': [
                    {'name': 'cpus', 'type': 'SCALAR', 'scalar': {'value': self.task_cpu}},
                    {'name': 'mem', 'type': 'SCALAR', 'scalar': {'value': self.task_mem}},
                ],
                'command': {
                    'shell': 'true',
                    'environment': {
                        'variables': [
                            {'name': 'AIRFLOW__CORE__SQL_ALCHEMY_CONN', 'value': self.core_sql_alchemy_conn},
                            {'name': 'AIRFLOW__CORE__FERNET_KEY', 'value': self.core_fernet_key},
                            {'name': 'AIRFLOW__CORE__LOGGING_LEVEL', 'value': 'DEBUG'},
                        ]
                    },
                    'value': " ".join(cmd),
                },
                'container': {
                    'type': 'DOCKER',
                    'volumes': [
                        {
                            'container_path': self.mesos_docker_volume_dag_container_path,
                            'mode': 'RW',
                            'source': {
                                'type': 'DOCKER_VOLUME',
                                'docker_volume': {
                                    'driver': self.mesos_docker_volume_driver,
                                    'name': self.mesos_docker_volume_dag_name,
                                },
                            },
                        },
                        {
                            'container_path': self.mesos_docker_volume_logs_container_path,
                            'mode': 'RW',
                            'source': {
                                'type': 'DOCKER_VOLUME',
                                'docker_volume': {
                                    'driver': self.mesos_docker_volume_driver,
                                    'name': self.mesos_docker_volume_logs_name,
                                },
                            },
                        },
                    ],
                    'docker': {
                        'image': self.mesos_slave_docker_image,
                        'force_pull_image': 'true',
                        'privileged': 'true',
                        'parameters': [
                            {'key': 'volume', 'value': self.mesos_docker_sock + ':/var/run/docker.sock'}
                        ],
                    },
                },
            }

            option = {'Filters': {'RefuseSeconds': '0.5'}}

            tasks.append(task)
            remaining_cpus -= self.task_cpu
            remaining_mem -= self.task_mem
        mesos_offer.accept(tasks, option)

    @provide_session
    def subscribed(self, driver, session=None):
        """
        Subscribe to Mesos Master

        :param driver: Mesos driver object
        """
        from airflow.models import Connection

        # Update the Framework ID in the database.
        conn_id = FRAMEWORK_CONNID_PREFIX + get_framework_name()
        connection = session.query(Connection).filter_by(conn_id=conn_id).first()
        if connection is None:
            connection = Connection(conn_id=conn_id, conn_type='mesos_framework-id', extra=driver.frameworkId)
        else:
            connection.extra = driver.frameworkId

        self.driver = driver

    def status_update(self, update):
        """Update the Status of the Tasks. Based by Mesos Events."""
        task_id = update["status"]["task_id"]["value"]
        task_state = update["status"]["state"]

        self.log.info("Task %s is in state %s", task_id, task_state)

        try:
            key = self.task_key_map[task_id]

        except KeyError:
            # The map may not contain an item if the framework re-registered
            # after a failover.
            # Discard these tasks.
            self.log.info("Unrecognised task key %s", task_id)
            return

        if task_state == "TASK_FINISHED":
            self.result_queue.put((key, State.SUCCESS))
            return

        if task_state in ('TASK_LOST', 'TASK_KILLED', 'TASK_FAILED'):
            self.result_queue.put((key, State.FAILED))
            return


class MesosExecutor(BaseExecutor):
    """
    MesosExecutor allows distributing the execution of task
    instances to multiple mesos workers.

    Apache Mesos is a distributed systems kernel which abstracts
    CPU, memory, storage, and other compute resources away from
    machines (physical or virtual), enabling fault-tolerant and
    elastic distributed systems to easily be built and run effectively.
    See http://mesos.apache.org/
    """

    class MesosFramework(threading.Thread):
        """MesosFramework class to start the threading"""

        def __init__(self, client):
            super().__init__(target=self)
            self.client = client
            self.stop = False

        def run(self):
            try:
                self.client.register()
            except KeyboardInterrupt:
                print("Stop requested by user, stopping framework....")

    def __init__(self):
        super().__init__()
        self.commands_to_run = []
        self.task_queue = Queue()
        self.result_queue = Queue()
        self.driver = None
        self.client = None
        self.mesos_framework = None

    @provide_session
    def start(self, session=None):
        """Setup and start routine to connect with the mesos master"""
        master = conf.get('mesos', 'MASTER')

        framework_name = get_framework_name()
        framework_id = None

        task_cpu = conf.getint('mesos', 'TASK_CPU', fallback=1)
        task_memory = conf.getint('mesos', 'TASK_MEMORY', fallback=256)

        if conf.getboolean('mesos', 'CHECKPOINT'):
            framework_checkpoint = True

            if conf.get('mesos', 'FAILOVER_TIMEOUT'):
                # Import here to work around a circular import error
                from airflow.models import Connection

                # Query the database to get the ID of the Mesos Framework, if available.
                conn_id = FRAMEWORK_CONNID_PREFIX + framework_name
                connection = session.query(Connection).filter_by(conn_id=conn_id).first()
                if connection is not None:
                    # Set the Framework ID to let the scheduler reconnect
                    # with running tasks.
                    framework_id = connection.extra

                # Set Timeout in the case of a mesos master leader change
                framework_failover_timeout = conf.getint('mesos', 'FAILOVER_TIMEOUT')

        else:
            framework_checkpoint = False

        self.log.info(
            'MesosFramework master : %s, name : %s, cpu : %d, mem : %d, checkpoint : %s, id : %s',
            master,
            framework_name,
            task_cpu,
            task_memory,
            framework_checkpoint,
            framework_id,
        )

        master_urls = "https://" + master

        self.client = MesosClient(
            mesos_urls=master_urls.split(','), frameworkName=framework_name, frameworkId=None
        )

        if framework_failover_timeout:
            self.client.set_failover_timeout(framework_failover_timeout)
        if framework_checkpoint:
            self.client.set_checkpoint(framework_checkpoint)

        if conf.getboolean('mesos', 'AUTHENTICATE'):
            if not conf.get('mesos', 'DEFAULT_PRINCIPAL'):
                self.log.error("Expecting authentication principal in the environment")
                raise AirflowException("mesos.default_principal not provided in authenticated mode")
            if not conf.get('mesos', 'DEFAULT_SECRET'):
                self.log.error("Expecting authentication secret in the environment")
                raise AirflowException("mesos.default_secret not provided in authenticated mode")
            self.client.principal = conf.get('mesos', 'DEFAULT_PRINCIPAL')
            self.client.secret = conf.get('mesos', 'DEFAULT_SECRET')

        driver = AirflowMesosScheduler(self, self.task_queue, self.result_queue, task_cpu, task_memory)
        self.driver = driver
        self.client.on(MesosClient.SUBSCRIBED, driver.subscribed)
        self.client.on(MesosClient.UPDATE, driver.status_update)
        self.client.on(MesosClient.OFFERS, driver.resource_offers)
        self.mesos_framework = MesosExecutor.MesosFramework(self.client)
        self.mesos_framework.start()

    def sync(self) -> None:
        """Updates states of the tasks."""
        self.log.debug("Update state of tasks")
        if self.running:
            self.log.debug('self.running: %s', self.running)

        while not self.result_queue.empty():
            results = self.result_queue.get()
            key, state = results
            if state == "success":
                self.log.info("tasks successfull %s", key)
                self.task_queue.task_done()
            if state == "failed":
                self.log.info("tasks failed %s", key)
                self.task_queue.task_done()
            self.change_state(*results)

    def execute_async(
        self,
        key: TaskInstanceKey,
        command: CommandType,
        queue: Optional[str] = None,
        executor_config: Optional[Any] = None,
    ):
        """Execute Tasks"""
        self.log.info('Add task %s with command %s with TaskInstance %s', key, command, executor_config)
        self.validate_command(command)
        self.task_queue.put((key, command, executor_config))

    def end(self) -> None:
        """Called when the executor shuts down"""
        self.log.info('Shutting down Mesos Executor')
        # Both queues should be empty...
        self.task_queue = Queue()
        self.result_queue = Queue()
        self.task_queue.join()
        self.result_queue.join()
        self.client.stop = True
        self.driver.tearDown()
        self.mesos_framework.stop = True

    def terminate(self):
        """Terminate the executor is not doing anything."""
        self.end()
