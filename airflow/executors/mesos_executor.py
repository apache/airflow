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
from builtins import str
from queue import Queue
from typing import Any, Optional

from avmesos.client import MesosClient
from mesos.interface import mesos_pb2

from airflow import configuration
from airflow.exceptions import AirflowException
from airflow.executors.base_executor import BaseExecutor, CommandType
from airflow.models.taskinstance import TaskInstance, TaskInstanceKey
from airflow.settings import Session
from airflow.utils.state import State

DEFAULT_FRAMEWORK_NAME = 'Airflow'
FRAMEWORK_CONNID_PREFIX = 'mesos_framework_'


def get_framework_name():
    """
    Get the mesos framework name if its set in airflow.cfg
    """
    if not configuration.conf.get('mesos', 'FRAMEWORK_NAME'):
        return DEFAULT_FRAMEWORK_NAME
    return configuration.conf.get('mesos', 'FRAMEWORK_NAME')


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
    def __init__(self,
                 executor,
                 task_queue,
                 task_cpu=1,
                 task_mem=256):
        self.task_queue = task_queue
        self.task_cpu = task_cpu
        self.task_mem = task_mem
        self.task_counter = 0
        self.task_key_map = {}
        self.task_ti_map = {}
        self.log = executor.log
        self.client = executor.client
        self.executor = executor
        self.driver = None

        if not configuration.conf.get('mesos', 'DOCKER_IMAGE_SLAVE'):
            self.log.error("Expecting docker image for  mesos executor")
            raise AirflowException("mesos.slave_docker_image not provided for mesos executor")

        if not configuration.conf.get('mesos', 'DOCKER_VOLUME_DAG_DRIVER'):
            self.log.error("Expecting docker volume dag driver for mesos executor")
            raise AirflowException("mesos.docker_volume_dag_driver not provided for mesos executor")

        if not configuration.conf.get('mesos', 'DOCKER_VOLUME_DAG_NAME'):
            self.log.error("Expecting docker volume dag name for mesos executor")
            raise AirflowException("mesos.docker_volume_dag_name not provided for mesos executor")

        if not configuration.conf.get('mesos', 'DOCKER_VOLUME_DAG_CONTAINER_PATH'):
            self.log.error("Expecting docker volume dag container path for mesos executor")
            raise AirflowException("mesos.docker_volume_dag_container_path not provided for mesos executor")

        if not configuration.conf.get('mesos', 'DOCKER_SOCK'):
            self.log.error("Expecting docker sock path for mesos executor")
            raise AirflowException("mesos.docker_sock not provided for mesos executor")

        self.mesos_slave_docker_image = configuration.conf.get(
            'mesos', 'DOCKER_IMAGE_SLAVE'
        ).replace('"', '')
        self.mesos_docker_volume_dag_driver = configuration.conf.get(
            'mesos', 'DOCKER_VOLUME_DAG_DRIVER'
        ).replace('"', '')
        self.mesos_docker_volume_dag_name = configuration.conf.get(
            'mesos', 'DOCKER_VOLUME_DAG_NAME'
        ).replace('"', '')
        self.mesos_docker_volume_dag_container_path = configuration.conf.get(
            'mesos', 'DOCKER_VOLUME_DAG_CONTAINER_PATH'
        ).replace('"', '')
        self.mesos_docker_sock = configuration.conf.get('mesos', 'DOCKER_SOCK')
        self.core_sql_alchemy_conn = configuration.conf.get('core', 'SQL_ALCHEMY_CONN')
        self.core_fernet_key = configuration.conf.get('core', 'FERNET_KEY')

    def resource_offers(self, offers):
        """
        If we got a offer, run a queued task
        """
        self.log.debug('MESOS OFFER')
        i = 0
        for offer in offers:
            if i == 0:
                self.run_job(offer)
            else:
                offer.decline()
            i += 1

    def run_job(self, mesos_offer):
        """
        Start a queued Airflow task in Mesos
        """
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
            "Received offer %s with cpus: %s and mem: %s",
            offer['id']['value'], offer_cpus, offer_mem
        )

        remaining_cpus = offer_cpus
        remaining_mem = offer_mem

        while (not self.task_queue.empty()) \
                and remaining_cpus >= self.task_cpu \
                and remaining_mem >= self.task_mem:

            key, cmd, ti = self.task_queue.get()
            self.log.info(key)
            tid = self.task_counter
            self.task_counter += 1
            self.task_key_map[str(tid)] = key
            self.task_ti_map[str(tid)] = ti

            port_begin = 31000 + tid
            port_end = 31000 + tid

            self.log.debug(
                "Launching task %d using offer %s",
                tid, offer['id']['value']
            )

            task = {
                'name': "AirflowTask %d" % tid,
                'task_id': {'value': str(tid)},
                'agent_id': {'value': offer['agent_id']['value']},
                'resources': [
                    {
                        'name': 'cpus',
                        'type': 'SCALAR',
                        'scalar': {'value': self.task_cpu}
                    },
                    {
                        'name': 'mem',
                        'type': 'SCALAR',
                        'scalar': {'value': self.task_mem}
                    },
                    {
                        'name': 'ports',
                        'type': 'RANGES',
                        'ranges':
                        {
                            'range': [
                                {
                                    'begin': port_begin,
                                    'end': port_end
                                }
                            ]
                        }
                    }
                ],
                'command': {
                    'shell': 'true',
                    'environment': {
                        'variables': [
                            {
                                'name':'AIRFLOW__CORE__SQL_ALCHEMY_CONN',
                                'value': self.core_sql_alchemy_conn
                            },
                            {
                                'name':'AIRFLOW__CORE__FERNET_KEY',
                                'value': self.core_fernet_key
                            },
                            {
                                'name':'AIRFLOW__CORE__LOGGING_LEVEL',
                                'value': 'DEBUG'
                            },
                            {
                                'name':'AIRFLOW__CORE__DAGS_FOLDER',
                                'value': self.mesos_docker_volume_dag_container_path
                            },
                            {
                                'name':'AIRFLOW__CORE__LOAD_EXAMPLES',
                                'value': 'False'
                            }
                        ]
                    },
                    'value': " ".join(cmd)
                },
                'container': {
                    'type': 'DOCKER',
                    'volumes' : [
                        {
                            'container_path' : self.mesos_docker_volume_dag_container_path,
                            'mode' : 'RO',
                            'source' : {
                                'type' : 'DOCKER_VOLUME',
                                'docker_volume' : {
                                    'driver': self.mesos_docker_volume_dag_driver,
                                    'name' : self.mesos_docker_volume_dag_name
                                }
                            }
                        }
                    ],
                    'docker': {
                        'image': self.mesos_slave_docker_image,
                        'force_pull_image': 'true',
                        'privileged' : 'true',
                        'parameters' : [
                            {
                                'key' : 'volume',
                                'value' : self.mesos_docker_sock + ':/var/run/docker.sock'
                            }
                        ]
                    }
                }
            }

            option = {
                'Filters' : {
                    'RefuseSeconds': '0.5'
                }
            }

            tasks.append(task)
            remaining_cpus -= self.task_cpu
            remaining_mem -= self.task_mem
        mesos_offer.accept(tasks, option)

    def subscribed(self, driver):
        """
        Subscribe to Mesos Master

        :param driver: Mesos driver object
        """
        from airflow.models import Connection

        # Update the Framework ID in the database.
        session = Session()
        conn_id = FRAMEWORK_CONNID_PREFIX + get_framework_name()
        connection = Session.query(Connection).filter_by(conn_id=conn_id).first()
        if connection is None:
            connection = Connection(
                conn_id=conn_id,
                conn_type='mesos_framework-id',
                extra=driver.frameworkId
            )
        else:
            connection.extra = driver.frameworkId

        session.commit()
        self.driver = driver
        Session.remove()

    def status_update(self, update):
        """
        Update the Status of the Tasks. Based by Mesos Events.
        """
        self.log.info(
            "Task %s is in state %s", update['status']['task_id']['value'], update['status']['state'])

        try:
            key = self.task_key_map[update['status']['task_id']['value']]
            ti = self.task_ti_map[update['status']['task_id']['value']]

        except KeyError:
            # The map may not contain an item if the framework re-registered
            # after a failover.
            # Discard these tasks.
            self.log.warning("Unrecognised task key %s", update['status']['task_id']['value'])
            return

        if update['status']['state'] == "TASK_RUNNING":
            ti.set_state(State.RUNNING)
            return

        if update['status']['state'] == "TASK_FINISHED":
            self.executor.change_state(key, State.SUCCESS)
            ti.set_state(State.SUCCESS)
            self.task_queue.task_done()
            return

        if update['status']['state'] == "TASK_LOST" or \
           update['status']['state'] == "TASK_KILLED" or \
           update['status']['state'] == "TASK_FAILED":
            self.executor.change_state(key, State.FAILED)
            ti.set_state(State.FAILED)
            self.task_queue.task_done()
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
        """
        MesosFramework class to start the threading
        """
        def __init__(self, client):
            threading.Thread.__init__(self)
            self.client = client
            self.stop = False

        def run(self):
            try:
                self.client.register()
            except KeyboardInterrupt:
                print('Stop requested by user, stopping framework....')

    def __init__(self):
        super().__init__()
        self.commands_to_run = []
        self.task_queue = Queue()
        self.driver = None
        self.client = None
        self.th = None  # pylint: disable=invalid-name
        self.framework = None

    def start(self):
        """
        Setup and start routine to connect with the mesos master
        """
        framework = mesos_pb2.FrameworkInfo()
        framework.user = ''

        if not configuration.conf.get('mesos', 'MASTER'):
            self.log.error("Expecting mesos master URL for mesos executor")
            raise AirflowException("mesos.master not provided for mesos executor")

        master = configuration.conf.get('mesos', 'MASTER')

        framework.name = get_framework_name()

        if not configuration.conf.get('mesos', 'TASK_CPU'):
            task_cpu = 1
        else:
            task_cpu = configuration.conf.getint('mesos', 'TASK_CPU')

        if not configuration.conf.get('mesos', 'TASK_MEMORY'):
            task_memory = 256
        else:
            task_memory = configuration.conf.getint('mesos', 'TASK_MEMORY')

        if configuration.conf.getboolean('mesos', 'CHECKPOINT'):
            framework.checkpoint = True

            if configuration.conf.get('mesos', 'FAILOVER_TIMEOUT'):
                # Import here to work around a circular import error
                from airflow.models import Connection

                # Query the database to get the ID of the Mesos Framework, if available.
                conn_id = FRAMEWORK_CONNID_PREFIX + framework.name
                session = Session()
                connection = session.query(Connection).filter_by(conn_id=conn_id).first()
                if connection is not None:
                    # Set the Framework ID to let the scheduler reconnect
                    # with running tasks.
                    framework.id.value = connection.extra

                framework.failover_timeout = configuration.conf.getint(
                    'mesos', 'FAILOVER_TIMEOUT'
                )
        else:
            framework.checkpoint = False

        self.log.info(
            'MesosFramework master : %s, name : %s, cpu : %s, mem : %s, checkpoint : %s',
            master, framework.name,
            str(task_cpu), str(task_memory), str(framework.checkpoint)
        )

        if configuration.conf.getboolean('mesos', 'AUTHENTICATE'):
            if not configuration.conf.get('mesos', 'DEFAULT_PRINCIPAL'):
                self.log.error("Expecting authentication principal in the environment")
                raise AirflowException(
                    "mesos.default_principal not provided in authenticated mode")
            if not configuration.conf.get('mesos', 'DEFAULT_SECRET'):
                self.log.error("Expecting authentication secret in the environment")
                raise AirflowException(
                    "mesos.default_secret not provided in authenticated mode")

            credential = mesos_pb2.Credential()
            credential.principal = configuration.conf.get('mesos', 'DEFAULT_PRINCIPAL')
            credential.secret = configuration.conf.get('mesos', 'DEFAULT_SECRET')

            master_urls = "https://" + master

            self.client = MesosClient(
                mesos_urls=master_urls.split(','),
                frameworkName=framework.name,
                frameworkId=None
            )
            self.client.principal = credential.principal
            self.client.secret = credential.secret

            driver = AirflowMesosScheduler(self, self.task_queue, task_cpu, task_memory)
            self.driver = driver
            self.client.on(MesosClient.SUBSCRIBED, driver.subscribed)
            self.client.on(MesosClient.UPDATE, driver.status_update)
            self.client.on(MesosClient.OFFERS, driver.resource_offers)
            self.framework = framework
            self.th = MesosExecutor.MesosFramework(self.client)
            self.th.start()

    def queue_task_instance(
            self,
            task_instance: TaskInstance,
            mark_success: bool = False,
            pickle_id: Optional[str] = None,
            ignore_all_deps: bool = False,
            ignore_depends_on_past: bool = False,
            ignore_task_deps: bool = False,
            ignore_ti_state: bool = False,
            pool: Optional[str] = None,
            cfg_path: Optional[str] = None) -> None:
        """Queues task instance."""
        pool = pool or task_instance.pool

        command_list_to_run = task_instance.command_as_list(
            local=True,
            mark_success=mark_success,
            ignore_all_deps=ignore_all_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state,
            pool=pool,
            pickle_id=pickle_id,
            cfg_path=cfg_path)
        self.queue_command(
            task_instance,
            command_list_to_run,
            priority=task_instance.task.priority_weight_total,
            queue=task_instance.task.queue)

    def trigger_tasks(self, open_slots: int) -> None:
        """
        Triggers tasks

        :param open_slots: Number of open slots
        """
        sorted_queue = self.order_queued_tasks_by_priority()

        for _ in range(min((open_slots, len(self.queued_tasks)))):
            key, (command, _, _, ti) = sorted_queue.pop(0)
            self.queued_tasks.pop(key)
            self.running.add(key)
            self.execute_async(key=key,
                               command=command,
                               queue=None,
                               executor_config=ti)

    def shutdown(self):
        """
        Logout from Mesos Master and stop the thread
        """
        self.client.stop = True
        self.driver.tearDown()
        self.th.stop = True

    def execute_async(self,
                      key: TaskInstanceKey,
                      command: CommandType,
                      queue: Optional[str] = None,
                      executor_config: Optional[Any] = None):
        """
        Execute Tasks
        """
        # TODO: workaround for my the pickel failure
        command[5] = command[5][:-1] + "1"
        self.log.info('Add task %s with command %s with TaskInstance %s', key, command, executor_config)
        self.task_queue.put((key, command, executor_config))

    def end(self):
        self.log.info("MESOS TASK END")
        self.shutdown()
