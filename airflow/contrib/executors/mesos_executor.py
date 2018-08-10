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

from future import standard_library

from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.www.utils import LoginMixin


from builtins import str
from queue import Queue, Empty

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

from airflow import configuration
from airflow.executors.base_executor import BaseExecutor
from airflow.settings import Session
from airflow.utils.state import State
from airflow.exceptions import AirflowException
from airflow.executors import Executors

standard_library.install_aliases()
DEFAULT_FRAMEWORK_NAME = 'Airflow'
FRAMEWORK_CONNID_PREFIX = 'mesos_framework_'


class MesosExecutorConfig:
    """
    MesosExecutorConfig allows resources and docker image to be specified
    at the operator level.
    """
    def __init__(self, image=None, request_memory=None, request_cpu=None):
        self.image = image
        self.request_memory = request_memory
        self.request_cpu = request_cpu

    def __repr__(self):
        return "{}(image={}, request_memory={} ,request_cpu={})".format(
            MesosExecutorConfig.__name__,
            self.image, self.request_memory, self.request_cpu
        )

    @staticmethod
    def from_dict(dictionary, default_config):
        """
        from_dict converts a dictionary to a MesosExecutorConfig
        """
        if dictionary is None:
            return default_config

        if not isinstance(dictionary, dict):
            raise TypeError("Input must be a dictionary")

        namespaced = dictionary.get(Executors.MesosExecutor, {})

        return MesosExecutorConfig(
            image=namespaced.get("image", default_config.image),
            request_memory=namespaced.get("request_memory",
                                          default_config.request_memory),
            request_cpu=namespaced.get("request_cpu", default_config.request_cpu),
        )

    def as_dict(self):
        """
        as_dict converts a MesosExecutorConfig to a dictionary
        """
        return {
            "image": self.image,
            "request_memory": self.request_memory,
            "request_cpu": self.request_cpu,
        }


def get_framework_name():
    if not configuration.conf.get('mesos', 'FRAMEWORK_NAME'):
        return DEFAULT_FRAMEWORK_NAME
    return configuration.conf.get('mesos', 'FRAMEWORK_NAME')


# AirflowMesosScheduler, implements Mesos Scheduler interface
# To schedule airflow jobs on mesos
class AirflowMesosScheduler(mesos.interface.Scheduler, LoggingMixin):
    """
    Airflow Mesos scheduler implements mesos scheduler interface
    to schedule airflow tasks on mesos.
    Basically, it schedules a command like
    'airflow run <dag_id> <task_instance_id> <start_date> --local -p=<pickle>'
    to run on a mesos slave.
    """
    def __init__(self,
                 task_queue,
                 result_queue):
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.task_counter = 0
        self.task_key_map = {}

    def registered(self, driver, frameworkId, masterInfo):
        self.log.info("AirflowScheduler registered to Mesos with framework ID %s",
                      frameworkId.value)

        if configuration.conf.getboolean('mesos', 'CHECKPOINT') and \
                configuration.conf.get('mesos', 'FAILOVER_TIMEOUT'):
            # Import here to work around a circular import error
            from airflow.models import Connection

            # Update the Framework ID in the database.
            session = Session()
            conn_id = FRAMEWORK_CONNID_PREFIX + get_framework_name()
            connection = Session.query(Connection).filter_by(conn_id=conn_id).first()
            if connection is None:
                connection = Connection(conn_id=conn_id, conn_type='mesos_framework-id',
                                        extra=frameworkId.value)
            else:
                connection.extra = frameworkId.value

            session.add(connection)
            session.commit()
            Session.remove()

    def reregistered(self, driver, masterInfo):
        self.log.info("AirflowScheduler re-registered to mesos")

    def disconnected(self, driver):
        self.log.info("AirflowScheduler disconnected from mesos")

    def offerRescinded(self, driver, offerId):
        self.log.info("AirflowScheduler offer %s rescinded", str(offerId))

    def frameworkMessage(self, driver, executorId, slaveId, message):
        self.log.info("AirflowScheduler received framework message %s", message)

    def executorLost(self, driver, executorId, slaveId, status):
        self.log.warning("AirflowScheduler executor %s lost", str(executorId))

    def slaveLost(self, driver, slaveId):
        self.log.warning("AirflowScheduler slave %s lost", str(slaveId))

    def error(self, driver, message):
        self.log.error("AirflowScheduler driver aborted %s", message)
        raise AirflowException("AirflowScheduler driver aborted %s" % message)

    def resourceOffers(self, driver, offers):
        for offer in offers:
            tasks = []
            offerCpus = 0
            offerMem = 0
            for resource in offer.resources:
                if resource.name == "cpus":
                    offerCpus += resource.scalar.value
                elif resource.name == "mem":
                    offerMem += resource.scalar.value

            self.log.info("Received offer %s with cpus: %s and mem: %s",
                          offer.id.value, offerCpus, offerMem)

            remainingCpus = offerCpus
            remainingMem = offerMem
            unmatched_tasks = []
            # Matches tasks to offer or adds them to unmatched list to be requeued
            while True:
                try:
                    key, cmd, mesos_executor_config = self.task_queue.get(block=False)
                except Empty:
                    for task in unmatched_tasks:
                        self.task_queue.put(task)
                    break
                task_cpu = mesos_executor_config.request_cpu
                task_mem = mesos_executor_config.request_memory
                docker_image = mesos_executor_config.image

                if remainingCpus <= task_cpu or remainingMem <= task_mem:
                    unmatched_tasks.append((key, cmd, mesos_executor_config))
                    continue

                tid = self.task_counter
                self.task_counter += 1
                self.task_key_map[str(tid)] = key

                self.log.info("Launching task %d using offer %s", tid, offer.id.value)

                task = mesos_pb2.TaskInfo()
                task.task_id.value = str(tid)
                task.slave_id.value = offer.slave_id.value
                task.name = "AirflowTask %d" % tid

                cpus = task.resources.add()
                cpus.name = "cpus"
                cpus.type = mesos_pb2.Value.SCALAR
                cpus.scalar.value = task_cpu

                mem = task.resources.add()
                mem.name = "mem"
                mem.type = mesos_pb2.Value.SCALAR
                mem.scalar.value = task_mem

                command = mesos_pb2.CommandInfo()
                command.shell = True
                command.value = cmd
                task.command.MergeFrom(command)

                # If docker image for airflow is specified in config then pull that
                # image before running the above airflow command
                if docker_image:
                    network = mesos_pb2.ContainerInfo.DockerInfo.Network.Value('BRIDGE')
                    docker = mesos_pb2.ContainerInfo.DockerInfo(
                        image=docker_image,
                        force_pull_image=False,
                        network=network
                    )
                    container = mesos_pb2.ContainerInfo(
                        type=mesos_pb2.ContainerInfo.DOCKER,
                        docker=docker
                    )
                    task.container.MergeFrom(container)

                tasks.append(task)

                remainingCpus -= task_cpu
                remainingMem -= task_mem

            driver.launchTasks(offer.id, tasks)

    def statusUpdate(self, driver, update):
        self.log.info(
            "Task %s is in state %s, data %s",
            update.task_id.value, mesos_pb2.TaskState.Name(update.state), str(update.data)
        )

        try:
            key = self.task_key_map[update.task_id.value]
        except KeyError:
            # The map may not contain an item if the framework re-registered
            # after a failover.
            # Discard these tasks.
            self.log.warning("Unrecognised task key %s", update.task_id.value)
            return

        if update.state == mesos_pb2.TASK_FINISHED:
            self.result_queue.put((key, State.SUCCESS))
            self.task_queue.task_done()

        if update.state == mesos_pb2.TASK_LOST or \
           update.state == mesos_pb2.TASK_KILLED or \
           update.state == mesos_pb2.TASK_FAILED:
            self.result_queue.put((key, State.FAILED))
            self.task_queue.task_done()


class MesosExecutor(BaseExecutor, LoginMixin):
    """
    MesosExecutor allows distributing the execution of task
    instances to multiple mesos workers.

    Apache Mesos is a distributed systems kernel which abstracts
    CPU, memory, storage, and other compute resources away from
    machines (physical or virtual), enabling fault-tolerant and
    elastic distributed systems to easily be built and run effectively.
    See http://mesos.apache.org/
    """
    def start(self):
        self.task_queue = Queue()
        self.result_queue = Queue()
        framework = mesos_pb2.FrameworkInfo()
        framework.user = ''

        if not configuration.conf.get('mesos', 'MASTER'):
            self.log.error("Expecting mesos master URL for mesos executor")
            raise AirflowException("mesos.master not provided for mesos executor")

        master = configuration.conf.get('mesos', 'MASTER')

        framework.name = get_framework_name()

        task_cpu = configuration.conf.getint('mesos', 'TASK_CPU')
        task_mem = configuration.conf.getint('mesos', 'TASK_MEMORY')

        docker_image = None
        # Configuration errors when DOCKER_IMAGE_SLAVE is not present in config
        if 'DOCKER_IMAGE_SLAVE' in configuration.as_dict()['mesos']:
            docker_image = configuration.get('mesos', 'DOCKER_IMAGE_SLAVE')
        self.default_mesos_config = MesosExecutorConfig(
            image=docker_image, request_memory=task_mem, request_cpu=task_cpu)

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
            str(task_cpu), str(task_mem), str(framework.checkpoint)
        )

        implicit_acknowledgements = 1

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

            framework.principal = credential.principal

            driver = mesos.native.MesosSchedulerDriver(
                AirflowMesosScheduler(self.task_queue,
                                      self.result_queue),
                framework,
                master,
                implicit_acknowledgements,
                credential)
        else:
            framework.principal = 'Airflow'
            driver = mesos.native.MesosSchedulerDriver(
                AirflowMesosScheduler(self.task_queue,
                                      self.result_queue),
                framework,
                master,
                implicit_acknowledgements)

        self.mesos_driver = driver
        self.mesos_driver.start()

    def execute_async(self, key, command, queue=None, executor_config=None):
        mesos_executor_config = MesosExecutorConfig.from_dict(executor_config,
                                                              self.default_mesos_config)
        self.task_queue.put((key, command, mesos_executor_config))

    def sync(self):
        while not self.result_queue.empty():
            results = self.result_queue.get()
            self.change_state(*results)

    def end(self):
        self.task_queue.join()
        self.mesos_driver.stop()
