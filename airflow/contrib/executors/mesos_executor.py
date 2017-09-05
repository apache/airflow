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

from future import standard_library

from airflow.utils.log.LoggingMixin import LoggingMixin
from airflow.www.utils import LoginMixin

standard_library.install_aliases()
from builtins import str
from queue import Queue

import mesos.interface
from mesos.interface import mesos_pb2
import mesos.native

from airflow import configuration
from airflow.executors.base_executor import BaseExecutor
from airflow.settings import Session
from airflow.utils.state import State
from airflow.exceptions import AirflowException


DEFAULT_FRAMEWORK_NAME = 'Airflow'
FRAMEWORK_CONNID_PREFIX = 'mesos_framework_'


def get_framework_name():
    if not configuration.get('mesos', 'FRAMEWORK_NAME'):
        return DEFAULT_FRAMEWORK_NAME
    return configuration.get('mesos', 'FRAMEWORK_NAME')


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
                 result_queue,
                 task_cpu=1,
                 task_mem=256):
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.task_cpu = task_cpu
        self.task_mem = task_mem
        self.task_counter = 0
        self.task_key_map = {}

    def registered(self, driver, frameworkId, masterInfo):
        self.logger.info("AirflowScheduler registered to Mesos with framework ID %s", frameworkId.value)

        if configuration.getboolean('mesos', 'CHECKPOINT') and configuration.get('mesos', 'FAILOVER_TIMEOUT'):
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
        self.logger.info("AirflowScheduler re-registered to mesos")

    def disconnected(self, driver):
        self.logger.info("AirflowScheduler disconnected from mesos")

    def offerRescinded(self, driver, offerId):
        self.logger.info("AirflowScheduler offer %s rescinded", str(offerId))

    def frameworkMessage(self, driver, executorId, slaveId, message):
        self.logger.info("AirflowScheduler received framework message %s", message)

    def executorLost(self, driver, executorId, slaveId, status):
        self.logger.warning("AirflowScheduler executor %s lost", str(executorId))

    def slaveLost(self, driver, slaveId):
        self.logger.warning("AirflowScheduler slave %s lost", str(slaveId))

    def error(self, driver, message):
        self.logger.error("AirflowScheduler driver aborted %s", message)
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

            self.logger.info("Received offer %s with cpus: %s and mem: %s", offer.id.value, offerCpus, offerMem)

            remainingCpus = offerCpus
            remainingMem = offerMem

            while (not self.task_queue.empty()) and \
                  remainingCpus >= self.task_cpu and \
                  remainingMem >= self.task_mem:
                key, cmd = self.task_queue.get()
                tid = self.task_counter
                self.task_counter += 1
                self.task_key_map[str(tid)] = key

                self.logger.info("Launching task %d using offer %s", tid, offer.id.value)

                task = mesos_pb2.TaskInfo()
                task.task_id.value = str(tid)
                task.slave_id.value = offer.slave_id.value
                task.name = "AirflowTask %d" % tid

                cpus = task.resources.add()
                cpus.name = "cpus"
                cpus.type = mesos_pb2.Value.SCALAR
                cpus.scalar.value = self.task_cpu

                mem = task.resources.add()
                mem.name = "mem"
                mem.type = mesos_pb2.Value.SCALAR
                mem.scalar.value = self.task_mem

                command = mesos_pb2.CommandInfo()
                command.shell = True
                command.value = cmd
                task.command.MergeFrom(command)

                tasks.append(task)

                remainingCpus -= self.task_cpu
                remainingMem -= self.task_mem

            driver.launchTasks(offer.id, tasks)

    def statusUpdate(self, driver, update):
        self.logger.info(
            "Task %s is in state %s, data %s",
            update.task_id.value, mesos_pb2.TaskState.Name(update.state), str(update.data)
        )

        try:
            key = self.task_key_map[update.task_id.value]
        except KeyError:
            # The map may not contain an item if the framework re-registered after a failover.
            # Discard these tasks.
            self.logger.warning("Unrecognised task key %s", update.task_id.value)
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

        if not configuration.get('mesos', 'MASTER'):
            self.logger.error("Expecting mesos master URL for mesos executor")
            raise AirflowException("mesos.master not provided for mesos executor")

        master = configuration.get('mesos', 'MASTER')

        framework.name = get_framework_name()

        if not configuration.get('mesos', 'TASK_CPU'):
            task_cpu = 1
        else:
            task_cpu = configuration.getint('mesos', 'TASK_CPU')

        if not configuration.get('mesos', 'TASK_MEMORY'):
            task_memory = 256
        else:
            task_memory = configuration.getint('mesos', 'TASK_MEMORY')

        if configuration.getboolean('mesos', 'CHECKPOINT'):
            framework.checkpoint = True

            if configuration.get('mesos', 'FAILOVER_TIMEOUT'):
                # Import here to work around a circular import error
                from airflow.models import Connection

                # Query the database to get the ID of the Mesos Framework, if available.
                conn_id = FRAMEWORK_CONNID_PREFIX + framework.name
                session = Session()
                connection = session.query(Connection).filter_by(conn_id=conn_id).first()
                if connection is not None:
                    # Set the Framework ID to let the scheduler reconnect with running tasks.
                    framework.id.value = connection.extra

                framework.failover_timeout = configuration.getint('mesos', 'FAILOVER_TIMEOUT')
        else:
            framework.checkpoint = False

        self.logger.info(
            'MesosFramework master : %s, name : %s, cpu : %s, mem : %s, checkpoint : %s',
            master, framework.name, str(task_cpu), str(task_memory), str(framework.checkpoint)
        )

        implicit_acknowledgements = 1

        if configuration.getboolean('mesos', 'AUTHENTICATE'):
            if not configuration.get('mesos', 'DEFAULT_PRINCIPAL'):
                self.logger.error("Expecting authentication principal in the environment")
                raise AirflowException("mesos.default_principal not provided in authenticated mode")
            if not configuration.get('mesos', 'DEFAULT_SECRET'):
                self.logger.error("Expecting authentication secret in the environment")
                raise AirflowException("mesos.default_secret not provided in authenticated mode")

            credential = mesos_pb2.Credential()
            credential.principal = configuration.get('mesos', 'DEFAULT_PRINCIPAL')
            credential.secret = configuration.get('mesos', 'DEFAULT_SECRET')

            framework.principal = credential.principal

            driver = mesos.native.MesosSchedulerDriver(
                AirflowMesosScheduler(self.task_queue, self.result_queue, task_cpu, task_memory),
                framework,
                master,
                implicit_acknowledgements,
                credential)
        else:
            framework.principal = 'Airflow'
            driver = mesos.native.MesosSchedulerDriver(
                AirflowMesosScheduler(self.task_queue, self.result_queue, task_cpu, task_memory),
                framework,
                master,
                implicit_acknowledgements)

        self.mesos_driver = driver
        self.mesos_driver.start()

    def execute_async(self, key, command, queue=None):
        self.task_queue.put((key, command))

    def sync(self):
        while not self.result_queue.empty():
            results = self.result_queue.get()
            self.change_state(*results)

    def end(self):
        self.task_queue.join()
        self.mesos_driver.stop()
