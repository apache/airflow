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

import datetime
import subprocess
import logging
import os
import threading
import uuid

from butter import eventfd
from cgroupspy import trees
from cgroupspy.controllers import MemoryController

from airflow.task_runner.base_task_runner import BaseTaskRunner


class CgroupTaskRunner(BaseTaskRunner):
    """
    Runs the raw Airflow task in a cgroup that has containment for memory and
    cpu. It uses the resource requirements defined in the task to construct
    the settings for the cgroup.
    """

    def __init__(self, local_task_job):
        super(CgroupTaskRunner, self).__init__(local_task_job)
        self._process = None
        self._finished_running = False
        self._cpu_shares = None
        self._mem_mb_limit = None

    def _watch_for_oom(self, event_control_file, oom_control_file):
        """
        Monitors the OOM control file to write a logging message when an OOM
        occurs.

        :param event_control_file: Path to the event control file
        :type event_control_file: unicode
        :param oom_control_file: Path to the OOM control file
        :type oom_control_file: unicode
        """
        efd = eventfd.eventfd()

        with open(oom_control_file, "w") as ocfd:
            with open(event_control_file, "w") as ecfd:
                ecfd.write("%d %d" % (efd, ocfd.fileno()))

            # The read is also successful when the cgroup is deleted at exit
            os.read(efd, 8)
            if not self._finished_running:
                logging.error("Process killed due to OOM! Please check that "
                              "this task consumes less than the configured "
                              "limit of {} MB."
                              .format(self._mem_mb_limit))

    def _create_cgroup(self, path):
        """
        Create the specified cgroup.

        :param path: The path of the cgroup to create.
        E.g. cpu/mygroup/mysubgroup
        :return: the Node associated with the created cgroup.
        :rtype: cgroupspy.nodes.Node
        """
        node = trees.Tree().root
        path_split = path.split(os.sep)
        for path_element in path_split:
            name_to_node = {x.name: x for x in node.children}
            if path_element not in name_to_node:
                self.logger.info("Creating cgroup {} in {}"
                                 .format(path_element, node.path))
                node = node.create_cgroup(path_element)
            else:
                self.logger.debug("Not creating cgroup {} in {} "
                                  "since it already exists"
                                 .format(path_element, node.path))
                node = name_to_node[path_element]
        return node

    def _delete_cgroup(self, path):
        """
        Delete the specified cgroup.

        :param path: The path of the cgroup to delete.
        E.g. cpu/mygroup/mysubgroup
        """
        node = trees.Tree().root
        path_split = path.split("/")
        for path_element in path_split:
            name_to_node = {x.name: x for x in node.children}
            if path_element not in name_to_node:
                self.logger.warn("Cgroup does not exist: {}"
                                 .format(path))
                return
            else:
                node = name_to_node[path_element]
        # node is now the leaf node
        parent = node.parent
        self.logger.info("Deleting cgroup {}/{}".format(parent, node.name))
        parent.delete_cgroup(node.name)

    def start(self):
        # Create a unique cgroup name
        cgroup_name = "airflow/{}/{}".format(datetime.datetime.now().
                                             strftime("%Y-%m-%d"),
                                             str(uuid.uuid1()))

        self.mem_cgroup_name = "memory/{}".format(cgroup_name)
        self.cpu_cgroup_name = "cpu/{}".format(cgroup_name)

        # Get the resource requirements from the task
        task = self._task_instance.task
        """:type: airflow.operators.BaseOperator"""
        resources = task.resources
        """:type: airflow.utils.operator_resources.Resources"""
        cpus = resources.cpus.qty
        self._cpu_shares = cpus * 1024
        self._mem_mb_limit = resources.ram.qty

        # Create the memory cgroup
        mem_cgroup_node = self._create_cgroup(self.mem_cgroup_name)
        self.logger.info("Setting {} with {} MB of memory"
                         .format(self.mem_cgroup_name, self._mem_mb_limit))
        assert(isinstance(mem_cgroup_node.controller,
                          MemoryController))
        mem_cgroup_node.controller.limit_in_bytes = self._mem_mb_limit * 1024 * 1024

        # Create the CPU cgroup
        cpu_cgroup_node = self._create_cgroup(self.cpu_cgroup_name)
        self.logger.info("Setting {} with {} CPU shares"
                         .format(self.cpu_cgroup_name, self._cpu_shares))
        cpu_cgroup_node.controller.shares = self._cpu_shares

        # Start the process w/ cgroups
        self.logger.info("Starting task process with cgroups cpu,memory:{}"
                         .format(cgroup_name))
        cmd = ['cgexec',
               '-g',
               "cpu,memory:{}".format(cgroup_name)]
        cmd.extend(self._command)
        self.logger.info("Running: {}".format(cmd))
        self._process = subprocess.Popen(cmd)

        # Start a thread to watch for OOMs. This thread will exit when either
        # an OOM occurs, or when the cgroup is deleted.
        event_control_file = "/sys/fs/cgroup/{}/cgroup.event_control"\
            .format(self.mem_cgroup_name)
        oom_control_file = "/sys/fs/cgroup/{}/memory.oom_control"\
            .format(self.mem_cgroup_name)
        self.logger.debug("Using event control file {}"
                          .format(event_control_file))
        self.logger.debug("Using OOM control file {}"
                          .format(oom_control_file))

        # Watcher thread prints a helpful OOM error message
        oom_watcher_thread = threading.Thread(
            target=self._watch_for_oom,
            args=(event_control_file, oom_control_file))
        oom_watcher_thread.daemon = True
        oom_watcher_thread.start()

    def return_code(self):
        return self._process.poll()

    def terminate(self):
        return self._process.terminate()

    def on_finish(self):
        # Let the OOM watcher thread know we're done to avoid false OOM alarms
        self._finished_running = True
        # Clean up the cgroups
        self._delete_cgroup(self.mem_cgroup_name)
        self._delete_cgroup(self.cpu_cgroup_name)

