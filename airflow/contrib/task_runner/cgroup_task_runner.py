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
import getpass
import subprocess
import os
import uuid

from cgroupspy import trees
import psutil

from airflow.task_runner.base_task_runner import BaseTaskRunner
from airflow.utils.helpers import kill_process_tree


class CgroupTaskRunner(BaseTaskRunner):
    """
    Runs the raw Airflow task in a cgroup that has containment for memory and
    cpu. It uses the resource requirements defined in the task to construct
    the settings for the cgroup.

    Note that this task runner will only work if the Airflow user has root privileges,
    e.g. if the airflow user is called `airflow` then the following entries (or an even
    less restrictive ones) are needed in the sudoers file (replacing
    /CGROUPS_FOLDER with your system's cgroups folder, e.g. '/sys/fs/cgroup/'):
    airflow ALL= (root) NOEXEC: /bin/chown /CGROUPS_FOLDER/memory/airflow/*
    airflow ALL= (root) NOEXEC: !/bin/chown /CGROUPS_FOLDER/memory/airflow/*..*
    airflow ALL= (root) NOEXEC: !/bin/chown /CGROUPS_FOLDER/memory/airflow/* *
    airflow ALL= (root) NOEXEC: /bin/chown /CGROUPS_FOLDER/cpu/airflow/*
    airflow ALL= (root) NOEXEC: !/bin/chown /CGROUPS_FOLDER/cpu/airflow/*..*
    airflow ALL= (root) NOEXEC: !/bin/chown /CGROUPS_FOLDER/cpu/airflow/* *
    airflow ALL= (root) NOEXEC: /bin/chmod /CGROUPS_FOLDER/memory/airflow/*
    airflow ALL= (root) NOEXEC: !/bin/chmod /CGROUPS_FOLDER/memory/airflow/*..*
    airflow ALL= (root) NOEXEC: !/bin/chmod /CGROUPS_FOLDER/memory/airflow/* *
    airflow ALL= (root) NOEXEC: /bin/chmod /CGROUPS_FOLDER/cpu/airflow/*
    airflow ALL= (root) NOEXEC: !/bin/chmod /CGROUPS_FOLDER/cpu/airflow/*..*
    airflow ALL= (root) NOEXEC: !/bin/chmod /CGROUPS_FOLDER/cpu/airflow/* *
    """

    def __init__(self, local_task_job):
        super(CgroupTaskRunner, self).__init__(local_task_job)
        self.process = None
        self._finished_running = False
        self._cpu_shares = None
        self._mem_mb_limit = None
        self._created_cpu_cgroup = False
        self._created_mem_cgroup = False
        self._cur_user = getpass.getuser()

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
                self.logger.debug("Creating cgroup {} in {}"
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
                self.logger.warning("Cgroup does not exist: {}"
                                    .format(path))
                return
            else:
                node = name_to_node[path_element]
        # node is now the leaf node
        parent = node.parent
        self.logger.debug("Deleting cgroup {}/{}".format(parent, node.name))
        parent.delete_cgroup(node.name)

    def start(self):
        # Use bash if it's already in a cgroup
        cgroups = self._get_cgroup_names()
        if cgroups["cpu"] != "/" or cgroups["memory"] != "/":
            self.logger.debug("Already running in a cgroup (cpu: {} memory: {} so "
                              "not creating another one"
                              .format(cgroups.get("cpu"),
                                      cgroups.get("memory")))
            self.process = self.run_command(['bash', '-c'], join_args=True)
            return

        # Create a unique cgroup name
        cgroup_name = "airflow/{}/{}".format(datetime.datetime.now().
                                             strftime("%Y-%m-%d"),
                                             str(uuid.uuid1()))

        self.mem_cgroup_name = "memory/{}".format(cgroup_name)
        self.cpu_cgroup_name = "cpu/{}".format(cgroup_name)

        # Get the resource requirements from the task
        task = self._task_instance.task
        resources = task.resources
        cpus = resources.cpus.qty
        self._cpu_shares = cpus * 1024
        self._mem_mb_limit = resources.ram.qty

        # Create the memory cgroup
        mem_cgroup_node = self._create_cgroup(self.mem_cgroup_name)
        self._created_mem_cgroup = True
        if self._mem_mb_limit > 0:
            self.logger.debug("Setting {} with {} MB of memory"
                              .format(self.mem_cgroup_name, self._mem_mb_limit))
            mem_cgroup_node.controller.limit_in_bytes = self._mem_mb_limit * 1024 * 1024

        # Create the CPU cgroup
        cpu_cgroup_node = self._create_cgroup(self.cpu_cgroup_name)
        self._created_cpu_cgroup = True
        if self._cpu_shares > 0:
            self.logger.debug("Setting {} with {} CPU shares"
                              .format(self.cpu_cgroup_name, self._cpu_shares))
            cpu_cgroup_node.controller.shares = self._cpu_shares

        # Start the process w/ cgroups
        self.logger.debug("Starting task process with cgroups cpu,memory:{}"
                          .format(cgroup_name))
        self.process = self.run_command(
            ['cgexec', '-g', 'cpu,memory:{}'.format(cgroup_name)]
        )

    def return_code(self):
        return_code = self.process.poll()
        # TODO(plypaul) Monitoring the the control file in the cgroup fs is better than
        # checking the return code here. The PR to use this is here:
        # https://github.com/plypaul/airflow/blob/e144e4d41996300ffa93947f136eab7785b114ed/airflow/contrib/task_runner/cgroup_task_runner.py#L43
        # but there were some issues installing the python butter package and
        # libseccomp-dev on some hosts for some reason.
        # I wasn't able to track down the root cause of the package install failures, but
        # we might want to revisit that approach at some other point.
        if return_code == 137:
            self.logger.warning("Task failed with return code of 137. This may indicate "
                                "that it was killed due to excessive memory usage. "
                                "Please consider optimizing your task or using the "
                                "resources argument to reserve more memory for your "
                                "task")
        return return_code

    def terminate(self):
        if self.process and psutil.pid_exists(self.process.pid):
            kill_process_tree(self.logger, self.process.pid)

    def on_finish(self):
        # Let the OOM watcher thread know we're done to avoid false OOM alarms
        self._finished_running = True
        # Clean up the cgroups
        if self._created_mem_cgroup:
            self._delete_cgroup(self.mem_cgroup_name)
        if self._created_cpu_cgroup:
            self._delete_cgroup(self.cpu_cgroup_name)

    def _get_cgroup_names(self):
        """
        :return: a mapping between the subsystem name to the cgroup name
        :rtype: dict[str, str]
        """
        with open("/proc/self/cgroup") as f:
            lines = f.readlines()
            d = {}
            for line in lines:
                line_split = line.rstrip().split(":")
                subsystem = line_split[1]
                group_name = line_split[2]
                d[subsystem] = group_name
            return d
