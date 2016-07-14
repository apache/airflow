# -*- coding: utf-8 -*-
# Copyright (c) 2016 Bolke de Bruin
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
import os
import subprocess
import select
import re


class MiniHiveCluster(object):
    def __init__(self):
        self._minicluster_home = os.environ['MINICLUSTER_HOME']
        self._minicluster_class = "com.ing.minicluster.MiniCluster"
        self._start_mini_cluster()
        self._is_started()

    def _start_mini_cluster(self):
        classpath = os.path.join(self._minicluster_home, "*")
        cmd = ["java", "-cp", classpath, self._minicluster_class]

        self.hive = subprocess.Popen(cmd, bufsize=0, stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE, universal_newlines=True)

    def terminate(self):
        self.hive.terminate()

    def _is_started(self):
        while self.hive.poll() is None:
            rlist, wlist, xlist = select.select([self.hive.stderr, self.hive.stdout], [], [])
            for f in rlist:
                line = f.readline()
                print(line,)
                m = re.match(".*Starting ThriftBinaryCLIService", line)
                if m:
                    return True
