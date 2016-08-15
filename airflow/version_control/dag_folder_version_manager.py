#!/usr/bin/env python
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


class DagFolderVersionManager():
    """
    Class that represents a version control strategy and is capable of returning views
    into @master_dags_folder at a given version as well as checking for the current
    version of @master_dags_folder. Under normal operation, only one instance of this
    class should be created, and the value of the core.DAGS_FOLDER configuration
    parameter should be used to create that instance. To implement different version
    control strategies (hg, svn, etc) inherit this class and implement
    `checkout_dags_folder` as well as `get_version_control_hash_of`.
    """

    def __init__(self, master_dags_folder):
        """
        @master_dags_folder can be set for dependency injection in tests
        """
        self.master_dags_folder = master_dags_folder

    def checkout_dags_folder(self, dag_version):
        """
        Return a path to a folder representing `self.master_dags_folder` as of
        @dag_version
        """
        raise NotImplementedError()

    def get_version_control_hash_of(self, filepath):
        """
        Return the version control version of @filepath.
        """
        raise NotImplementedError()
