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

from airflow.hooks.base_hook import BaseHook
from airflow import configuration

from hdfs import InsecureClient, HdfsError

from airflow.utils.log.LoggingMixin import LoggingMixin

_kerberos_security_mode = configuration.get("core", "security") == "kerberos"
if _kerberos_security_mode:
    try:
        from hdfs.ext.kerberos import KerberosClient
    except ImportError:
        log = LoggingMixin().logger
        log.error("Could not load the Kerberos extension for the WebHDFSHook.")
        raise
from airflow.exceptions import AirflowException


class AirflowWebHDFSHookException(AirflowException):
    pass


class WebHDFSHook(BaseHook):
    """
    Interact with HDFS. This class is a wrapper around the hdfscli library.
    """
    def __init__(self, webhdfs_conn_id='webhdfs_default', proxy_user=None):
        self.webhdfs_conn_id = webhdfs_conn_id
        self.proxy_user = proxy_user

    def get_conn(self):
        """
        Returns a hdfscli InsecureClient object.
        """
        nn_connections = self.get_connections(self.webhdfs_conn_id)
        for nn in nn_connections:
            try:
                self.logger.debug('Trying namenode %s', nn.host)
                connection_str = 'http://{nn.host}:{nn.port}'.format(nn=nn)
                if _kerberos_security_mode:
                    client = KerberosClient(connection_str)
                else:
                    proxy_user = self.proxy_user or nn.login
                    client = InsecureClient(connection_str, user=proxy_user)
                client.status('/')
                self.logger.debug('Using namenode %s for hook', nn.host)
                return client
            except HdfsError as e:
                self.logger.debug(
                    "Read operation on namenode {nn.host} failed witg error: {e.message}".format(**locals())
                )
        nn_hosts = [c.host for c in nn_connections]
        no_nn_error = "Read operations failed on the namenodes below:\n{}".format("\n".join(nn_hosts))
        raise AirflowWebHDFSHookException(no_nn_error)

    def check_for_path(self, hdfs_path):
        """
        Check for the existence of a path in HDFS by querying FileStatus.
        """
        c = self.get_conn()
        return bool(c.status(hdfs_path, strict=False))

    def load_file(self, source, destination, overwrite=True, parallelism=1,
                  **kwargs):
        """
        Uploads a file to HDFS

        :param source: Local path to file or folder. If a folder, all the files
          inside of it will be uploaded (note that this implies that folders empty
          of files will not be created remotely).
        :type source: str
        :param destination: PTarget HDFS path. If it already exists and is a
          directory, files will be uploaded inside.
        :type destination: str
        :param overwrite: Overwrite any existing file or directory.
        :type overwrite: bool
        :param parallelism: Number of threads to use for parallelization. A value of
          `0` (or negative) uses as many threads as there are files.
        :type parallelism: int
        :param \*\*kwargs: Keyword arguments forwarded to :meth:`upload`.


        """
        c = self.get_conn()
        c.upload(hdfs_path=destination,
                 local_path=source,
                 overwrite=overwrite,
                 n_threads=parallelism,
                 **kwargs)
        self.logger.debug("Uploaded file %s to %s", source, destination)
