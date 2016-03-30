from airflow.hooks.base_hook import BaseHook
from airflow import configuration
import logging

from hdfs import InsecureClient, HdfsError

_kerberos_security_mode = configuration.get("core", "security") == "kerberos"
if _kerberos_security_mode:
  try:
    from hdfs.ext.kerberos import KerberosClient
  except ImportError:
    logging.error("Could not load the Kerberos extension for the WebHDFSHook.")
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
                logging.debug('Trying namenode {}'.format(nn.host))
                connection_str = 'http://{nn.host}:{nn.port}'.format(nn=nn)
                if _kerberos_security_mode:
                  client = KerberosClient(connection_str)
                else:
                  proxy_user = self.proxy_user or nn.login
                  client = InsecureClient(connection_str, user=proxy_user)
                client.status('/')
                logging.debug('Using namenode {} for hook'.format(nn.host))
                return client
            except HdfsError as e:
                logging.debug("Read operation on namenode {nn.host} failed with"
                              " error: {e.message}".format(**locals()))
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
        logging.debug("Uploaded file {} to {}".format(source, destination))
