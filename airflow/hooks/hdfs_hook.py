from airflow.hooks.base_hook import BaseHook
from snakebite.client import Client, HAClient, Namenode

from airflow.utils import AirflowException


class HDFSHookException(AirflowException):
    pass


class HDFSHook(BaseHook):
    '''
    Interact with HDFS. This class is a wrapper around the snakebite library.
    '''
    def __init__(self, hdfs_conn_id='hdfs_default'):
        self.hdfs_conn_id = hdfs_conn_id

    def get_conn(self):
        '''
        Returns a snakebite HDFSClient object.
        '''
        connections = self.get_connections(self.hdfs_conn_id)
        client = None
        if len(connections) == 1:
            client = Client(connections[0].host, connections[0].port)
        elif len(connections) > 1:
            nn = [Namenode(conn.host, conn.port) for conn in connections]
            client = HAClient(nn)
        else:
            raise HDFSHookException("conn_id doesn't exist in the repository")
        return client
