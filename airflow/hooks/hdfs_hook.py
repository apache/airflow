from airflow.hooks.base_hook import BaseHook
from airflow import configuration

try:
    snakebite_imported = True
    from snakebite.client import Client, HAClient, Namenode, AutoConfigClient
except ImportError:
    snakebite_imported = False

from airflow.utils import AirflowException


class HDFSHookException(AirflowException):
    pass


class HDFSHook(BaseHook):
    '''
    Interact with HDFS. This class is a wrapper around the snakebite library.
    '''
    def __init__(self, hdfs_conn_id='hdfs_default', proxy_user=None):
        if not snakebite_imported:
            raise ImportError(
                'This HDFSHook implementation requires snakebite, but '
                'snakebite is not compatible with Python 3 '
                '(as of August 2015). Please use Python 2 if you require '
                'this hook  -- or help by submitting a PR!')
        self.hdfs_conn_id = hdfs_conn_id
        self.proxy_user = proxy_user

    def get_conn(self):
        '''
        Returns a snakebite HDFSClient object.
        '''
        connections = self.get_connections(self.hdfs_conn_id)

        use_sasl = False
        if configuration.get('core', 'security') == 'kerberos':
            use_sasl = True

        client = None

        ''' When using HAClient, proxy_user must be the same, so is ok to always take the first '''
        effective_user = self.proxy_user or connections[0].login
        if len(connections) == 1:
            autoconfig = connections[0].extra_dejson.get('autoconfig', False)
            if autoconfig:
                client = AutoConfigClient(effective_user=effective_user, use_sasl=use_sasl)
            else:
                hdfs_namenode_principal = connections[0].extra_dejson.get('hdfs_namenode_principal')
                client = Client(connections[0].host, connections[0].port,
                                effective_user=effective_user, use_sasl=use_sasl,
                                hdfs_namenode_principal=hdfs_namenode_principal)
        elif len(connections) > 1:
            hdfs_namenode_principal = connections[0].extra_dejson.get('hdfs_namenode_principal')
            nn = [Namenode(conn.host, conn.port) for conn in connections]
            client = HAClient(nn, effective_user=effective_user, use_sasl=use_sasl,
                              hdfs_namenode_principal=hdfs_namenode_principal)
        else:
            raise HDFSHookException("conn_id doesn't exist in the repository")
        
        return client
