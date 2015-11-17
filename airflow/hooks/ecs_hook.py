from airflow.hooks.base_hook import BaseHook
from airflow.utils import AirflowException
import json
import boto3

class ECSHook(BaseHook):
    
    def __init__( self, aws_conn_id="aws_credentials"):
        self.conn = self.get_connection(aws_conn_id)    
        self._a_key = None
        self._s_key = None
        self.extra_params = json.loads(self.conn.extra)
        if 'aws_secret_access_key' in self.extra_params:
            self._a_key = self.extra_params['aws_access_key_id']
            self._s_key = self.extra_params['aws_secret_access_key']
            self._region_key = self.extra_params['region_name']
        else:
            raise AirflowException("aws_credentials doesn't exist in the repository")
        
    def get_conn(self):
        #client = boto3.client('ecs',aws_access_key_id=self._a_key, 
        #                          aws_secret_access_key=self._s_key,
        #                          region_name = self._region_key)
        client = boto3.client('ecs')
        return client
    