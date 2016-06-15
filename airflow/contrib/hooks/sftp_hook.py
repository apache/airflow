from airflow.hooks.base_hook import BaseHook
import pysftp

class SFTPHook(BaseHook):
    
    
    def __init__(self,  ftp_conn_id='sftp_default'):
        self.ftp_conn_id = ftp_conn_id
        self.conn = None
        
    def get_conn(self):
        """
        Returns a SFTP connection object
        """
        if self.conn is None:
            params = self.get_connection(self.ftp_conn_id)
            self.conn = pysftp.Connection(params.host, username=params.login, password=params.password)

        return self.conn
    
    def list_directory(self, path):
        fileList = self.conn.listdir(path)
        fileList.reverse()
        return fileList
    
    def get_file(self, from_file, to_file):
        self.conn.get(from_file, to_file)
    
    
 
    
