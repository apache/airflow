from airflow.models import BaseOperator
from airflow.contrib.hooks.sftp_hook import SFTPHook
from airflow.hooks.S3_hook import S3Hook
import os


import logging
class FTPToS3(BaseOperator):
    
 
        
    ui_color = '#f0ede4'
    """
    Copy file from a FTP folder to a S3 folder

    :param s3_bucket: s3 destination bucket
    :type: s3_bucket:str
    :param s3_key: s3 destination key
    :type s3_key: str
    :param ftp_folder: ftp folder where the the source files are stored
    :type ftp_folder: str
    :param: sftp_conn_id: the configuration key defined in the ariflow panel
    :type: sftp_conn_id: str
    :param: s3_conn_id: the configuration key defined in the airflow panel
    :type: s3_conn_id: str
    :param: filter: a filter function that receiver file_name as parameters.
    :type: filter: function
    :param: replace: replace destination file
    :type: replace: bool
    :param: tmp_directory: just a temporary directory
    :type: tmp_directory: str
    """
  
    def __init__(self, s3_bucket, s3_key, ftp_folder, sftp_conn_id='sftp_default', s3_conn_id='s3_default', filter= None, replace = False, tmp_directory = "tmp"): 
        self.s3_key =  s3_key
        self.s3_bucket = s3_bucket
        self.sftp_conn_id = sftp_conn_id
        self.s3_conn_id = s3_conn_id
        self.filter = filter
        self.ftp_folder = ftp_folder
        self.tmp_directory = tmp_directory
        self.replace = replace
        
    def execute(self, context):
        sftp_hook = SFTPHook(ftp_conn_id = self.sftp_conn_id)
        s3_hook =  S3Hook(s3_conn_id = self.s3_conn_id)
        sftp_hook.get_conn()
        file_list = sftp_hook.list_directory(self.ftp_folder)
        if (self.filter != None):
            filter(self.filter, file_list)
          
        #create tmp directory
        if not os.path.exists(self.tmp_directory):
            os.makedirs(self.tmp_directory)  
        
        for file_name  in file_list:
            s3_key_file = self.s3_key +"/"+str(file_name)
            exists = s3_hook.check_for_key(s3_key_file, self.s3_bucket)
            
            if (exists) and (not self.replace):
                continue
            
            ftp_file_fullpath = self.ftp_folder + "/" + str(file_name)
            local_file_fullpath = self.tmp_directory + "/" + str(file_name)
            
            logging.info("Dowloading file ["+str(ftp_file_fullpath)+"] from sftp to local ["+str(local_file_fullpath)+"]");
            sftp_hook.get_file(ftp_file_fullpath, local_file_fullpath)
            logging.info("Done.")
            logging.info("Uploading file ["+str(local_file_fullpath)+"] to S3 on bucket ["+str(self.s3_bucket)+"] and key ["+str(s3_key_file)+"]")
            s3_hook.load_file(local_file_fullpath, s3_key_file, self.s3_bucket, self.replace)
            logging.info("Done.")
        
