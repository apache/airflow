import sys
reload(sys)
sys.setdefaultencoding("utf-8")

from airflow import DAG
from airflow.operators import BashOperator
from airflow.operators import HiveOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016, 3, 1),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
	'social_bee_setup', default_args = default_args, 
	schedule_interval="@once")

#create a tweet folder where all the tweets fetched from api will be dumped
t1 = BashOperator(
	task_id = 'create__tweets_dump_folder',
	bash_command = 'sudo mkdir -p /root/tweets',
	dag = dag)

#get the latest version of the script to fetch tweet manually

#create hdfs directories

t2 = BashOperator(
	task_id = 'create_main_folder_hdfs',
	bash_command = 'sudo -u hdfs hadoop fs -mkdir -p /user/admin/tweets',
	dag = dag)
	
t1.set_downstream(t2)

#create multiple (per channel) folders within hdfs 
channels = ['fromingnl','toingnl','fromabnamro','toabnamro','fromsnsbank','tosnsbank','fromrabobank','torabobank']

for channel in channels:
	create_hdfs_dir = BashOperator(
		task_id = 'create_hdfs_dir_'+channel,
		bash_command = 'HADOOP_USER_NAME=hdfs hadoop fs -mkdir -p /user/admin/tweets/'+channel,
		dag = dag)
	create_hdfs_dir.set_upstream(t2)
	
	hdfs_permission = BashOperator(
                task_id = 'hdfs_permission_'+channel,
                bash_command = 'HADOOP_USER_NAME=hdfs hadoop fs -chown root /user/admin/tweets/'+channel,
                dag = dag)

	hdfs_permission.set_upstream(create_hdfs_dir)

	drop_tables = HiveOperator(
		task_id = 'drop_table_'+channel,
		hql = 'drop table if exists '+channel,
		dag = dag)
	drop_tables.set_upstream(hdfs_permission)
	
	create_tables = HiveOperator(
		task_id = 'create_hive_'+channel+'_ext_table',
		hql = "CREATE TABLE "+channel+"(id BIGINT, " \
			"id_str STRING, created_at STRING, " \
			"in_reply_to_screen_name STRING, "\
			"in_reply_to_status_id BIGINT, " \
			"in_reply_to_status_id_str STRING, " \
			"in_reply_to_user_id BIGINT, " \
			"in_reply_to_user_id_str STRING, " \
			"favorite_count INT, retweet_count INT, " \
			"text STRING, text_en STRING, " \
                        "sentiment STRING, score FLOAT) " \
                        "PARTITIONED BY (dt STRING) "\
			"ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' "\
			"STORED AS TEXTFILE;" \
			"alter table "+channel+" SET serdeproperties ('skip.header.line.count' = '1');",
		dag = dag)

	create_tables.set_upstream(drop_tables)