#-------------------------------------------------------------------------------------------------------------------------------------
# Caveat: This Dag will not run because of missing scripts.
#         The purpose of this is to give you a sample of a real world example DAG!
#-------------------------------------------------------------------------------------------------------------------------------------

#-------------------------------------------------------------------------------------------------------------------------------------
# Load The Dependencies
#-------------------------------------------------------------------------------------------------------------------------------------

import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import time
from airflow import DAG
from airflow.operators import BashOperator
from airflow.operators import HiveOperator
from airflow.operators import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks import HiveCliHook
from datetime import date, timedelta

#-------------------------------------------------------------------------------------------------------------------------------------
# Import Scripts for Python callable
#-------------------------------------------------------------------------------------------------------------------------------------

sys.path.append("./python_scripts/example_twitter/analyze_scripts")
sys.path.append("./python_scripts/example_twitter/broker_scripts")
sys.path.append("./python_scripts/example_twitter/clean_scripts")
sys.path.append("./python_scripts/example_twitter/twitter_api")

from twitterapi import fetchtweet
from cleanapi import cleantweet
from analyzeapi import analyzetweet
from brokerapi import hivetomysql_tweet

#-------------------------------------------------------------------------------------------------------------------------------------
# set default arguments
#-------------------------------------------------------------------------------------------------------------------------------------

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016, 3, 13),
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
	'example_twitter_dag', default_args = default_args, 
	schedule_interval="@daily")

fetch_tweet = PythonOperator(
	task_id = 'fetch_tweet',
	python_callable = fetchtweet,
	dag = dag)

clean_tweet = PythonOperator(
        task_id = 'clean_tweet',
        python_callable = translatetweet,
        dag = dag)

clean_tweet.set_upstream(fetch_tweet)

analyze_tweet = PythonOperator(
	task_id = 'analyze_tweet',
	python_callable = analyzetweet,
	dag = dag)

analyze_tweet.set_upstream(clean_tweet)

hive_to_mysql = PythonOperator(
        task_id ='hive_to_mysql',
        python_callable = hivetomysql_tweets,
        dag = dag)

hive_to_mysql_hr = PythonOperator(
        task_id ='hive_to_mysql_hr',
        python_callable = run_hivetomysql_tweets_hr,
	dag = dag)

hive_to_mysql_hr.set_upstream(hive_to_mysql)

#-------------------------------------------------------------------------------------------------------------------------------------
#the following tasks 
#-------------------------------------------------------------------------------------------------------------------------------------

from_channels = ['fromingnl','fromabnamro','fromsnsbank','fromrabobank']
to_channels = ['toingnl','toabnamro','tosnsbank','torabobank']

for channel in to_channels:
	yesterday = date.today()-timedelta(days=1)
	file_name = 'senti_en_'+channel+'_'+yesterday.strftime("%Y-%m-%d")+'.csv'
	dt = yesterday.strftime("%Y-%m-%d")
	load_to_hdfs = BashOperator(
		task_id = 'put_'+channel+'_to_hdfs',
		bash_command = 'HADOOP_USER_NAME=hdfs hadoop fs -put -f /root/tweets/'+file_name+' /user/admin/tweets/'+channel+'/',
		dag = dag)
	
	load_to_hdfs.set_upstream(analyze_tweet)
	
	load_to_hive = HiveOperator(
		task_id = 'load_'+channel+'_to_hive',
		hql = "LOAD DATA INPATH " \
		"'/user/admin/tweets/"+channel+"/"+file_name+"' "\
		"INTO TABLE "+channel+" " \
		"PARTITION(dt='"+dt+"')",
		dag = dag)
	load_to_hive.set_upstream(load_to_hdfs)
	load_to_hive.set_downstream(hive_to_mysql)	

for channel in from_channels:
	yesterday = date.today()-timedelta(days=1)
	file_name = channel+'_'+yesterday.strftime("%Y-%m-%d")+'.csv'
	dt = yesterday.strftime("%Y-%m-%d")
	load_to_hdfs = BashOperator(
		task_id = 'put_'+channel+'_to_hdfs',
		bash_command = 'HADOOP_USER_NAME=hdfs hadoop fs -put -f /root/tweets/'+file_name+' /user/admin/tweets/'+channel+'/',
		dag = dag)
	
	load_to_hdfs.set_upstream(analyze_tweet)
	
	load_to_hive = HiveOperator(
		task_id = 'load_'+channel+'_to_hive',
		hql = "LOAD DATA INPATH " \
	"'/user/admin/tweets/"+channel+"/"+file_name+"' "\
		"INTO TABLE "+channel+" " \
		"PARTITION(dt='"+dt+"')",
		dag = dag)
	load_to_hive.set_upstream(load_to_hdfs)
	load_to_hive.set_downstream(hive_to_mysql)

save_follower_mysql = PythonOperator(
			task_id = 'save_follower_mysql',
			python_callable = runstorefetchfollower,
			dag = dag)

save_follower_mysql.set_upstream(hive_to_mysql)