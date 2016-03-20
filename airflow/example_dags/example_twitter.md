# Example Twitter DAG

<p><strong>Introduction: </strong>This example dag depicts a typical ETL process and is a perfect use case automation scenario for Airflow. <i>Please note that the main scripts associated with the tasks are returning None.</i> The purpose of this DAG is to demonstrate how to write a functional DAG within Airflow.</p>

<p><strong>Background:</strong> Twitter is a social networking platform that enables users to send or broadcast short messages (140 Characters). A user has a user name, i.e. JohnDoe, which is also known as a Twitter Handle. User can </p>


<p><strong>Steps:</strong> Using our DAG, we basically want to achieve the following steps:</p>

1. Retrieve Tweets: 
2. Clean Tweets: 
3. Analyze Tweets: 
4. Put Tweets to HDFS: 
5. Load data to Hive: 
6. Save Summary to MySQL: 