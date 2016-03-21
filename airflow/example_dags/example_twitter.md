# Example Twitter DAG

<p><strong>Introduction: </strong>This example dag depicts a typical ETL process and is a perfect use case automation scenario for Airflow. <i>Please note that the main scripts associated with the tasks are returning None.</i> The purpose of this DAG is to demonstrate how to write a functional DAG within Airflow.</p>

<p><strong>Background:</strong> Twitter is a social networking platform that enables users to send or broadcast short messages (140 Characters). A user has a user ID, i.e. JohnDoe, which is also known as a Twitter Handle. A short message, or tweet, can either be sent directed at another user using the @ symbol (i.e. @JohnDoe) or can be broadcast with a hashtag # followed by the topic name. <i>As most of the data on twitter is public, and twitter provides a generous API to retrieve these data, Twitter is the so called Gold Mine for Text Mining based data analytic.</i> This example DAG was driven out of our real use case, where we have used the SEARCH API from twitter to retrieve tweets from yesterday. The DAG is scheduled to run each day, and therefore works in an ETL fashion.</p>


<p><strong>Overview:</strong> At first, we need tasks that will get the tweets of our interest and save them on the hard-disk. Then, we need subsequent tasks that will clean and analyze the tweets. Then we want to store these files into HDFS, and load them into a Data Warehousing platform like Hive or HBase. The main reason we have selected Hive here is because it gives us a familiar SQL like interface, and makes our life of writing different queries a lot easier. Finally, the DAG needs to store a summarized result to a traditional database, i.e. MySQL or PostgreSQL, which is used by a reporting or business intelligence application. In other words, we basically want to achieve the following steps:</p>

1. Fetch Tweets
2. Clean Tweets 
3. Analyze Tweets 
4. Put Tweets to HDFS 
5. Load data to Hive 
6. Save Summary to MySQL 

<p><strong>Screenshot:</strong></p>
<img src="http://i.imgur.com/rRpSO12.png" width="99%"/>

<p><strong>Example Structure:</strong> <u>In this example dag, we are collecting tweets for four users account or twitter handle. Each twitter handle has two channels, incoming tweets and outgoing tweets. Hence, in this example, by running the fetch_tweet task, we should have eight output files.</u> For better management, each of the eight output files should be saved with the yesterday's date (we are collecting tweets from yesterday), i.e. toTwitter_A_2016-03-21.csv. We are using three kind of operators: PythonOperator, BashOperator, and HiveOperator. However, for this example only the Python scripts are stored externally. Hence this example DAG only has the following directory structure: <br/><br/>
```
example_dags
│   example_twitter.md
│   example_twitter_dag.py.txt    
│
└───python_scripts
    │
    ├───example_twitter
    │   │   __init__.py
    │   │   analyzeapi.py
    │   │   brokerapi.py
	│   │   cleanapi.py
	│   │   twitterapi.py
```
</p>
<p>The python scripts here are just placeholders, ensuring the DAG loads and doesn't break the start up process of Airflow server. In case you are interested to actually make this DAG fully functional, first start with filling out the scripts in chronological order. My approach was to store the retrieved data in memory using Pandas dataframe first, and then use the built in method to save the CSV file on hard-disk.</p>
<p>The eight different CSV files are then put into eight different folders within HDFS. Each of the newly inserted files are then loaded into eight different external hive tables. Hive tables can be external or internal. In this case, we are inserting the data right into the table, and so we are making our tables internal. Each file is inserted into the respected Hive table named after the twitter channel, i.e. toTwitter_A or fromTwitter_A. It is also important to note that when we created the tables, we facilitated for partitioning by date using the variable dt and declared comma as the row deliminator. The partitioning is very handy and ensures our query execution time remains constant even with growing volume of data.</p>
<p>As most probably these folders and hive tables doesn't exist in your system, you will get an error for these tasks within the DAG. If you rebuild a function DAG from this example, make sure those folders and hive tables exists. When you create the table, keep the consideration of table partitioning and declaring comma as the row deliminator in your mind. Furthermore, you may also need to skip headers on each read and ensure that the user under which you have Airflow running has the right permission access. Below is a sample HQL snippet on creating such table:</p>
<p><code>
CREATE TABLE toTwitter_A(id BIGINT, id_str STRING<br/>
			 created_at STRING, text STRING) <br/>
                        PARTITIONED BY (dt STRING) <br/>
			ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' <br/>
			STORED AS TEXTFILE; <br/>
			alter table toTwitter_A SET serdeproperties ('skip.header.line.count' = '1');
</code></p>
<p>When you review the code for the DAG, you will notice that these tasks are generated using for loop. These two for loops could be combined into one loop. However, in most cases, you will be running different analysis on your incoming incoming and outgoing tweets, and hence they are kept seperated in this example.</p>
<p>Final step is a running the broker script, brokerapi.py, which will run queries in Hive and store the summarized data to MySQL in our case. To connect to Hive, pyhs2 library is extremely useful and easy to use. To insert data into MySQL from Python, sqlalchemy is also a good one to use.</p>