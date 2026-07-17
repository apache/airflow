/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

module.exports = [
  {
    id: 'cloud',
    name: 'Cloud Platforms',
    icon: '<svg fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 15a4 4 0 004 4h9a5 5 0 10-.1-9.999 5.002 5.002 0 10-9.78 2.096A4.001 4.001 0 003 15z" /></svg>',
    color: 'cyan',
    keywords: ['amazon', 'google', 'microsoft-azure', 'alibaba', 'yandex', 'oracle'],
    description: 'AWS, GCP, Azure, and other cloud providers',
  },
  {
    id: 'databases',
    name: 'Databases',
    icon: '<svg fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4" /></svg>',
    color: 'blue',
    keywords: ['postgres', 'mysql', 'mongo', 'redis', 'neo4j', 'elasticsearch', 'cassandra', 'couchbase', 'influxdb', 'sqlite', 'odbc', 'jdbc', 'common-sql'],
    description: 'SQL, NoSQL, and time-series databases',
  },
  {
    id: 'data-warehouses',
    name: 'Data Warehouses',
    icon: '<svg fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10" /></svg>',
    color: 'purple',
    keywords: ['snowflake', 'databricks', 'teradata', 'vertica', 'trino', 'presto', 'dbt'],
    description: 'Snowflake, Databricks, and analytics platforms',
  },
  {
    id: 'messaging',
    name: 'Messaging & Notifications',
    icon: '<svg fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z" /></svg>',
    color: 'green',
    keywords: ['slack', 'sendgrid', 'smtp', 'discord', 'telegram', 'pagerduty', 'opsgenie', 'twilio'],
    description: 'Slack, email, SMS, and alerting services',
  },
  {
    id: 'ai-ml',
    name: 'AI & Machine Learning',
    icon: '<svg fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" /></svg>',
    color: 'amber',
    keywords: ['openai', 'cohere', 'anthropic', 'huggingface', 'mlflow', 'pinecone', 'qdrant', 'weaviate', 'pgvector'],
    description: 'OpenAI, vector DBs, and ML platforms',
  },
  {
    id: 'data-processing',
    name: 'Data Processing',
    icon: '<svg fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z" /></svg>',
    color: 'orange',
    keywords: ['spark', 'flink', 'beam', 'dataflow', 'pig', 'hive', 'hdfs', 'kylin', 'livy'],
    description: 'Spark, Flink, Beam, and batch processing frameworks',
  },
  {
    id: 'orchestration',
    name: 'Workflow & Orchestration',
    icon: '<svg fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" /></svg>',
    color: 'rose',
    keywords: ['standard', 'celery', 'docker', 'kubernetes', 'ssh', 'http', 'ftp', 'sftp'],
    description: 'Core operators, executors, and connectivity',
  },
];
