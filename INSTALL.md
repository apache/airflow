# INSTALL / BUILD instructions for Apache Airflow

This is a generic installation method that requires a number of dependencies to be installed.

Depending on your system you might need different prerequisites, but the following
systems/prerequisites are known to work:

Linux (Debian Bullseye and Linux Mint Debbie):

      sudo apt install build-essential python3-dev libsqlite3-dev openssl \
                 sqlite default-libmysqlclient-dev libmysqlclient-dev postgresql

On Ubuntu 20.04 you may get an error of ```mariadb_config not found```
and mysql_config not found.

Install MariaDB development headers:

      sudo apt-get install libmariadb-dev libmariadbclient-dev

MacOS (Mojave/Catalina):

      brew install sqlite mysql postgresql

- [Optional] Fetch the tarball and untar the source move into the directory that was untarred.
- [Optional] Run Apache RAT (release audit tool) to validate license headers
- RAT docs here:
  ```https://creadur.apache.org/rat/.```
- Requires Java and Apache Rat:
  
      java -jar apache-rat.jar -E ./.rat-excludes -d
- [Required] Instead of fetching and untarring the source tarball, you can use `pip` to directly install Apache Airflow with a specified version.

      pip install apache-airflow==<version>

**[optional] Airflow pulls in quite a lot of dependencies in order
to connect to other services. You might want to test or run Airflow
from a virtual env to make sure those dependencies are separated
from your system wide versions**

      python3 -m venv PATH_TO_YOUR_VENV 
      source PATH_TO_YOUR_VENV/bin/activate

### [required] Building and installing by pip (preferred)
      pip install .

#### Or directly
      python setup.py install

**You can also install recommended version of the dependencies by using
constraint- ```python<PYTHON_MAJOR_MINOR_VERSION>.txt``` files as constraint file. This is needed in case
you have problems with installing the current requirements from PyPI.
There are different constraint files for different python versions. For example:**

      pip install . \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-main/constraints-3.8.txt"


By default `pip install` in Airflow 2.0 installs only the provider packages that are needed by the extras and
install them as packages from PyPI rather than from local sources:

      pip install .[google,amazon] \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-main/constraints-3.8.txt"


You can upgrade just airflow, without paying attention to provider's dependencies by using 'constraints-no-providers'
constraint files. This allows you to keep installed provider packages.

      pip install . --upgrade \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-main/constraints-no-providers-3.8.txt"


You can also install airflow in "editable mode" (with `-e`) flag and then provider packages are
available directly from the sources (and the provider packages installed from `PyPI` are UNINSTALLED in
order to avoid having providers in two places. And `provider.yaml` files are used to discover capabilities
of the providers which are part of the airflow source code.

You can read more about `provider.yaml` and community-managed providers in
`https://airflow.apache.org/docs/apache-airflow-providers/index.html` for developing custom providers
and in ``CONTRIBUTING.rst`` for developing community maintained providers.

This is useful if you want to develop providers:

      pip install -e . \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-main/constraints-3.8.txt"

You can also skip installing provider packages from `PyPI` by setting `INSTALL_PROVIDERS_FROM_SOURCE` to "true".
In this case Airflow will be installed in non-editable mode with all providers installed from the sources.
Additionally `provider.yaml` files will also be copied to providers folders which will make the providers
discoverable by Airflow even if they are not installed from packages in this case.

      INSTALL_PROVIDERS_FROM_SOURCES="true" pip install . \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-main/constraints-3.8.txt"

Airflow can be installed with extras to install some additional features (for example 'async' or 'doc' or
to install automatically providers and all dependencies needed by that provider:

      pip install .[async,google,amazon] \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-main/constraints-3.8.txt"

The list of available extras:
1. aiobotocore
2. airbyte
3. alibaba
4. all
5. all_dbs
6. amazon
7. apache.atlas
8. apache.beam
9. apache.cassandra
10. apache.drill
11. apache.druid
12. apache.flink
13. apache.hdfs
14. apache.hive
15. apache.impala
16. apache.kafka
17. apache.kylin
18. apache.livy
19. apache.pig
20. apache.pinot
21. apache.spark
22. apache.sqoop
23. apache.webhdfs
24. apprise
25. arangodb
26. asana
27. async
28. atlas
29. atlassian.jira
30. aws
31. azure
32. cassandra
33. celery
34. cgroups
35. cloudant
36. cncf.kubernetes
37. common.sql
38. crypto
39. dask
40. daskexecutor
41. databricks
42. datadog
43. dbt.cloud
44. deprecated_api
45. devel
46. devel_all
47. devel_ci
48. devel_hadoop
49. dingding
50. discord
51. doc
52. doc_gen
53. docker
54. druid
55. elasticsearch
56. exasol
57. facebook
58. ftp
59. gcp
60. gcp_api
61. github
62. github_enterprise
63. google
64. google_auth
65. grpc
66. hashicorp
67. hdfs
68. hive
69. http
70. imap
71. influxdb
72. jdbc
73. jenkins
74. kerberos
75. kubernetes
76. ldap
77. leveldb
78. microsoft.azure
79. microsoft.mssql
80. microsoft.psrp
81. microsoft.winrm
82. mongo
83. mssql
84. mysql
85. neo4j
86. odbc
87. openfaas
88. openlineage
89. opsgenie
90. oracle
91. otel
92. pagerduty
93. pandas
94. papermill
95. password
96. pinot
97. plexus
98. postgres
99. presto
100. rabbitmq
101. redis
102. s3
103. salesforce
104. samba
105. segment
106. sendgrid
107. sentry
108. sftp
109. singularity
110. slack
111. smtp
112. snowflake
113. spark
114. sqlite
115. ssh
116. statsd
117. tableau
118. tabular
119. telegram
120. trino
121. vertica
122. virtualenv
123. webhdfs
124. winrm
125. yandex
126. zendesk<br>


**For installing Airflow in development environments - see `CONTRIBUTING.rst`**

***COMPILING FRONT-END ASSETS (in case you see "Please make sure to build the frontend in `static/ directory` and then restart the server")***
<br>
**Optional : Installing yarn - 
`https://classic.yarnpkg.com/en/docs/install`**

      python setup.py compile_assets
