

### Release 2021.2.5

| Commit                                                                                         | Committed   | Subject                                                                                 |
|:-----------------------------------------------------------------------------------------------|:------------|:----------------------------------------------------------------------------------------|
| [1872d8719](https://github.com/apache/airflow/commit/1872d8719d24f94aeb1dcba9694837070b9884ca) | 2021-02-03  | `Add Apache Beam operators (#12814)`                                                    |
| [0e8c77b93](https://github.com/apache/airflow/commit/0e8c77b93a5ca5ecfdcd1c4bd91f54846fc15d57) | 2021-02-03  | `Support google-cloud-logging&#39; &gt;=2.0.0 (#13801)`                                        |
| [833e33832](https://github.com/apache/airflow/commit/833e3383230e1f6f73f8022ddf439d3d531eff01) | 2021-02-02  | `Fix four bugs in StackdriverTaskHandler (#13784)`                                      |
| [d2efb3323](https://github.com/apache/airflow/commit/d2efb33239d36e58fb69066fd23779724cb11a90) | 2021-02-02  | `Support google-cloud-monitoring&gt;=2.0.0 (#13769)`                                       |
| [ac2f72c98](https://github.com/apache/airflow/commit/ac2f72c98dc0821b33721054588adbf2bb53bb0b) | 2021-02-01  | `Implement provider versioning tools (#13767)`                                          |
| [823741cfe](https://github.com/apache/airflow/commit/823741cfea3e7a2584d1e68126db3d6e6739b08f) | 2021-01-28  | `Improve GCS system test envs (#13946)`                                                 |
| [6d6588fe2](https://github.com/apache/airflow/commit/6d6588fe2b8bb5fa33e930646d963df3e0530f23) | 2021-01-28  | `Add Google Cloud Workflows Operators (#13366)`                                         |
| [810c15ed8](https://github.com/apache/airflow/commit/810c15ed85d7bcde8d5b8bc44e1cbd4859e29d2e) | 2021-01-27  | `Fix and improve GCP BigTable hook and system test (#13896)`                            |
| [661661733](https://github.com/apache/airflow/commit/6616617331bf6e8548bf6391cebb636220c1cc53) | 2021-01-27  | `Add env variables to PubSub example dag (#13794)`                                      |
| [f473ca713](https://github.com/apache/airflow/commit/f473ca7130f844bc59477674e641b42b80698bb7) | 2021-01-24  | `Replace &#39;google_cloud_storage_conn_id&#39; by &#39;gcp_conn_id&#39; when using &#39;GCSHook&#39; (#13851)` |
| [a9ac2b040](https://github.com/apache/airflow/commit/a9ac2b040b64de1aa5d9c2b9def33334e36a8d22) | 2021-01-23  | `Switch to f-strings using flynt. (#13732)`                                             |
| [9592be88e](https://github.com/apache/airflow/commit/9592be88e57cc7f59b9eac978292abd4d7692c0b) | 2021-01-22  | `Fix Google Spanner example dag (#13842)`                                               |
| [af52fdb51](https://github.com/apache/airflow/commit/af52fdb51152a72441a44a271e498b1ec20dfd57) | 2021-01-22  | `Improve environment variables in GCP Dataflow system test (#13841)`                    |
| [e7946f1cb](https://github.com/apache/airflow/commit/e7946f1cb7c144181443cbcc843d90bd597b09b5) | 2021-01-22  | `Improve environment variables in GCP Datafusion system test (#13837)`                  |
| [61c1d6ec6](https://github.com/apache/airflow/commit/61c1d6ec6ce638f8ccd76705f69e9474c308389a) | 2021-01-22  | `Improve environment variables in GCP Memorystore system test (#13833)`                 |
| [202f66093](https://github.com/apache/airflow/commit/202f66093ad12c293f97204b0775bef2b077cd9a) | 2021-01-22  | `Improve environment variables in GCP Lifeciences system test (#13834)`                 |
| [70bf307f3](https://github.com/apache/airflow/commit/70bf307f3894214c523701940b89ac0b991a3a63) | 2021-01-21  | `Add How To Guide for Dataflow (#13461)`                                                |
| [3fd5ef355](https://github.com/apache/airflow/commit/3fd5ef355556cf0ad7896bb570bbe4b2eabbf46e) | 2021-01-21  | `Add missing logos for integrations (#13717)`                                           |
| [309788e5e](https://github.com/apache/airflow/commit/309788e5e2023c598095a4ee00df417d94b6a5df) | 2021-01-18  | `Refactor DataprocOperators to support google-cloud-dataproc 2.0 (#13256)`              |
| [7ec858c45](https://github.com/apache/airflow/commit/7ec858c4523b24e7a3d6dd1d49e3813e6eee7dff) | 2021-01-17  | `updated Google DV360 Hook to fix SDF issue (#13703)`                                   |
| [ef8617ec9](https://github.com/apache/airflow/commit/ef8617ec9d6e4b7c433a29bd388f5102a7a17c11) | 2021-01-14  | `Support google-cloud-tasks&gt;=2.0.0 (#13347)`                                            |
| [189af5404](https://github.com/apache/airflow/commit/189af54043a6aa6e7557bda6cf7cfca229d0efd2) | 2021-01-13  | `Add system tests for Stackdriver operators (#13644)`                                   |
| [a6f999b62](https://github.com/apache/airflow/commit/a6f999b62e3c9aeb10ab24342674d3670a8ad259) | 2021-01-11  | `Support google-cloud-automl &gt;=2.1.0 (#13505)`                                          |
| [947dbb73b](https://github.com/apache/airflow/commit/947dbb73bba736eb146f33117545a18fc2fd3c09) | 2021-01-11  | `Support google-cloud-datacatalog&gt;=3.0.0 (#13534)`                                      |
| [2fb68342b](https://github.com/apache/airflow/commit/2fb68342b01da4cb5d79ac9e5c0f7687d74351f3) | 2021-01-07  | `Replace deprecated module and operator in example_tasks.py (#13527)`                   |
| [003584bbf](https://github.com/apache/airflow/commit/003584bbf1d66a3545ad6e6fcdceb0410fc83696) | 2021-01-05  | `Fix failing backport packages test (#13497)`                                           |
| [7d1ea4cb1](https://github.com/apache/airflow/commit/7d1ea4cb102e7d9878eeeaab5b098ae7767b844b) | 2021-01-05  | `Replace deprecated module and operator in example_tasks.py (#13473)`                   |
| [c7d75ad88](https://github.com/apache/airflow/commit/c7d75ad887cd12d5603563c5fa873c0e2f8975aa) | 2021-01-05  | `Revert &#34;Support google-cloud-datacatalog 3.0.0 (#13224)&#34; (#13482)`                     |
| [feb84057d](https://github.com/apache/airflow/commit/feb84057d34b2f64e3b5dcbaae2d3b18f5f564e4) | 2021-01-04  | `Support google-cloud-datacatalog 3.0.0 (#13224)`                                       |
| [3a3e73998](https://github.com/apache/airflow/commit/3a3e7399810fd399d08f136e6936743c16508fc6) | 2021-01-04  | `Fix insert_all method of BigQueryHook to support tables without schema (#13138)`       |
| [c33d2c06b](https://github.com/apache/airflow/commit/c33d2c06b68c8b9a5a36c965ab8be540a2dca967) | 2021-01-02  | `Fix another pylint c-extension-no-member (#13438)`                                     |
| [f6518dd6a](https://github.com/apache/airflow/commit/f6518dd6a1217d906d863fe13dc37916efd78b3e) | 2021-01-02  | `Generalize MLEngineStartTrainingJobOperator to custom images (#13318)`                 |
| [9de712708](https://github.com/apache/airflow/commit/9de71270838ad3cc59043f1ab0bb6ca97af13622) | 2020-12-31  | `Support google-cloud-bigquery-datatransfer&gt;=3.0.0 (#13337)`                            |
| [406181d64](https://github.com/apache/airflow/commit/406181d64ac32d133523ca52f954bc50a07defc4) | 2020-12-31  | `Add Parquet data type to BaseSQLToGCSOperator (#13359)`                                |
| [295d66f91](https://github.com/apache/airflow/commit/295d66f91446a69610576d040ba687b38f1c5d0a) | 2020-12-30  | `Fix Grammar in PIP warning (#13380)`                                                   |
| [13a9747bf](https://github.com/apache/airflow/commit/13a9747bf1d92020caa5d4dc825e096ce583f2df) | 2020-12-28  | `Revert &#34;Support google-cloud-tasks&gt;=2.0.0 (#13334)&#34; (#13341)`                          |
| [04ec45f04](https://github.com/apache/airflow/commit/04ec45f045419ec87432ee285ac0828ab68008c3) | 2020-12-28  | `Add DataprocCreateWorkflowTemplateOperator (#13338)`                                   |
| [1f712219f](https://github.com/apache/airflow/commit/1f712219fa8971d98bc486896603ce8109c42844) | 2020-12-28  | `Support google-cloud-tasks&gt;=2.0.0 (#13334)`                                            |
| [f4745c8ce](https://github.com/apache/airflow/commit/f4745c8ce1955c28676b5afe129a88a61aa743b9) | 2020-12-26  | `Fix typo in example (#13321)`                                                          |
| [e9d65bd45](https://github.com/apache/airflow/commit/e9d65bd4582b083914f2fc1213bea44cf41d1a08) | 2020-12-24  | `Decode Remote Google Logs (#13115)`                                                    |
| [e7aeacf33](https://github.com/apache/airflow/commit/e7aeacf335d373007a32ac65680ba6b5b19f5c9f) | 2020-12-24  | `Add OracleToGCS Transfer (#13246)`                                                     |
| [323084e97](https://github.com/apache/airflow/commit/323084e97ddacbc5512709bf0cad8f53082d16b0) | 2020-12-24  | `Add timeout option to gcs hook methods. (#13156)`                                      |
| [0b626c804](https://github.com/apache/airflow/commit/0b626c8042b304a52d6c481fa6eb689d655f33d3) | 2020-12-22  | `Support google-cloud-redis&gt;=2.0.0 (#13117)`                                            |
| [9042a5855](https://github.com/apache/airflow/commit/9042a585539a18953d688fff455438f4061732d1) | 2020-12-22  | `Add more operators to example DAGs for Cloud Tasks (#13235)`                           |
| [8c00ec89b](https://github.com/apache/airflow/commit/8c00ec89b97aa6e725379d08c8ff29a01be47e73) | 2020-12-22  | `Support google-cloud-pubsub&gt;=2.0.0 (#13127)`                                           |
| [b26b0df5b](https://github.com/apache/airflow/commit/b26b0df5b03c4cd826fd7b2dff5771d64e18e6b7) | 2020-12-22  | `Update compatibility with google-cloud-kms&gt;=2.0 (#13124)`                              |
| [9a1d3820d](https://github.com/apache/airflow/commit/9a1d3820d6f1373df790da8751f25e723f9ce037) | 2020-12-22  | `Support google-cloud-datacatalog&gt;=1.0.0 (#13097)`                                      |
| [f95b1c9c9](https://github.com/apache/airflow/commit/f95b1c9c95c059e85ad5676daaa191929785fee2) | 2020-12-21  | `Add regional support to dataproc workflow template operators (#12907)`                 |
| [6cf76d7ac](https://github.com/apache/airflow/commit/6cf76d7ac01270930de7f105fb26428763ee1d4e) | 2020-12-18  | `Fix typo in pip upgrade command :( (#13148)`                                           |
| [23f27c1b1](https://github.com/apache/airflow/commit/23f27c1b1cdbcb6bb50fd2aa772aeda7151d5634) | 2020-12-18  | `Add system tests for CloudKMSHook (#13122)`                                            |
| [cddbf81b1](https://github.com/apache/airflow/commit/cddbf81b12650ee5905b0f762c1213caa1d3a7ed) | 2020-12-17  | `Fix Google BigQueryHook method get_schema() (#13136)`                                  |
| [1259c712a](https://github.com/apache/airflow/commit/1259c712a42d69135dc389de88f79942c70079a3) | 2020-12-17  | `Update compatibility with google-cloud-os-login&gt;=2.0.0 (#13126)`                       |
| [bcf77586e](https://github.com/apache/airflow/commit/bcf77586eff9907fa057cf2633115d5ab3e4142b) | 2020-12-16  | `Fix Data Catalog operators (#13096)`                                                   |
| [5090fb0c8](https://github.com/apache/airflow/commit/5090fb0c8967d2d8719c6f4a468f2151395b5444) | 2020-12-15  | `Add script to generate integrations.json (#13073)`                                     |
| [b4b9cf559](https://github.com/apache/airflow/commit/b4b9cf55970ca41fa7852ab8d25e59f4c379f8c2) | 2020-12-14  | `Check for missing references to operator guides (#13059)`                              |
| [1c1ef7ee6](https://github.com/apache/airflow/commit/1c1ef7ee693fead93e269dfd9774a72b6eed2e85) | 2020-12-14  | `Add project_id to client inside BigQuery hook update_table method (#13018)`            |
| [32971a1a2](https://github.com/apache/airflow/commit/32971a1a2de1db0b4f7442ed26facdf8d3b7a36f) | 2020-12-09  | `Updates providers versions to 1.0.0 (#12955)`                                          |
| [b40dffa08](https://github.com/apache/airflow/commit/b40dffa08547b610162f8cacfa75847f3c4ca364) | 2020-12-08  | `Rename remaing modules to match AIP-21 (#12917)`                                       |
| [9b39f2478](https://github.com/apache/airflow/commit/9b39f24780e85f859236672e9060b2fbeee81b36) | 2020-12-08  | `Add support for dynamic connection form fields per provider (#12558)`                  |
| [1dcd3e13f](https://github.com/apache/airflow/commit/1dcd3e13fd0a078fc9440e91b77f6f87aa60dd3b) | 2020-12-05  | `Add support for extra links coming from the providers (#12472)`                        |
| [2037303ee](https://github.com/apache/airflow/commit/2037303eef93fd36ab13746b045d1c1fee6aa143) | 2020-11-29  | `Adds support for Connection/Hook discovery from providers (#12466)`                    |
| [02d94349b](https://github.com/apache/airflow/commit/02d94349be3d201ce9d37d7358573c937fd010df) | 2020-11-29  | `Don&#39;t use time.time() or timezone.utcnow() for duration calculations (#12353)`         |
| [76bcd08dc](https://github.com/apache/airflow/commit/76bcd08dcae8d62307f5e9b8c2e182b54ed22a27) | 2020-11-28  | `Added &#39;@apply_defaults&#39; decorator. (#12620)`                                           |
| [e1ebfa68b](https://github.com/apache/airflow/commit/e1ebfa68b109b5993c47891cfd0b9b7e46b6d770) | 2020-11-27  | `Add DataflowJobMessagesSensor and DataflowAutoscalingEventsSensor (#12249)`            |
| [3fa51f94d](https://github.com/apache/airflow/commit/3fa51f94d7a17f170ddc31908d36c91f4456a20b) | 2020-11-24  | `Add check for duplicates in provider.yaml files (#12578)`                              |
| [c34ef853c](https://github.com/apache/airflow/commit/c34ef853c890e08f5468183c03dc8f3f3ce84af2) | 2020-11-20  | `Separate out documentation building per provider  (#12444)`                            |
| [9e3b2c554](https://github.com/apache/airflow/commit/9e3b2c554dadf58972198e4e16f15af2f15ec37a) | 2020-11-19  | `GCP Secrets Optional Lookup (#12360)`                                                  |
| [008035450](https://github.com/apache/airflow/commit/00803545023b096b8db4fbd6eb473843096d7ce4) | 2020-11-18  | `Update provider READMEs for 1.0.0b2 batch release (#12449)`                            |
| [7ca0b6f12](https://github.com/apache/airflow/commit/7ca0b6f121c9cec6e25de130f86a56d7c7fbe38c) | 2020-11-18  | `Enable Markdownlint rule MD003/heading-style/header-style (#12427) (#12438)`           |
| [8d0950646](https://github.com/apache/airflow/commit/8d09506464c8480fa42e8bfe6a36c6f631cd23f6) | 2020-11-18  | `Fix download method in GCSToBigQueryOperator (#12442)`                                 |
