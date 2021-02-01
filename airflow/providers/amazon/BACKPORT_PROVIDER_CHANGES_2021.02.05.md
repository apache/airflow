

### Release 2021.2.5

| Commit                                                                                         | Committed   | Subject                                                                                 |
|:-----------------------------------------------------------------------------------------------|:------------|:----------------------------------------------------------------------------------------|
| [66e82969d](https://github.com/apache/airflow/commit/66e82969dd0ad656618bda4719a545bbaeed5d10) | 2021-01-31  | `Implement provider versioning tools`                                                   |
| [ecfdc60bb](https://github.com/apache/airflow/commit/ecfdc60bb607fe0d13fa7e315476c607813abab6) | 2021-01-29  | `Add bucket_name to template fileds in S3 operators (#13973)`                           |
| [d0ab7f6d3](https://github.com/apache/airflow/commit/d0ab7f6d3a2976167f9c4fb309c502a4f866f983) | 2021-01-25  | `Add ExasolToS3Operator (#13847)`                                                       |
| [6d55f329f](https://github.com/apache/airflow/commit/6d55f329f93c5cd1e94973194c0cd7caa65309e1) | 2021-01-25  | `AWS Glue Crawler Integration (#13072)`                                                 |
| [f473ca713](https://github.com/apache/airflow/commit/f473ca7130f844bc59477674e641b42b80698bb7) | 2021-01-24  | `Replace &#39;google_cloud_storage_conn_id&#39; by &#39;gcp_conn_id&#39; when using &#39;GCSHook&#39; (#13851)` |
| [a9ac2b040](https://github.com/apache/airflow/commit/a9ac2b040b64de1aa5d9c2b9def33334e36a8d22) | 2021-01-23  | `Switch to f-strings using flynt. (#13732)`                                             |
| [3fd5ef355](https://github.com/apache/airflow/commit/3fd5ef355556cf0ad7896bb570bbe4b2eabbf46e) | 2021-01-21  | `Add missing logos for integrations (#13717)`                                           |
| [29730d720](https://github.com/apache/airflow/commit/29730d720066a4c16d524e905de8cdf07e8cd129) | 2021-01-20  | `Add acl_policy to S3CopyObjectOperator (#13773)`                                       |
| [c065d3218](https://github.com/apache/airflow/commit/c065d32189bfee80ab938d96ad74f6492e9c9b24) | 2021-01-19  | `AllowDiskUse parameter and docs in MongotoS3Operator (#12033)`                         |
| [ab5fe56ac](https://github.com/apache/airflow/commit/ab5fe56ac4bda0d3fcdcbf58ed2632255b7ac713) | 2021-01-16  | `Fix bug in GCSToS3Operator (#13718)`                                                   |
| [04d278f93](https://github.com/apache/airflow/commit/04d278f93ffafb40fb6e95b41ecfa5f5cba5ef98) | 2021-01-13  | `Add S3ToFTPOperator (#11747)`                                                          |
| [8d42d9ed6](https://github.com/apache/airflow/commit/8d42d9ed69b03b372c6bc01309ef22e01b8db55f) | 2021-01-11  | `add xcom push for ECSOperator (#12096)`                                                |
| [308f1d066](https://github.com/apache/airflow/commit/308f1d06668ad427fd2483077d8e60f55ee617e6) | 2021-01-07  | `[AIRFLOW-3723] Add Gzip capability to mongo_to_S3 operator (#13187)`                   |
| [f69405fb0](https://github.com/apache/airflow/commit/f69405fb0b7c236968c730e1ad31a60eea2338c4) | 2021-01-07  | `Fix S3KeysUnchangedSensor so that template_fields work (#13490)`                       |
| [4e479e1e1](https://github.com/apache/airflow/commit/4e479e1e1b8eea71df48f5cc08a7dd15929ba177) | 2021-01-06  | `Add S3KeySizeSensor (#13049)`                                                          |
| [f7a1334ab](https://github.com/apache/airflow/commit/f7a1334abe4417409498daad52c97d3f0eb95137) | 2021-01-02  | `Add &#39;mongo_collection&#39; to template_fields in MongoToS3Operator (#13361)`               |
| [bd74eb0ca](https://github.com/apache/airflow/commit/bd74eb0ca0bb5f81cd98e2c151257a404d4a55a5) | 2020-12-31  | `Allow Tags on AWS Batch Job Submission (#13396)`                                       |
| [295d66f91](https://github.com/apache/airflow/commit/295d66f91446a69610576d040ba687b38f1c5d0a) | 2020-12-30  | `Fix Grammar in PIP warning (#13380)`                                                   |
| [625576a3a](https://github.com/apache/airflow/commit/625576a3af470cddad250735b74ba11e4880de0a) | 2020-12-18  | `Fix spelling (#13135)`                                                                 |
| [6cf76d7ac](https://github.com/apache/airflow/commit/6cf76d7ac01270930de7f105fb26428763ee1d4e) | 2020-12-18  | `Fix typo in pip upgrade command :( (#13148)`                                           |
| [5090fb0c8](https://github.com/apache/airflow/commit/5090fb0c8967d2d8719c6f4a468f2151395b5444) | 2020-12-15  | `Add script to generate integrations.json (#13073)`                                     |
| [32971a1a2](https://github.com/apache/airflow/commit/32971a1a2de1db0b4f7442ed26facdf8d3b7a36f) | 2020-12-09  | `Updates providers versions to 1.0.0 (#12955)`                                          |
| [d5589673a](https://github.com/apache/airflow/commit/d5589673a95aaced0b851ea0a4061a010a924a82) | 2020-12-08  | `Move dummy_operator.py to dummy.py (#11178) (#11293)`                                  |
| [b40dffa08](https://github.com/apache/airflow/commit/b40dffa08547b610162f8cacfa75847f3c4ca364) | 2020-12-08  | `Rename remaing modules to match AIP-21 (#12917)`                                       |
| [9b39f2478](https://github.com/apache/airflow/commit/9b39f24780e85f859236672e9060b2fbeee81b36) | 2020-12-08  | `Add support for dynamic connection form fields per provider (#12558)`                  |
| [bd90136aa](https://github.com/apache/airflow/commit/bd90136aaf5035e3234fe545b79a3e4aad21efe2) | 2020-11-30  | `Move operator guides to provider documentation packages (#12681)`                      |
| [02d94349b](https://github.com/apache/airflow/commit/02d94349be3d201ce9d37d7358573c937fd010df) | 2020-11-29  | `Don&#39;t use time.time() or timezone.utcnow() for duration calculations (#12353)`         |
| [de3b1e687](https://github.com/apache/airflow/commit/de3b1e687b26c524c6909b7b4dfbb60d25019751) | 2020-11-28  | `Move connection guides to provider documentation packages (#12653)`                    |
| [663259d4b](https://github.com/apache/airflow/commit/663259d4b541ab10ce55fec4d2460e23917062c2) | 2020-11-25  | `Fix AWS DataSync tests failing (#11020)`                                               |
| [3fa51f94d](https://github.com/apache/airflow/commit/3fa51f94d7a17f170ddc31908d36c91f4456a20b) | 2020-11-24  | `Add check for duplicates in provider.yaml files (#12578)`                              |
| [ed09915a0](https://github.com/apache/airflow/commit/ed09915a02b9b99e60689e647452addaab1688fc) | 2020-11-23  | `[AIRFLOW-5115] Bugfix for S3KeySensor failing to accept template_fields (#12389)`      |
| [370e7d07d](https://github.com/apache/airflow/commit/370e7d07d1ed1a53b73fe878425fdcd4c71a7ed1) | 2020-11-21  | `Fix Python Docstring parameters (#12513)`                                              |
| [c34ef853c](https://github.com/apache/airflow/commit/c34ef853c890e08f5468183c03dc8f3f3ce84af2) | 2020-11-20  | `Separate out documentation building per provider  (#12444)`                            |
| [008035450](https://github.com/apache/airflow/commit/00803545023b096b8db4fbd6eb473843096d7ce4) | 2020-11-18  | `Update provider READMEs for 1.0.0b2 batch release (#12449)`                            |
| [7ca0b6f12](https://github.com/apache/airflow/commit/7ca0b6f121c9cec6e25de130f86a56d7c7fbe38c) | 2020-11-18  | `Enable Markdownlint rule MD003/heading-style/header-style (#12427) (#12438)`           |
