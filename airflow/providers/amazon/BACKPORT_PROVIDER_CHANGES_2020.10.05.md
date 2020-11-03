

### Release 2020.10.5

| Commit                                                                                         | Committed   | Subject                                                                       |
|:-----------------------------------------------------------------------------------------------|:------------|:------------------------------------------------------------------------------|
| [ca4238eb4](https://github.com/apache/airflow/commit/ca4238eb4d9a2aef70eb641343f59ee706d27d13) | 2020-10-02  | Fixed month in backport packages to October (#11242)                          |
| [5220e4c38](https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5) | 2020-10-02  | Prepare Backport release 2020.09.07 (#11238)                                  |
| [00ffedb8c](https://github.com/apache/airflow/commit/00ffedb8c402eb5638782628eb706a5f28215eac) | 2020-09-30  | Add amazon glacier to GCS transfer operator (#10947)                          |
| [e3f96ce7a](https://github.com/apache/airflow/commit/e3f96ce7a8ac098aeef5e9930e6de6c428274d57) | 2020-09-24  | Fix incorrect Usage of Optional[bool] (#11138)                                |
| [f3e87c503](https://github.com/apache/airflow/commit/f3e87c503081a3085dff6c7352640d7f08beb5bc) | 2020-09-22  | Add D202 pydocstyle check (#11032)                                            |
| [b61225a88](https://github.com/apache/airflow/commit/b61225a8850b20be17842c2428b91d873584c4da) | 2020-09-21  | Add D204 pydocstyle check (#11031)                                            |
| [2410f592a](https://github.com/apache/airflow/commit/2410f592a4ab160b377f1a9e5de3b7262b9851cc) | 2020-09-19  | Get Airflow configs with sensitive data from AWS Systems Manager (#11023)     |
| [2bf7b7cac](https://github.com/apache/airflow/commit/2bf7b7cac7858f5a6a495f1a9eb4780ec84f95b4) | 2020-09-19  | Add typing to amazon provider EMR (#10910)                                    |
| [9edfcb7ac](https://github.com/apache/airflow/commit/9edfcb7ac46917836ec956264da8876e58d92392) | 2020-09-19  | Support extra_args in S3Hook and GCSToS3Operator (#11001)                     |
| [4e1f3a69d](https://github.com/apache/airflow/commit/4e1f3a69db8614c302e4916332555034053b935c) | 2020-09-14  | [AIRFLOW-10645] Add AWS Secrets Manager Hook (#10655)                         |
| [e9add7916](https://github.com/apache/airflow/commit/e9add79160e3a16bb348e30f4e83386a371dbc1e) | 2020-09-14  | Fix Failing static tests on Master (#10927)                                   |
| [383a118d2](https://github.com/apache/airflow/commit/383a118d2df618e46d81c520cd2c4a31d81b33dd) | 2020-09-14  | Add more type annotations to AWS hooks (#10671)                               |
| [9549274d1](https://github.com/apache/airflow/commit/9549274d110f689a0bd709db829a4d69e274eed9) | 2020-09-09  | Upgrade black to 20.8b1 (#10818)                                              |
| [2934220dc](https://github.com/apache/airflow/commit/2934220dc98e295764f7791d33e121629ed2fbbb) | 2020-09-08  | Always return a list from S3Hook list methods (#10774)                        |
| [f40ac9b15](https://github.com/apache/airflow/commit/f40ac9b151124dbcd87197d6ae38c85191d41f38) | 2020-09-01  | Add placement_strategy option (#9444)                                         |
| [e4878e677](https://github.com/apache/airflow/commit/e4878e6775bbe5cb2a1d786e57e009271b78bba0) | 2020-08-31  | fix type hints for s3 hook read_key method (#10653)                           |
| [2ca615cff](https://github.com/apache/airflow/commit/2ca615cffefe97dfa38e1b7f60d9ed33c6628992) | 2020-08-29  | Update Google Cloud branding (#10642)                                         |
| [8969b7185](https://github.com/apache/airflow/commit/8969b7185ebc3c90168ce9a2fb97dfbc74d2bed9) | 2020-08-28  | Removed bad characters from AWS operator (#10590)                             |
| [8349061f9](https://github.com/apache/airflow/commit/8349061f9cb01a92c87edd349cc844c4053851e8) | 2020-08-26  | Improve Docstring for AWS Athena Hook/Operator (#10580)                       |
| [fdd9b6f65](https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3) | 2020-08-25  | Enable Black on Providers Packages (#10543)                                   |
| [3696c34c2](https://github.com/apache/airflow/commit/3696c34c28c6bc7b442deab999d9ecba24ed0e34) | 2020-08-24  | Fix typo in the word &#34;release&#34; (#10528)                                       |
| [3734876d9](https://github.com/apache/airflow/commit/3734876d9898067ee933b84af522d53df6160d7f) | 2020-08-24  | Implement impersonation in google operators (#10052)                          |
| [ee7ca128a](https://github.com/apache/airflow/commit/ee7ca128a17937313566f2badb6cc569c614db94) | 2020-08-22  | Fix broken Markdown refernces in Providers README (#10483)                    |
| [c6358045f](https://github.com/apache/airflow/commit/c6358045f9d61af63c96833cb6682d6f382a6408) | 2020-08-22  | Fixes S3ToRedshift COPY query (#10436)                                        |
| [7c206a82a](https://github.com/apache/airflow/commit/7c206a82a6f074abcc4898a005ecd2c84a920054) | 2020-08-22  | Replace assigment with Augmented assignment (#10468)                          |
| [27d08b76a](https://github.com/apache/airflow/commit/27d08b76a2d171d716a1599157a8a60a121dbec6) | 2020-08-21  | Amazon SES Hook (#10391)                                                      |
| [dea345b05](https://github.com/apache/airflow/commit/dea345b05c2cd226e70f97a3934d7456aa1cc754) | 2020-08-17  | Fix AwsGlueJobSensor to stop running after the Glue job finished (#9022)      |
| [f6734b3b8](https://github.com/apache/airflow/commit/f6734b3b850d33d3712763f93c114e80f5af9ffb) | 2020-08-12  | Enable Sphinx spellcheck for doc generation (#10280)                          |
| [82f744b87](https://github.com/apache/airflow/commit/82f744b871bb2c5e9a2d628e1c45ae16c1244240) | 2020-08-11  | Add type annotations to AwsGlueJobHook, RedshiftHook modules (#10286)         |
| [19bc97d0c](https://github.com/apache/airflow/commit/19bc97d0ce436a6ec9d8e9a5adcd48c0a769d01f) | 2020-08-10  | Revert &#34;Add Amazon SES hook (#10004)&#34; (#10276)                                |
| [f06fe616e](https://github.com/apache/airflow/commit/f06fe616e66256bdc53710de505c2c6b1bd21528) | 2020-08-10  | Add Amazon SES hook (#10004)                                                  |
| [0c77ea8a3](https://github.com/apache/airflow/commit/0c77ea8a3c417805f66d10f0c757ca218bf8dee0) | 2020-08-06  | Add type annotations to S3 hook module (#10164)                               |
| [24c8e4c2d](https://github.com/apache/airflow/commit/24c8e4c2d6e359ecc2c7d6275dccc68de4a82832) | 2020-08-06  | Changes to all the constructors to remove the args argument (#10163)          |
| [9667314b2](https://github.com/apache/airflow/commit/9667314b2fb879edc451793a8350123507e1cfd6) | 2020-08-05  | Add correct signatures for operators in amazon provider package (#10167)      |
| [000287753](https://github.com/apache/airflow/commit/000287753b478f29e6c25442ac253e3a6c8e8c87) | 2020-08-03  | Improve Typing coverage of amazon/aws/athena (#10025)                         |
| [53ada6e79](https://github.com/apache/airflow/commit/53ada6e7911f411e80ebb00be9f07a7cc0788d01) | 2020-08-03  | Add S3KeysUnchangedSensor (#9817)                                             |
| [aeea71274](https://github.com/apache/airflow/commit/aeea71274d4527ff2351102e94aa38bda6099e7f) | 2020-08-02  | Remove `args` parameter from provider operator constructors (#10097)          |
| [2b8dea64e](https://github.com/apache/airflow/commit/2b8dea64e9e8716fba8c38a1b439f7835bbd2918) | 2020-08-01  | Fix typo in Athena sensor retries (#10079)                                    |
| [1508c43ec](https://github.com/apache/airflow/commit/1508c43ec9594e801b415dd82472fa017791b759) | 2020-07-29  | Adding new SageMaker operator for ProcessingJobs (#9594)                      |
| [7d24b088c](https://github.com/apache/airflow/commit/7d24b088cd736cfa18f9214e4c9d6ce2d5865f3d) | 2020-07-25  | Stop using start_date in default_args in example_dags (2) (#9985)             |
| [8b10a4b35](https://github.com/apache/airflow/commit/8b10a4b35e45d536a6475bfe1491ee75fad50186) | 2020-07-25  | Stop using start_date in default_args in example_dags (#9982)                 |
| [33f0cd265](https://github.com/apache/airflow/commit/33f0cd2657b2e77ea3477e0c93f13f1474be628e) | 2020-07-22  | apply_default keeps the function signature for mypy (#9784)                   |
| [e7c87fe45](https://github.com/apache/airflow/commit/e7c87fe453c6a70ed087c7ffbccaacbf0d2831b9) | 2020-07-20  | Refactor AwsBaseHook._get_credentials (#9878)                                 |
| [2577f9334](https://github.com/apache/airflow/commit/2577f9334a5cb71cccd97e62b0ae2d097cb99e1a) | 2020-07-16  | Fix S3FileTransformOperator to support S3 Select transformation only (#8936)  |
| [52b6efe1e](https://github.com/apache/airflow/commit/52b6efe1ecaae74b9c2497f565e116305d575a76) | 2020-07-15  | Add option to delete by prefix to S3DeleteObjectsOperator (#9350)             |
| [553bb7af7](https://github.com/apache/airflow/commit/553bb7af7cb7a50f7141b5b89297713cee6d19f6) | 2020-07-13  | Keep functions signatures in decorators (#9786)                               |
| [2f31b3060](https://github.com/apache/airflow/commit/2f31b3060ed8274d5d1b1db7349ce607640b9199) | 2020-07-08  | Get Airflow configs with sensitive data from Secret Backends (#9645)          |
| [07b81029e](https://github.com/apache/airflow/commit/07b81029ebc2a296fb54181f2cec11fcc7704d9d) | 2020-07-08  | Allow AWSAthenaHook to get more than 1000/first page of results (#6075)       |
| [564192c16](https://github.com/apache/airflow/commit/564192c1625a552456cebb3751978c08eebdb2a1) | 2020-07-08  | Add AWS StepFunctions integrations to the aws provider (#8749)                |
| [ecce1ace7](https://github.com/apache/airflow/commit/ecce1ace7a277c948c61d7d4cbfc8632cc216559) | 2020-07-08  | [AIRFLOW-XXXX] Remove unnecessary docstring in AWSAthenaOperator              |
| [a79e2d4c4](https://github.com/apache/airflow/commit/a79e2d4c4aa105f3fac5ae6a28e29af9cd572407) | 2020-07-06  | Move provider&#39;s log task handlers to the provider package (#9604)             |
| [ee20086b8](https://github.com/apache/airflow/commit/ee20086b8c499fa40dcaac71652f21b466e7f80f) | 2020-07-02  | Move S3TaskHandler to the AWS provider package (#9602)                        |
| [40add26d4](https://github.com/apache/airflow/commit/40add26d459c2511a6d9d305ae7300f0d6104211) | 2020-06-29  | Remove almost all references to airflow.contrib (#9559)                       |
| [c858babdd](https://github.com/apache/airflow/commit/c858babddf8b18b417993b5bfefec1c5635510da) | 2020-06-26  | Remove kwargs from Super calls in AWS Secrets Backends (#9523)                |
| [87fdbd070](https://github.com/apache/airflow/commit/87fdbd0708d942af98d35604fe5962962e25d246) | 2020-06-25  | Use literal syntax instead of function calls to create data structure (#9516) |
| [c7a454aa3](https://github.com/apache/airflow/commit/c7a454aa32bf33133d042e8438ac259b32144b21) | 2020-06-22  | Add AWS ECS system test (#8888)                                               |
| [df8efd04f](https://github.com/apache/airflow/commit/df8efd04f394afc4b5affb677bc78d8b7bd5275a) | 2020-06-21  | Enable &amp; Fix &#34;Docstring Content Issues&#34; PyDocStyle Check (#9460)              |
| [e13a14c87](https://github.com/apache/airflow/commit/e13a14c8730f4f633d996dd7d3468fe827136a84) | 2020-06-21  | Enable &amp; Fix Whitespace related PyDocStyle Checks (#9458)                     |
| [d0e7db402](https://github.com/apache/airflow/commit/d0e7db4024806af35e3c9a2cae460fdeedd4d2ec) | 2020-06-19  | Fixed release number for fresh release (#9408)                                |
