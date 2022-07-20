
 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.


Package apache-airflow-providers-amazon
------------------------------------------------------

Amazon integration (including `Amazon Web Services (AWS) <https://aws.amazon.com/>`__).


This is detailed commit list of changes for versions provider package: ``amazon``.
For high-level changelog, see :doc:`package information including changelog <index>`.



4.0.0
.....

Latest change: 2022-06-08

=================================================================================================  ===========  ===========================================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===========================================================================================================
`dc9c083a4b <https://github.com/apache/airflow/commit/dc9c083a4b696ec5ed4e7059de2fb98222e1ab62>`_  2022-06-08   ``fix: patches #24215. Won't raise KeyError when 'create_job_kwargs' contains the 'Command' key. (#24308)``
`717a7588bc <https://github.com/apache/airflow/commit/717a7588bc8170363fea5cb75f17efcf68689619>`_  2022-06-07   ``Update package description to remove double min-airflow specification (#24292)``
`b1ad017cee <https://github.com/apache/airflow/commit/b1ad017cee66f5e042144cc7baa2d44b23b47c4f>`_  2022-06-07   ``pydocstyle D202 added (#24221)``
`aeabe994b3 <https://github.com/apache/airflow/commit/aeabe994b3381d082f75678a159ddbb3cbf6f4d3>`_  2022-06-07   ``Prepare docs for May 2022 provider's release (#24231)``
`b88ce95188 <https://github.com/apache/airflow/commit/b88ce951881914e51058ad71858874fdc00a3cbe>`_  2022-06-07   ``Update doc and sample dag for EMR Containers (#24087)``
`c23826915d <https://github.com/apache/airflow/commit/c23826915dcdca4f22b52b74633336cb2f4a1eca>`_  2022-06-07   ``Apply per-run log templates to log handlers (#24153)``
`41898d8922 <https://github.com/apache/airflow/commit/41898d89220c8525d941367a875b4806e618b0d0>`_  2022-06-06   ``Refactor GlueJobHook get_or_create_glue_job method. (#24215)``
`027b707d21 <https://github.com/apache/airflow/commit/027b707d215a9ff1151717439790effd44bab508>`_  2022-06-05   ``Add explanatory note for contributors about updating Changelog (#24229)``
`daa138c8eb <https://github.com/apache/airflow/commit/daa138c8eb586fae80ae3e5d42f6a1a547016ad0>`_  2022-06-04   ``fix: StepFunctionHook ignores explicit set 'region_name' (#23976)``
`5b32966c35 <https://github.com/apache/airflow/commit/5b32966c3545e2ed5182975764efb750eb7a3477>`_  2022-06-03   ``Remove old Athena Sample DAG (#24170)``
`595981c8ad <https://github.com/apache/airflow/commit/595981c8ad3cfeb4ad7a4514d00060e978aa9d81>`_  2022-05-31   ``Light Refactor and Clean-up AWS Provider (#23907)``
`94f2ce9342 <https://github.com/apache/airflow/commit/94f2ce9342d995f1d2eb00e6a9444e57c90e4963>`_  2022-05-30   ``Add partition related methods to GlueCatalogHook: (#23857)``
`92ddcf4ac6 <https://github.com/apache/airflow/commit/92ddcf4ac6fa452c5056b1f7cad1fca4d5759802>`_  2022-05-27   ``Introduce 'flake8-implicit-str-concat' plugin to static checks (#23873)``
`47fc253ea4 <https://github.com/apache/airflow/commit/47fc253ea432d3d794c09dfa893cdbda03af4237>`_  2022-05-24   ``Move string arg evals to 'execute()' in 'EksCreateClusterOperator' (#23877)``
`ec6761a5c0 <https://github.com/apache/airflow/commit/ec6761a5c0d031221d53ce213c0e42813606c55d>`_  2022-05-23   ``Clean up f-strings in logging calls (#23597)``
`5d2296becb <https://github.com/apache/airflow/commit/5d2296becb9401df6ca58bb7d15d6655eb168aed>`_  2022-05-22   ``Fix Amazon EKS example DAG raises warning during Imports (#23849)``
`e54ca47262 <https://github.com/apache/airflow/commit/e54ca47262579742fc3c297c7f8d4c48f2437f82>`_  2022-05-22   ``Add support for associating  custom tags to job runs submitted via EmrContainerOperator (#23769)``
`69f444f87a <https://github.com/apache/airflow/commit/69f444f87acef511da8f44ebd04ee435d10b7e5c>`_  2022-05-22   ``Add number of node params only for single-node cluster in RedshiftCreateClusterOperator (#23839)``
`509b277dce <https://github.com/apache/airflow/commit/509b277dce50fb1fbc25aea565182933bb506ee2>`_  2022-05-22   ``Update sample dag and doc for RDS (#23651)``
`4c9f756035 <https://github.com/apache/airflow/commit/4c9f7560355eefd57a29afee73bf04273e81a7e8>`_  2022-05-20   ``Reformat the whole AWS documentation (#23810)``
`fb3b980b5d <https://github.com/apache/airflow/commit/fb3b980b5d313b6773f45fa191766eedfea4e36d>`_  2022-05-19   ``Update the DMS Sample DAG and Docs (#23681)``
`64d0d9cd92 <https://github.com/apache/airflow/commit/64d0d9cd926bba9348adda577fc2f8b5b576c3b7>`_  2022-05-17   ``Add AWS project structure tests (re: AIP-47) (#23630)``
`ca25436108 <https://github.com/apache/airflow/commit/ca2543610872ccf62ccb085c5e0b6f9b8717c1aa>`_  2022-05-16   ``Add doc and sample dag for GCSToS3Operator (#23730)``
`1a8f1ee276 <https://github.com/apache/airflow/commit/1a8f1ee27687492a69a8000bbec99ac31e7b0d33>`_  2022-05-12   ``Update doc and sample dag for Quicksight (#23653)``
`094e50eb43 <https://github.com/apache/airflow/commit/094e50eb439e1c22e14ee78f375b7e00c71e9269>`_  2022-05-12   ``Replace "absolute()" with "resolve()" in pathlib objects (#23675)``
=================================================================================================  ===========  ===========================================================================================================

3.4.0
.....

Latest change: 2022-05-12

=================================================================================================  ===========  ========================================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ========================================================================================================
`75c60923e0 <https://github.com/apache/airflow/commit/75c60923e01375ffc5f71c4f2f7968f489e2ca2f>`_  2022-05-12   ``Prepare provider documentation 2022.05.11 (#23631)``
`3ed895bb7b <https://github.com/apache/airflow/commit/3ed895bb7b0dee45955b29e492ae408b867b6af8>`_  2022-05-11   ``Add 'RedshiftDeleteClusterOperator' support (#23563)``
`5c149c341e <https://github.com/apache/airflow/commit/5c149c341eb420fd5f8fc77534b1236ad8a9c6c1>`_  2022-05-10   ``Add Quicksight create ingestion Hook and Operator (#21863)``
`d21e49dfda <https://github.com/apache/airflow/commit/d21e49dfda3fa8432ad995c62620d89d1bb3c217>`_  2022-05-10   ``Add sample dag and doc for S3ListPrefixesOperator (#23448)``
`7354d2eac9 <https://github.com/apache/airflow/commit/7354d2eac990208d5a631c05649c482560bd827b>`_  2022-05-09   ``Add sample dag and doc for S3ListOperator (#23449)``
`389676bb0d <https://github.com/apache/airflow/commit/389676bb0dba7c1e191f61992f244a03393c6922>`_  2022-05-09   ``Add doc and sample dag for EC2 (#23547)``
`428a439953 <https://github.com/apache/airflow/commit/428a43995390b3623a51aa7bac7e21da69a8db22>`_  2022-05-09   ``Clean up in-line f-string concatenation (#23591)``
`ec4dcce338 <https://github.com/apache/airflow/commit/ec4dcce3384262d858b6951631da809f15974db7>`_  2022-05-09   ``Update sample dag and doc for Datasync (#23511)``
`5d1e6ff19a <https://github.com/apache/airflow/commit/5d1e6ff19ab4a63259a2c5aed02b601ca055a289>`_  2022-05-09   ``Add default 'aws_conn_id' to SageMaker Operators #21808 (#23515)``
`c3386c5fcf <https://github.com/apache/airflow/commit/c3386c5fcf193bc3a13621af5f10162962f4308f>`_  2022-05-09   ``Fix conn close error on retrieving log events (#23470)``
`ad863ce510 <https://github.com/apache/airflow/commit/ad863ce51020992508c83c1dcf0624623b09bca6>`_  2022-05-08   ``Amazon Sagemaker Sample DAG and docs update (#23256)``
`27a80511ec <https://github.com/apache/airflow/commit/27a80511ec3ffcf036354741bd0bfe18d4b4a471>`_  2022-05-07   ``Fix LocalFilesystemToS3Operator and S3CreateObjectOperator to support full s3:// style keys (#23180)``
`46af5baba8 <https://github.com/apache/airflow/commit/46af5baba810a07eec395e89db08fc5dab175e23>`_  2022-05-07   ``Update the Athena Sample DAG and Docs (#23428)``
`6065d1203e <https://github.com/apache/airflow/commit/6065d1203e2ce0aeb19551c545fb668978b72506>`_  2022-05-06   ``Update docs Amazon Glacier Docs (#23372)``
`7f40fa0130 <https://github.com/apache/airflow/commit/7f40fa0130c177820df0b8b06cd556913cbe08b4>`_  2022-05-04   ``Add doc and example dag for Amazon SQS Operators (#23312)``
`2d109401b3 <https://github.com/apache/airflow/commit/2d109401b3566aef613501691d18cf7e4c776cd2>`_  2022-05-04   ``Bump pre-commit hook versions (#22887)``
`d6141c6594 <https://github.com/apache/airflow/commit/d6141c6594da86653b15d67eaa99511e8fe37a26>`_  2022-05-01   ``Fix attempting to reattach in 'ECSOperator' (#23370)``
`fa0d93418b <https://github.com/apache/airflow/commit/fa0d93418b026f9822c8ffc0521381f019ef7821>`_  2022-04-30   ``Allow back script_location in Glue to be None (#23357)``
`9021c2b97d <https://github.com/apache/airflow/commit/9021c2b97d6c662f7a8b7380f74af5b0739a3f50>`_  2022-04-26   ``Fix doc build failure on main (#23240)``
`754e293c54 <https://github.com/apache/airflow/commit/754e293c546ebffc32422ff8883db57755f8518b>`_  2022-04-26   ``Add RedshiftCreateClusterOperator``
`ff85c12f4a <https://github.com/apache/airflow/commit/ff85c12f4ab42887a8710f1febaf19c5c43a962d>`_  2022-04-26   ``Fix "Chain not supported for different length Iterable"``
`444794446d <https://github.com/apache/airflow/commit/444794446d13455a57b2a87bb1a907c91afba32c>`_  2022-04-26   ``Add sample dag and doc for S3KeysUnchangedSensor``
`692a089943 <https://github.com/apache/airflow/commit/692a0899430f86d160577c3dd0f52644c4ffad37>`_  2022-04-25   ``Add doc and sample dag for S3FileTransformOperator``
`8cfb2be989 <https://github.com/apache/airflow/commit/8cfb2be98931e0f0bfb15ca411b36be3d6e66b80>`_  2022-04-25   ``Add doc and example dag for AWS Step Functions Operators``
`d91b6cd40f <https://github.com/apache/airflow/commit/d91b6cd40fbc898fd60e3de252148b56cd0e175d>`_  2022-04-23   ``'S3Hook': fix 'load_bytes' docstring (#23182)``
`8b6b0848a3 <https://github.com/apache/airflow/commit/8b6b0848a3cacf9999477d6af4d2a87463f03026>`_  2022-04-23   ``Use new Breese for building, pulling and verifying the images. (#23104)``
`9e1ac6e425 <https://github.com/apache/airflow/commit/9e1ac6e425aa52a55601bb2b5587fd97d361bfcc>`_  2022-04-20   ``Add 'S3CreateObjectOperator' (#22758)``
`ddb5d9b4a2 <https://github.com/apache/airflow/commit/ddb5d9b4a2b4e6605f66f82a6bec30393f096c05>`_  2022-04-14   ``Add doc and sample dag for S3CopyObjectOperator and S3DeleteObjectsOperator (#22959)``
`dffb0d27f2 <https://github.com/apache/airflow/commit/dffb0d27f25b9a1f2497535cb87de3b889aae9d0>`_  2022-04-12   ``Deprecate 'S3PrefixSensor' and 'S3KeySizeSensor' in favor of 'S3KeySensor' (#22737)``
`6933022e94 <https://github.com/apache/airflow/commit/6933022e94acf139b2dea9a589bb8b25c62a5d20>`_  2022-04-10   ``Fix new MyPy errors in main (#22884)``
=================================================================================================  ===========  ========================================================================================================

3.3.0
.....

Latest change: 2022-04-07

=================================================================================================  ===========  ==================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==================================================================================
`56ab82ed7a <https://github.com/apache/airflow/commit/56ab82ed7a5c179d024722ccc697b740b2b93b6a>`_  2022-04-07   ``Prepare mid-April provider documentation. (#22819)``
`de84eaf1b0 <https://github.com/apache/airflow/commit/de84eaf1b042304dd966219da22c7c529afbb662>`_  2022-04-06   ``Pass custom headers through in SES email backend (#22667)``
`e7650b85a3 <https://github.com/apache/airflow/commit/e7650b85a3d49da545e579d75ad4c01b88a3864a>`_  2022-03-31   ``Add doc and sample dag for SqlToS3Operator (#22603)``
`898d31e9c2 <https://github.com/apache/airflow/commit/898d31e9c2111553d4ef445ee146501de8c54b74>`_  2022-03-31   ``Adds HiveToDynamoDB Transfer Sample DAG and Docs (#22517)``
`616a5dd4d7 <https://github.com/apache/airflow/commit/616a5dd4d71793026076e926ed71ab87ec1c2839>`_  2022-03-31   ``Add doc and sample dag for MongoToS3Operator (#22575)``
`55ee62e28a <https://github.com/apache/airflow/commit/55ee62e28a0209349bf3e49a25565e7719324500>`_  2022-03-31   ``Add doc for LocalFilesystemToS3Operator (#22574)``
`02526b3f64 <https://github.com/apache/airflow/commit/02526b3f64d090e812ebaf3c37a23da2a3e3084e>`_  2022-03-27   ``Add doc and example dag for AWS CloudFormation Operators (#22533)``
`df473d7e89 <https://github.com/apache/airflow/commit/df473d7e8923bad5d76066cad2d32cafb1e0c4b3>`_  2022-03-27   ``GoogleApiToS3Operator: update sample dag and doc (#22507)``
`d91b8334f0 <https://github.com/apache/airflow/commit/d91b8334f038a04c1c7b2bef61e8f2baead72870>`_  2022-03-27   ``SalesforceToS3Operator: update sample dag and doc (#22489)``
`8ade99f97d <https://github.com/apache/airflow/commit/8ade99f97dddb115500ff7de71b8d4fb02bbf982>`_  2022-03-27   ``Add doc and sample dag for S3ToFTPOperator and FTPToS3Operator (#22534)``
`7ab45d41d6 <https://github.com/apache/airflow/commit/7ab45d41d6c4de322dc8afe8a74b712d0bae4ee7>`_  2022-03-24   ``Update secrets backends to use get_conn_value instead of get_conn_uri (#22348)``
=================================================================================================  ===========  ==================================================================================

3.2.0
.....

Latest change: 2022-03-22

=================================================================================================  ===========  ================================================================================================================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ================================================================================================================================================================================
`d7dbfb7e26 <https://github.com/apache/airflow/commit/d7dbfb7e26a50130d3550e781dc71a5fbcaeb3d2>`_  2022-03-22   ``Add documentation for bugfix release of Providers (#22383)``
`4de9d6622c <https://github.com/apache/airflow/commit/4de9d6622c0cb5899a286f4ec8f131b625eb1d44>`_  2022-03-22   ``ImapAttachmentToS3Operator: fix it, update sample dag and update doc (#22351)``
`e972b6a48d <https://github.com/apache/airflow/commit/e972b6a48d45581e30424a43e612d70dae55f8b7>`_  2022-03-21   ``Add docs and example dag for AWS Glue (#22295)``
`dd0cbaad3f <https://github.com/apache/airflow/commit/dd0cbaad3f91922860bebb0c4ff6e2065893e9d1>`_  2022-03-21   ``Update doc and sample dag for S3ToSFTPOperator and SFTPToS3Operator (#22313)``
`5eb6335742 <https://github.com/apache/airflow/commit/5eb63357426598f99ed50b002b72aebdf8790f73>`_  2022-03-19   ``Update sample dag and doc for S3CreateBucketOperator, S3PutBucketTaggingOperator, S3GetBucketTaggingOperator, S3DeleteBucketTaggingOperator, S3DeleteBucketOperator (#22312)``
`926f6d1894 <https://github.com/apache/airflow/commit/926f6d1894ce9d097ef2256d14a99968638da9c0>`_  2022-03-15   ``Add arguments to filter list: start_after_key, from_datetime, to_datetime, object_filter callable (#22231)``
=================================================================================================  ===========  ================================================================================================================================================================================

3.1.1
.....

Latest change: 2022-03-14

=================================================================================================  ===========  =====================================================================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =====================================================================================================================================
`16adc035b1 <https://github.com/apache/airflow/commit/16adc035b1ecdf533f44fbb3e32bea972127bb71>`_  2022-03-14   ``Add documentation for Classifier release for March 2022 (#22226)``
`46c61ed3e9 <https://github.com/apache/airflow/commit/46c61ed3e94de1484f5f8f3f2fa74e522d67ba9d>`_  2022-03-14   ``EMR Sample DAG and Docs Update (#22189)``
`c1ab8e2d7b <https://github.com/apache/airflow/commit/c1ab8e2d7b68a31408e750129592e16432474512>`_  2022-03-14   ``Protect against accidental misuse of XCom.get_value() (#22244)``
`4f6d24f865 <https://github.com/apache/airflow/commit/4f6d24f8658e0896c46e613aa656a853b358c321>`_  2022-03-14   ``use different logger to avoid duplicate log entry (#22256)``
`6f8f535619 <https://github.com/apache/airflow/commit/6f8f5356194c0dbacfaaf530cfb990f845e52fb2>`_  2022-03-14   ``Add sample dags and update doc for RedshiftClusterSensor, RedshiftPauseClusterOperator and RedshiftResumeClusterOperator (#22128)``
`a840561e1f <https://github.com/apache/airflow/commit/a840561e1f00ce026a813039e430a085d2cfe35e>`_  2022-03-13   ``AWS RDS integration fixes (#22125)``
`d08284ed25 <https://github.com/apache/airflow/commit/d08284ed251b7c5712190181623b500a38cd640d>`_  2022-03-11   `` Add map_index to XCom model and interface (#22112)``
`46a120dc5f <https://github.com/apache/airflow/commit/46a120dc5f37d0a38cbfe338af215dc63e590aff>`_  2022-03-11   ``Fix RedshiftDataOperator and update doc (#22157)``
`e63f6e36d1 <https://github.com/apache/airflow/commit/e63f6e36d14a8cd2462e80f26fb4809ab8698380>`_  2022-03-11   ``additional information in the ECSOperator around support of launch_type=EXTERNAL (#22093)``
`16ad03cae5 <https://github.com/apache/airflow/commit/16ad03cae500ff8166df0d8966d932e7329507b4>`_  2022-03-11   ``Bugfix for retrying on provision failuers(#22137)``
`bd809bc83a <https://github.com/apache/airflow/commit/bd809bc83a0727700504c3546fbb58b3421ada90>`_  2022-03-09   ``If uploading task logs to S3 fails, retry once (#21981)``
`45162565db <https://github.com/apache/airflow/commit/45162565dbfbd0322a5d07afdd4247f69395f071>`_  2022-03-08   ``EMR on EKS Sample DAG and Docs Update (#22095)``
`c7286e5306 <https://github.com/apache/airflow/commit/c7286e53064d717c97807f7ccd6cad515f88fe52>`_  2022-03-08   ``Bug-fix GCSToS3Operator (#22071)``
`184a46fc93 <https://github.com/apache/airflow/commit/184a46fc93cf78e6531f25d53aa022ee6fd66496>`_  2022-03-08   ``refactors polling logic for athena queries (#21488)``
`a150ee0bc1 <https://github.com/apache/airflow/commit/a150ee0bc124f21b99fa94adbb16e6ccfe654ae4>`_  2022-03-08   ``Add template fields to DynamoDBToS3Operator (#22080)``
`c8d49f63ca <https://github.com/apache/airflow/commit/c8d49f63ca60fa0fb447768546c2503b746a66dd>`_  2022-03-08   ``fixes query status polling logic (#21423)``
`374354dda7 <https://github.com/apache/airflow/commit/374354dda7bea02aa8c31a1d27a75c3f120f74e8>`_  2022-03-08   ``Dynamo to S3 Sample DAG and Docs (#21920)``
`c1faaf3745 <https://github.com/apache/airflow/commit/c1faaf3745dd631d4491202ed245cf8190f35697>`_  2022-03-07   ``Add sample dag and doc for RedshiftToS3Operator (#22060)``
`f5b96315fe <https://github.com/apache/airflow/commit/f5b96315fe65b99c0e2542831ff73a3406c4232d>`_  2022-03-07   ``Add documentation for Feb Providers release (#22056)``
`0209a5c3f0 <https://github.com/apache/airflow/commit/0209a5c3f07d21f70a81bc3c99c67e72cf3c2e97>`_  2022-03-07   ``Add docs and sample dags for AWS Batch (#22010)``
`01a1a263fd <https://github.com/apache/airflow/commit/01a1a263fdac53f4cd9fa82e5ae89172cf68b91c>`_  2022-03-07   ``retry on very specific eni provision failures (#22002)``
`c22fb319ff <https://github.com/apache/airflow/commit/c22fb319fffa56d0e93b60b3bd4495b60c74c4ab>`_  2022-03-07   ``Cleanup RedshiftSQLOperator documentation (#21976)``
`7724a5a2ec <https://github.com/apache/airflow/commit/7724a5a2ec9531f03497a259c4cd7823cdea5e0c>`_  2022-03-07   ``Move S3ToRedshiftOperator documentation to transfer dir (#21975)``
`80c52a1875 <https://github.com/apache/airflow/commit/80c52a18754b5ea997e1ba8d67e6d6a21d14468d>`_  2022-03-06   ``Added AWS RDS sensors (#21231)``
`f968eba470 <https://github.com/apache/airflow/commit/f968eba4700e8963ddc3bebcabf959d2f2adaadd>`_  2022-03-06   ``Added AWS RDS operators (#20907)``
`a9b7dd6900 <https://github.com/apache/airflow/commit/a9b7dd69008710f1e5b188e4f8bc2d09a5136776>`_  2022-03-06   ``Resolve mypy issue in athena example dag (#22020)``
`9ce45ff756 <https://github.com/apache/airflow/commit/9ce45ff756fa825bd363a5a00c2333d91c60c012>`_  2022-03-04   ``Rename 'S3' hook name to 'Amazon S3' (#21988)``
`af79df69b2 <https://github.com/apache/airflow/commit/af79df69b2fe4d71fa244087d5cfd031d809bc7e>`_  2022-03-02   ``Add RedshiftDataHook (#19137)``
`3f9295a4f4 <https://github.com/apache/airflow/commit/3f9295a4f4f478aaeab4f6ac44a8a9a57d428d22>`_  2022-03-02   ``Update ECS sample DAG and Docs to new standards (#21828)``
`08575ddd8a <https://github.com/apache/airflow/commit/08575ddd8a72f96a3439f73e973ee9958188eb83>`_  2022-03-01   ``Change BaseOperatorLink interface to take a ti_key, not a datetime (#21798)``
`33edeb2cb1 <https://github.com/apache/airflow/commit/33edeb2cb1c83c61f2ce5981066228d10a77df5b>`_  2022-03-01   ``Feature: Add invoke lambda function operator (#21686)``
`3218cca14b <https://github.com/apache/airflow/commit/3218cca14be4a551454dbee8b9263290cf98383c>`_  2022-03-01   ``Fix the Type Hints in ''RedshiftSQLOperator'' (#21885)``
`c819b4f8d0 <https://github.com/apache/airflow/commit/c819b4f8d0719037ce73d845c4ff9f1e4cb6cc38>`_  2022-02-28   ``Configurable AWS Session Factory (#21778)``
`5276ef8ad9 <https://github.com/apache/airflow/commit/5276ef8ad9749b2aaf4878effda513ee378f4665>`_  2022-02-28   ``Add JSON output on SqlToS3Operator (#21779)``
`cb24ee9414 <https://github.com/apache/airflow/commit/cb24ee9414afcdc1a2b0fe1ec0b9f0ba5e1bd7b7>`_  2022-02-27   ``Add SageMakerDeleteModelOperator (#21673)``
`f0b6398dd6 <https://github.com/apache/airflow/commit/f0b6398dd642dfb75c1393e8c3c88682794d152c>`_  2022-02-26   ``Bug Fix - S3DeleteObjectsOperator will try and delete all keys (#21458)``
`037865970b <https://github.com/apache/airflow/commit/037865970ba628265afd44fe2bddbc6b15996fa6>`_  2022-02-26   ``Added Hook for Amazon RDS. Added 'boto3_stub' library for autocomplete. (#20642)``
`dec05fb6b2 <https://github.com/apache/airflow/commit/dec05fb6b29f2aff454bcbc3939b3b78ba5b785f>`_  2022-02-19   ``Update EKS sample DAGs and docs (#21523)``
`b28f4c578c <https://github.com/apache/airflow/commit/b28f4c578c0b598f98731350a93ee87956d866ae>`_  2022-02-19   ``Fix Amazon SES emailer signature (#21681)``
`fc44836504 <https://github.com/apache/airflow/commit/fc44836504129664edb81c510e6deb41a7e1126d>`_  2022-02-15   ``S3KeySensor to use S3Hook url parser (#21500)``
`69bf74f9a6 <https://github.com/apache/airflow/commit/69bf74f9a6d0013ea8f9e1e7ecad9f2ac92ffd1f>`_  2022-02-15   ``Fix EcsOperatorError, so it can be loaded from a picklefile (#21441)``
`21a90c5b7e <https://github.com/apache/airflow/commit/21a90c5b7e2f236229812f9017582d67d3d7c3f0>`_  2022-02-15   ``Get log events after sleep to get all logs (#21574)``
`8e81615edb <https://github.com/apache/airflow/commit/8e81615edb7e0bc07cbba4d94fa802c44a3b4e6a>`_  2022-02-11   ``[doc] Improve s3 operator example by adding task upload_keys (#21422)``
`2c5f636e5c <https://github.com/apache/airflow/commit/2c5f636e5cfac7cc246d6ed93660bf0f8e968982>`_  2022-02-11   ``Use temporary file in GCSToS3Operator (#21295)``
`598e836043 <https://github.com/apache/airflow/commit/598e836043d3d1899087c3a78640f192f9681321>`_  2022-02-09   ``Added SNS example DAG and rst (#21475)``
`0a3ff43d41 <https://github.com/apache/airflow/commit/0a3ff43d41d33d05fb3996e61785919effa9a2fa>`_  2022-02-08   ``Add pre-commit check for docstring param types (#21398)``
=================================================================================================  ===========  =====================================================================================================================================

3.0.0
.....

Latest change: 2022-02-08

=================================================================================================  ===========  ============================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ============================================================================================
`d94fa37830 <https://github.com/apache/airflow/commit/d94fa378305957358b910cfb1fe7cb14bc793804>`_  2022-02-08   ``Fixed changelog for January 2022 (delayed) provider's release (#21439)``
`33ca0f2654 <https://github.com/apache/airflow/commit/33ca0f26544a4d280f2f56843e97deac7f33cea5>`_  2022-02-08   ``eks_hook log level fatal -> FATAL  (#21427)``
`8f81b9a01c <https://github.com/apache/airflow/commit/8f81b9a01c7708a282271f9afd6b16a91011f105>`_  2022-02-08   ``Add conditional 'template_fields_renderers' check for new SQL lexers (#21403)``
`d1150182cb <https://github.com/apache/airflow/commit/d1150182cb1f699e9877fc543322f3160ca80780>`_  2022-02-06   ``Bug fix in AWS glue operator related to num_of_dpus #19787 (#21353)``
`0a6ea572fb <https://github.com/apache/airflow/commit/0a6ea572fb5340a904e9cefaa656ac0127b15216>`_  2022-02-06   ``Fix to check if values are integer or float and convert accordingly. (#21277)``
`6c3a67d4fc <https://github.com/apache/airflow/commit/6c3a67d4fccafe4ab6cd9ec8c7bacf2677f17038>`_  2022-02-05   ``Add documentation for January 2021 providers release (#21257)``
`39e395f981 <https://github.com/apache/airflow/commit/39e395f9816c04ef2f033eb0b4f635fc3018d803>`_  2022-02-04   ``Add more SQL template fields renderers (#21237)``
`ddb5246bd1 <https://github.com/apache/airflow/commit/ddb5246bd1576e2ce6abf8c80c3328d7d71a75ce>`_  2022-02-03   ``Refactor operator links to not create ad hoc TaskInstances (#21285)``
`5185d6ab26 <https://github.com/apache/airflow/commit/5185d6ab26113b760fcf14b3dc4863b341b361d8>`_  2022-01-29   ``Alleviate import warning for 'EmrClusterLink' in deprecated AWS module (#21195)``
`7ae156fd29 <https://github.com/apache/airflow/commit/7ae156fd29b446332939eb142b68ecd80ee8b999>`_  2022-01-29   ``[SQSSensor] Add opt-in to disable auto-delete messages (#21159)``
`ff4939f23c <https://github.com/apache/airflow/commit/ff4939f23ca11e77705fb0a54fe9d3b37375ec05>`_  2022-01-25   ``Remove ':type' directives from 'SqlToS3Operator' (#21079)``
`bad070f7f4 <https://github.com/apache/airflow/commit/bad070f7f484a9b4065a0d86195a1e8002d9bfef>`_  2022-01-24   ``Create a generic operator SqlToS3Operator and deprecate the MySqlToS3Operator.  (#20807)``
`c5ffe0c2da <https://github.com/apache/airflow/commit/c5ffe0c2dac14b940b600acaccda3b387ca14a28>`_  2022-01-23   ``fix: cloudwatch logs fetch logic (#20814)``
`27b77d37a9 <https://github.com/apache/airflow/commit/27b77d37a9b2e63e95a123c31085e580fc82b16c>`_  2022-01-21   ``Fix all Amazon Provider MyPy errors (#20935)``
`73c0d241d8 <https://github.com/apache/airflow/commit/73c0d241d804507abc651a365f93d60c543349d5>`_  2022-01-21   ``Remove a few stray ':type's in docs (#21014)``
`a59a2be571 <https://github.com/apache/airflow/commit/a59a2be5710876137551658add2e0f76eff3c456>`_  2022-01-21   ``Move some base_aws logging from info to debug level (#20858)``
`602abe8394 <https://github.com/apache/airflow/commit/602abe8394fafe7de54df7e73af56de848cdf617>`_  2022-01-20   ``Remove ':type' lines now sphinx-autoapi supports typehints (#20951)``
`34efb42ebe <https://github.com/apache/airflow/commit/34efb42ebebc462b5b5eb98c70979db440e20cf2>`_  2022-01-20   ``Rename params to cloudformation_parameter in CloudFormation operators. (#20989)``
`730db3fb77 <https://github.com/apache/airflow/commit/730db3fb774f60127ab1c865e19031f1f9c193f7>`_  2022-01-18   ``Remove all "fake" stub files (#20936)``
`307d356519 <https://github.com/apache/airflow/commit/307d35651998901b064b02a0748b1c6f96ae3ac0>`_  2022-01-14   ``AWS: Adds support for optional kwargs in the EKS Operators (#20819)``
`b15027410b <https://github.com/apache/airflow/commit/b15027410b4a985c15b1d7b2b2a0eedf2173f416>`_  2022-01-14   ``Fix MyPy issues in AWS Sensors (#20863)``
`ce06e6b917 <https://github.com/apache/airflow/commit/ce06e6b917b4af9d5c16cac3646daa340798045b>`_  2022-01-13   ``AwsAthenaOperator: do not generate ''client_request_token'' if not provided (#20854)``
`f8fd0f7b4c <https://github.com/apache/airflow/commit/f8fd0f7b4ca6cb52307be4323028bf4e409566e7>`_  2022-01-13   ``Explain stub files are introduced for Mypy errors in examples (#20827)``
`341b461e4f <https://github.com/apache/airflow/commit/341b461e4fbd9ae5961ef9448c8f08e1686ee5e4>`_  2022-01-09   ``Fix mypy in providers/aws/hooks (#20353)``
`6776586904 <https://github.com/apache/airflow/commit/6776586904ab2a15727dcfc1c8d03c7a32a78444>`_  2022-01-09   ``Standardize AWS SQS classes names (#20732)``
`88e3f2ae5e <https://github.com/apache/airflow/commit/88e3f2ae5e5101928858099f9d4e7fb6542c4110>`_  2022-01-08   ``Rename amazon EMR hook name (#20767)``
`0ebd55e0f8 <https://github.com/apache/airflow/commit/0ebd55e0f8fc7eb26a2b35b779106201ffe88f55>`_  2022-01-06   ``Standardize AWS Batch naming (#20369)``
`88ea157507 <https://github.com/apache/airflow/commit/88ea1575079c0e94e1f62df38d6d592b8c827bbd>`_  2022-01-06   ``Standardize AWS Redshift naming (#20374)``
`51dc4fd808 <https://github.com/apache/airflow/commit/51dc4fd80842d1f013d4f0572cdcaa6d40c30674>`_  2022-01-06   ``Fix MyPy issues in AWS Sensors (#20717)``
`dd12cfcfe9 <https://github.com/apache/airflow/commit/dd12cfcfe9034b8c11fe9e2c3e504bae2036bade>`_  2022-01-06   ``Fix MyPy in Amazon provider for Sagemaker operator (#20715)``
`cf5c31137d <https://github.com/apache/airflow/commit/cf5c31137d43103628499a3f3690e2a716cc34b0>`_  2022-01-06   ``Fix MyPy errors for Amazon DMS in hooks and operator (#20710)``
`6af2137ef0 <https://github.com/apache/airflow/commit/6af2137ef093f39069c5f148daaf5a85c4b4b891>`_  2022-01-06   ``Fix MyPy issues in ''airflow/providers/amazon/aws/transfers'' (#20708)``
`1fc0fa5ea9 <https://github.com/apache/airflow/commit/1fc0fa5ea96913faf78a5bf5d5f75f1d2fb91e97>`_  2022-01-06   ``Standardize DynamoDB naming (#20360)``
`9c0ba1b6ab <https://github.com/apache/airflow/commit/9c0ba1b6abc593bad6fe51ed52d9c0963cd09b7c>`_  2022-01-04   ``Standardize AWS ECS naming (#20332)``
=================================================================================================  ===========  ============================================================================================

2.6.0
.....

Latest change: 2021-12-31

=================================================================================================  ===========  ==========================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==========================================================================================
`f77417eb0d <https://github.com/apache/airflow/commit/f77417eb0d3f12e4849d80645325c02a48829278>`_  2021-12-31   ``Fix K8S changelog to be PyPI-compatible (#20614)``
`97496ba2b4 <https://github.com/apache/airflow/commit/97496ba2b41063fa24393c58c5c648a0cdb5a7f8>`_  2021-12-31   ``Update documentation for provider December 2021 release (#20523)``
`83f8e178ba <https://github.com/apache/airflow/commit/83f8e178ba7a3d4ca012c831a5bfc2cade9e812d>`_  2021-12-31   ``Even more typing in operators (template_fields/ext) (#20608)``
`746ee587da <https://github.com/apache/airflow/commit/746ee587da485acdc816129fe71df23e4f024e0b>`_  2021-12-31   ``Delete pods by default in KubernetesPodOperator (#20575)``
`b164124bfe <https://github.com/apache/airflow/commit/b164124bfe15e7ef00c235daa2667e0b4ae82466>`_  2021-12-31   ``Fix mypy errors in amazon aws transfer (#20590)``
`d56e7b56bb <https://github.com/apache/airflow/commit/d56e7b56bb9827daaf8890557147fd10bdf72a7e>`_  2021-12-30   ``Fix template_fields type to have MyPy friendly Sequence type (#20571)``
`6d25d63679 <https://github.com/apache/airflow/commit/6d25d63679085279ca1672c2eee2c45d6704efaa>`_  2021-12-30   ``Fixe static checks on few other not sorted stub files (#20572)``
`a3caea0903 <https://github.com/apache/airflow/commit/a3caea09039a011440feb010e6c5ba0b26a21c4b>`_  2021-12-30   ``Fix mypy aws example dags (#20497)``
`a0821235fb <https://github.com/apache/airflow/commit/a0821235fb6877a471973295fe42283ef452abf6>`_  2021-12-30   ``Use typed Context EVERYWHERE (#20565)``
`051fac0776 <https://github.com/apache/airflow/commit/051fac0776b7e61e80653d1e91fdf2dfb1017a6f>`_  2021-12-29   ``Add support to replace S3 file on MySqlToS3Operator (#20506)``
`2ab2ae8849 <https://github.com/apache/airflow/commit/2ab2ae8849bf6d80a700b1b74cef37eb187161ad>`_  2021-12-22   ``Fix backwards compatibility issue in AWS provider's _get_credentials (#20463)``
`d557965bab <https://github.com/apache/airflow/commit/d557965bab8e03d48922ed233ed3b9551cf65cec>`_  2021-12-21   ``Organize Sagemaker classes in Amazon provider (#20370)``
`1ce71f823f <https://github.com/apache/airflow/commit/1ce71f823f1c7cfde5e8eec479f6a67b92a1555a>`_  2021-12-21   ``move emr_container hook (#20375)``
`4dd751d2fc <https://github.com/apache/airflow/commit/4dd751d2fc3f091417146079d7d1b3ac6dd86c9c>`_  2021-12-20   ``Standardize AWS Athena naming (#20305)``
`ecbe160797 <https://github.com/apache/airflow/commit/ecbe16079754d81d095290c115213a051481909a>`_  2021-12-20   ``Standardize AWS EKS naming (#20354)``
`463e3b2b25 <https://github.com/apache/airflow/commit/463e3b2b25ee5c116313160c64bb8e2644e1fef3>`_  2021-12-19   ``Fix mypy errors in aws/transfers (#20403)``
`404cd2eb69 <https://github.com/apache/airflow/commit/404cd2eb69605c3b960c7c6936cb8b342acb6f64>`_  2021-12-19   ``Fix mypy errors in aws/sensors (#20402)``
`1f4710f16a <https://github.com/apache/airflow/commit/1f4710f16af725f45cd7b90fb1311b4b80e4cc29>`_  2021-12-19   ``Fix mypy errors in providers/amazon/aws/operators (#20401)``
`1baa648e2a <https://github.com/apache/airflow/commit/1baa648e2af124032c2f2752dc0f587ee253a3a9>`_  2021-12-19   ``Standardize AWS Glue naming (#20372)``
`5769defb2a <https://github.com/apache/airflow/commit/5769defb2ac1bf7ea36a8a9ee9c26cfe515fba70>`_  2021-12-18   ``Add aws_conn_id to DynamoDBToS3Operator (#20363)``
`cef498efe9 <https://github.com/apache/airflow/commit/cef498efe9f4818e5f8e85c179ca730e84b31f4a>`_  2021-12-18   ``Standardize Amazon SES naming (#20367)``
`2a3db4d385 <https://github.com/apache/airflow/commit/2a3db4d38525df040dbee467f4d63fc2157f9084>`_  2021-12-17   ``Standardize AWS CloudFormation naming (#20357)``
`bb82cc0fbb <https://github.com/apache/airflow/commit/bb82cc0fbb7a6630eac1155d0c3b445dff13ceb6>`_  2021-12-17   ``Fix deprecation messages after splitting redshift modules (#20366)``
`ca492e1681 <https://github.com/apache/airflow/commit/ca492e16810763df0317e48658813c5b578d7edb>`_  2021-12-17   ``Standardize AWS Lambda naming (#20365)``
`f6a41a036d <https://github.com/apache/airflow/commit/f6a41a036d450020917d5862f627e606d0d36ac0>`_  2021-12-17   ``Standardize AWS Kinesis/Firehose naming (#20362)``
`3d354084ef <https://github.com/apache/airflow/commit/3d354084efb3e3c4ca15dce847701c8f6322a010>`_  2021-12-17   ``Standardize Amazon SNS naming (#20368)``
`bfcaf195a5 <https://github.com/apache/airflow/commit/bfcaf195a56242df1d78439323a013605cb100ee>`_  2021-12-16   ``Split redshift sql and cluster objects (#20276)``
`a3870611a7 <https://github.com/apache/airflow/commit/a3870611a72deaface2729efe8eb809468c2a053>`_  2021-12-16   ``Organize EMR classes in Amazon provider (#20160)``
`206cce971d <https://github.com/apache/airflow/commit/206cce971da6941e8c1b0d3c4dbf4fa8afe0fba4>`_  2021-12-16   ``ECSOperator: fix KeyError on missing exitCode (#20264)``
`38fd65dcfe <https://github.com/apache/airflow/commit/38fd65dcfe85149170d6be640c23018e0065cf7c>`_  2021-12-16   ``Rename DataSync Hook and Operator (#20328)``
`2fb5e1d0ec <https://github.com/apache/airflow/commit/2fb5e1d0ec306839a3ff21d0bddbde1d022ee8c7>`_  2021-12-15   ``Fix cached_property MyPy declaration and related MyPy errors (#20226)``
`5712e2b587 <https://github.com/apache/airflow/commit/5712e2b58741d4332350c012be412881b04a61c2>`_  2021-12-15   ``Deprecate passing execution_date to XCom methods (#19825)``
`60b72dd8f5 <https://github.com/apache/airflow/commit/60b72dd8f5161f0e00f29a9de54522682f7cd5f6>`_  2021-12-14   ``Organize Dms classes in Amazon provider (#20156)``
`e77c05fa1b <https://github.com/apache/airflow/commit/e77c05fa1b29908adebc36280249b77b50b3dda2>`_  2021-12-13   ``Add RedshiftResumeClusterOperator and RedshiftPauseClusterOperator (#19665)``
`a208463802 <https://github.com/apache/airflow/commit/a2084638020613979fa1ed9ba944050f274bb160>`_  2021-12-13   ``Organize S3 Classes in Amazon Provider (#20167)``
`1d2f2d3dfe <https://github.com/apache/airflow/commit/1d2f2d3dfec950d278d63277e4410c8294af5142>`_  2021-12-13   ``Organize Step Function classes in Amazon provider (#20158)``
`985bb06ba5 <https://github.com/apache/airflow/commit/985bb06ba57ab67bb218ca9ca7549a81bea88f87>`_  2021-12-09   ``Organize EC2 classes in Amazon provider (#20157)``
`0e2a0ccd30 <https://github.com/apache/airflow/commit/0e2a0ccd3087f53222e7859f414daf0ffa50dfbb>`_  2021-12-08   ``Added function in AWSAthenaHook to get s3 output query results file URI  (#20124)``
`6e15e3a65e <https://github.com/apache/airflow/commit/6e15e3a65ec15d9b52abceed36da9f8cccee72d9>`_  2021-12-06   ``Bug fix in AWS glue operator when specifying the WorkerType & NumberOfWorkers (#19787)``
`af28b41903 <https://github.com/apache/airflow/commit/af28b4190316401c9dfec6108d22b0525974eadb>`_  2021-12-05   ``Add sensor for AWS Batch (#19850) (#19885)``
`480c333c45 <https://github.com/apache/airflow/commit/480c333c45d31cdfdc63cdfceecd4ad8529eefd4>`_  2021-12-03   ``Add state details to EMR container failure reason (#19579)``
`2539cb44b4 <https://github.com/apache/airflow/commit/2539cb44b47d78e81a88fde51087f4cc77c924c5>`_  2021-12-01   ``Move to watchtower 2.0.1 (#19907)``
=================================================================================================  ===========  ==========================================================================================

2.5.0
.....

Latest change: 2021-11-30

=================================================================================================  ===========  =====================================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =====================================================================================================
`853576d901 <https://github.com/apache/airflow/commit/853576d9019d2aca8de1d9c587c883dcbe95b46a>`_  2021-11-30   ``Update documentation for November 2021 provider's release (#19882)``
`83b51e5306 <https://github.com/apache/airflow/commit/83b51e53062dc596a630edd4bd01407a556f1aa6>`_  2021-11-26   ``Amazon provider remove deprecation, second try (#19815)``
`d58df468c8 <https://github.com/apache/airflow/commit/d58df468c8d77c5d45e80f2333eb074bb7771a95>`_  2021-11-24   ``Revert "Adjust built-in base_aws methods to avoid Deprecation warnings (#19725)" (#19791)``
`4be04143a5 <https://github.com/apache/airflow/commit/4be04143a5f7e246127e942bf1d73abcd22ce189>`_  2021-11-24   ``Adjust built-in base_aws methods to avoid Deprecation warnings (#19725)``
`fe682ec3d3 <https://github.com/apache/airflow/commit/fe682ec3d376f0983410d64beb4f3529fb7b0f99>`_  2021-11-24   ``Fix duplicate changelog entries (#19759)``
`186513e24e <https://github.com/apache/airflow/commit/186513e24e723b79bc57e3ca0ade3c73e4fa2f9a>`_  2021-11-15   ``Catch AccessDeniedException in AWS Secrets Manager Backend (#19324)``
`4c495ca94e <https://github.com/apache/airflow/commit/4c495ca94e67a03a31832fea77a29220b6c13673>`_  2021-11-15   ``Cleanup of start_date and default arg use for Amazon example DAGs (#19237)``
`eb16fd51e0 <https://github.com/apache/airflow/commit/eb16fd51e0a6b30fe6fd87cc7316d283528f4ac9>`_  2021-11-12   ``Adding support for using ''client_type'' API for interacting with EC2 and support filters (#9011)``
`aa2cb5545f <https://github.com/apache/airflow/commit/aa2cb5545f09d694b9143b323efcd4f6b6c66e60>`_  2021-11-12   ``Remove remaining 'pylint: disable' comments (#19541)``
`9053de7718 <https://github.com/apache/airflow/commit/9053de771873561aaf21a930056666e043b14b9c>`_  2021-11-10   ``Do not check for S3 key before attempting download (#19504)``
`ebb7394005 <https://github.com/apache/airflow/commit/ebb739400598962343e0f5059d96f15a9bfa9c9f>`_  2021-11-08   ``MySQLToS3Operator  actually allow writing parquet files to s3. (#19094)``
=================================================================================================  ===========  =====================================================================================================

2.4.0
.....

Latest change: 2021-11-04

=================================================================================================  ===========  ========================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ========================================================================================
`5a113f3027 <https://github.com/apache/airflow/commit/5a113f302769f0ecad3a54bad3027d459cb276a4>`_  2021-11-04   ``Add documentation for RC2 release of Amazon provider for October (#19413)``
`96dd70348a <https://github.com/apache/airflow/commit/96dd70348ad7e31cfeae6d21af70671b41551fe9>`_  2021-11-04   `` fix SagemakerProcessingOperator ThrottlingException (#19195)``
`a3266ba2b9 <https://github.com/apache/airflow/commit/a3266ba2b9c0d6d8b669178f3ce8752bef92924f>`_  2021-11-03   ``Doc: Fix typos in variable and comments (#19349)``
`6148ddd365 <https://github.com/apache/airflow/commit/6148ddd365939bb5129b342900a576bd855e9fc4>`_  2021-11-03   ``Fix S3ToRedshiftOperator (#19358)``
`1600f1dfd7 <https://github.com/apache/airflow/commit/1600f1dfd767ebb0097e1455348dc13ea68e9bb5>`_  2021-10-30   ``Remove duplicated entries in changelog (#19331)``
`d9567eb106 <https://github.com/apache/airflow/commit/d9567eb106929b21329c01171fd398fbef2dc6c6>`_  2021-10-29   ``Prepare documentation for October Provider's release (#19321)``
`1543dc28f4 <https://github.com/apache/airflow/commit/1543dc28f4a2f1631dfaedd948e646a181ccf7ee>`_  2021-10-29   ``Fixing ses email backend (#18042)``
`3c08c025c5 <https://github.com/apache/airflow/commit/3c08c025c5445ffc0533ac28d07ccf2e69a19ca8>`_  2021-10-27   ``Move validation of templated input params to run after the context init (#19048)``
`f5ad26dcdd <https://github.com/apache/airflow/commit/f5ad26dcdd7bcb724992528dce71056965b94d26>`_  2021-10-21   ``Fixup string concatenations (#19099)``
`86a2a19ad2 <https://github.com/apache/airflow/commit/86a2a19ad2bdc87a9ad14bb7fde9313b2d7489bb>`_  2021-10-17   ``More f-strings (#18855)``
`258451cfba <https://github.com/apache/airflow/commit/258451cfba12959fabe729e70509a478ada7e72c>`_  2021-10-15   ``MySQLToS3Operator add support for parquet format (#18755)``
`176165de3b <https://github.com/apache/airflow/commit/176165de3b297c0ed7d2b60cf6b4c37fc7a2337f>`_  2021-10-11   ``Update S3PrefixSensor to support checking multiple prefixes within a bucket (#18807)``
`1d7cfdbcd9 <https://github.com/apache/airflow/commit/1d7cfdbcd91705b2f88ef4ece503b7a072767e02>`_  2021-10-10   ``Remove extra postgres dependency from AWS Provider (#18844)``
`1df9a512c2 <https://github.com/apache/airflow/commit/1df9a512c284f2585ce01bb77ac91d550f6ccaa3>`_  2021-10-08   ``Add RedshiftSQLHook, RedshiftSQLOperator (#18447)``
`22768ff61e <https://github.com/apache/airflow/commit/22768ff61e1b8d3294c30562fa2ee284ee0f7739>`_  2021-10-08   ``Removed duplicated code on S3ToRedshiftOperator (#18671)``
=================================================================================================  ===========  ========================================================================================

2.3.0
.....

Latest change: 2021-10-08

=================================================================================================  ===========  =========================================================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =========================================================================================================================
`a680e876b6 <https://github.com/apache/airflow/commit/a680e876b680ecd05f7ac8da6c5a8f2518b9e071>`_  2021-10-08   ``Prepare documentation for RC2 Amazon Provider release for September (#18830)``
`ea8f478909 <https://github.com/apache/airflow/commit/ea8f47890908e3a132273ec96f753ea6af66f837>`_  2021-10-08   ``Add AWS Fargate profile support (#18645)``
`9344c34522 <https://github.com/apache/airflow/commit/9344c345220fc9c3355596f96132051c96b03ac6>`_  2021-10-08   ``Enable AWS Secrets Manager backend to retrieve conns using different fields (#18764)``
`1a35644b5d <https://github.com/apache/airflow/commit/1a35644b5dbf2520b95f3d2320902f6e46407b9a>`_  2021-10-08   ``Add emr cluster link (#18691)``
`e0af0b976c <https://github.com/apache/airflow/commit/e0af0b976c0cc43d2b1aa204d047fe755e4c5be7>`_  2021-10-08   ``AwsGlueJobOperator: add wait_for_completion to Glue job run (#18814)``
`8e56ed234b <https://github.com/apache/airflow/commit/8e56ed234bf48775d553744c792fadc3ad63fbf7>`_  2021-10-07   ``Enable FTPToS3Operator to transfer several files (#17937)``
`28284a37b3 <https://github.com/apache/airflow/commit/28284a37b3fc7a4581c552f4f3a8d72fa8b0a21d>`_  2021-10-07   ``Amazon Athena Example (#18785)``
`38c6cf9c27 <https://github.com/apache/airflow/commit/38c6cf9c27e7f2bbcc9f39f56c8e3d1405b78087>`_  2021-10-07   ``AwsGlueJobOperator: add run_job_kwargs to Glue job run (#16796)``
`72e49ad3a3 <https://github.com/apache/airflow/commit/72e49ad3a3402b506e878ade74dba588c1fc7f71>`_  2021-10-06   ``Amazon SQS Example (#18760)``
`767a4f5207 <https://github.com/apache/airflow/commit/767a4f5207f8fc6c3d8072fa780d84460d41fc7a>`_  2021-10-05   ``ECSOperator: airflow exception on edge case when cloudwatch log stream is not found (#18733)``
`760bf6e51c <https://github.com/apache/airflow/commit/760bf6e51c5b61412243028d4f10cdb14830878a>`_  2021-10-06   ``Adds an s3 list prefixes operator (#17145)``
`12763f125a <https://github.com/apache/airflow/commit/12763f125ab90a1f9e769f1e51dc34cd0dd6d51a>`_  2021-10-05   ``Add additional dependency for postgres extra for amazon provider (#18737)``
`86bf2a29ba <https://github.com/apache/airflow/commit/86bf2a29ba784b25c335408eb4647ad2eb48b525>`_  2021-10-04   ``Simplify strings previously split across lines (#18679)``
`12133861ec <https://github.com/apache/airflow/commit/12133861ecefd28f1d569cf2d190c2f26f6fd2fb>`_  2021-10-01   ``Support all Unix wildcards in S3KeySensor (#18211)``
`840ea3efb9 <https://github.com/apache/airflow/commit/840ea3efb9533837e9f36b75fa527a0fbafeb23a>`_  2021-09-30   ``Update documentation for September providers release (#18613)``
`8a1437e55e <https://github.com/apache/airflow/commit/8a1437e55ed50bcb9301c55c1217e9e66532f6ed>`_  2021-09-29   ``Refresh credentials for long-running pods on EKS (#17951)``
`2fbd23878f <https://github.com/apache/airflow/commit/2fbd23878f185372c3e46221c3c95918d6cbcffa>`_  2021-09-28   ``Update s3_list.py (#18561)``
`a458fcc573 <https://github.com/apache/airflow/commit/a458fcc573845ff65244a2dafd204ed70129f3e8>`_  2021-09-27   ``Updating miscellaneous provider DAGs to use TaskFlow API where applicable (#18278)``
`e25eea052f <https://github.com/apache/airflow/commit/e25eea052fd54c94b490a377de05c6bae4c24dbb>`_  2021-09-19   ``Inclusive Language (#18349)``
`4cd190c9bc <https://github.com/apache/airflow/commit/4cd190c9bcbe4229de3c8527d0e3480dea3be42f>`_  2021-09-18   ``ECSOperator realtime logging (#17626)``
`27088c4533 <https://github.com/apache/airflow/commit/27088c4533199a19e6f810abc4e565bc8e107cf0>`_  2021-09-18   ``Add IAM Role Credentials to S3ToRedshiftTransfer and RedshiftToS3Transfer (#18156)``
`2d4f3cb644 <https://github.com/apache/airflow/commit/2d4f3cb644200190619e76d3e2a140f340d908b9>`_  2021-09-15   ``Adding missing 'replace' param in docstring (#18241)``
`81ebd78db4 <https://github.com/apache/airflow/commit/81ebd78db48a4876377dc20d361a7938be11373a>`_  2021-09-12   ``Added upsert method on S3ToRedshift operator (#18027)``
`02397761af <https://github.com/apache/airflow/commit/02397761af7ed77b0e7c4f4d8de34d8a861c5b40>`_  2021-09-12   ``Deprecate default pod name in EKSPodOperator (#18036)``
`e6cb2f7beb <https://github.com/apache/airflow/commit/e6cb2f7beb4c6ea4ad4a965f9c0f2b8f6978129c>`_  2021-09-10   ``ECSOperator returns last logs when ECS task fails (#17209)``
`b8795501ea <https://github.com/apache/airflow/commit/b8795501ea6db64d83528bdd6b517779cfc9deec>`_  2021-09-10   ``Aws secrets manager backend (#17448)``
`6e101317a2 <https://github.com/apache/airflow/commit/6e101317a22bb58a9edf512bbda662c862e53c78>`_  2021-09-10   ``Simplify s3 ''unify_bucket_name_and_key'' (#17325)``
`3fe948a860 <https://github.com/apache/airflow/commit/3fe948a860a6eed2ee51a6f1be658a3ba260683f>`_  2021-09-08   ``sftp_to_s3 stream file option (#17609)``
`867e9305f0 <https://github.com/apache/airflow/commit/867e9305f08bf9580f25430d8b6e84071c59f9e6>`_  2021-09-03   ``AwsBaseHook make 'client_type' & 'resource_type' optional params for 'get_client_type' & 'get_resource_type' (#17987)``
`bcd81f23af <https://github.com/apache/airflow/commit/bcd81f23af13cde8231df205127e08048b67820c>`_  2021-09-03   ``Add Spark to the EMR cluster for the job flow examples (#17563)``
`7c4d3173f2 <https://github.com/apache/airflow/commit/7c4d3173f2c73ceeadb7155cf367983a90c6dca8>`_  2021-09-02   ``Delete unnecessary parameters in EKSPodOperator (#17960)``
=================================================================================================  ===========  =========================================================================================================================

2.2.0
.....

Latest change: 2021-08-30

=================================================================================================  ===========  =========================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =========================================================================================
`0a68588479 <https://github.com/apache/airflow/commit/0a68588479e34cf175d744ea77b283d9d78ea71a>`_  2021-08-30   ``Add August 2021 Provider's documentation (#17890)``
`653c13ec0c <https://github.com/apache/airflow/commit/653c13ec0cf4729f991adf8c7b37b3a272caac56>`_  2021-08-30   ``Fix broken XCOM in EKSPodOperator (#17918)``
`42e13e1a5a <https://github.com/apache/airflow/commit/42e13e1a5a4c97a2085ddf96f7d93e7bf71949b8>`_  2021-08-30   ``Remove all deprecation warnings in providers (#17900)``
`890bd4310e <https://github.com/apache/airflow/commit/890bd4310e12a0a4fadfaec1f9b36d2aaae6119e>`_  2021-08-28   ``Add an Amazon EMR on EKS provider package (#16766)``
`41632e03b8 <https://github.com/apache/airflow/commit/41632e03b8caf71de308414c48e9cb211a083761>`_  2021-08-27   ``Fix provider.yaml errors due to exit(0) in test (#17858)``
`be75dcd39c <https://github.com/apache/airflow/commit/be75dcd39cd10264048c86e74110365bd5daf8b7>`_  2021-08-23   ``Update description about the new ''connection-types'' provider meta-data``
`76ed2a49c6 <https://github.com/apache/airflow/commit/76ed2a49c6cd285bf59706cf04f39a7444c382c9>`_  2021-08-19   ``Import Hooks lazily individually in providers manager (#17682)``
`bee48f31f1 <https://github.com/apache/airflow/commit/bee48f31f197f0ef7c2fb3b37f3d3d472ea388f4>`_  2021-08-19   ``Implemented Basic EKS Integration (#16571)``
`d26b862330 <https://github.com/apache/airflow/commit/d26b862330628b54953a91436d76163e1d31b713>`_  2021-08-19   ``Add optional SQL parameters in ''RedshiftToS3Operator'' (#17640)``
`29aab6434f <https://github.com/apache/airflow/commit/29aab6434ffe0fb8c83b6fd6c9e44310966d496a>`_  2021-08-17   ``Adds secrets backend/logging/auth information to provider yaml (#17625)``
`1632c9f519 <https://github.com/apache/airflow/commit/1632c9f519510ff218656bbc1554c80cb158e85a>`_  2021-08-14   ``Add new LocalFilesystemToS3Operator under Amazon provider (#17168) (#17382)``
`e7eeaa6086 <https://github.com/apache/airflow/commit/e7eeaa60861f523a03a64de9dae1784cfa8b21a4>`_  2021-08-12   ``Doc: Fix docstrings for ''MongoToS3Operator'' (#17588)``
`987575787d <https://github.com/apache/airflow/commit/987575787d82abf5b4e68b669fdb3bcab08965e6>`_  2021-08-12   ``Add Mongo projections to hook and transfer (#17379)``
`77c4325fb0 <https://github.com/apache/airflow/commit/77c4325fb064d7e8b5e1088b2675a0a419d56b03>`_  2021-08-10   ``Fixing ParamValidationError when executing load_file in Glue hooks/operators (#16012)``
`d28efbfb77 <https://github.com/apache/airflow/commit/d28efbfb7780afd1ff13a258dc5dc3e3381ddabd>`_  2021-08-02   ``Improve AWS SQS Sensor (#16880) (#16904)``
`71088986f1 <https://github.com/apache/airflow/commit/71088986f12be3806d48e7abc722c3f338f01301>`_  2021-08-02   ``make platform version as independent parameter of ECSOperator (#17281)``
`80fc80ace6 <https://github.com/apache/airflow/commit/80fc80ace69982882dd0ac5c70eeedc714658941>`_  2021-08-02   ``Fixes #16972 - Slugify role session name in AWS base hook (#17210)``
=================================================================================================  ===========  =========================================================================================

2.1.0
.....

Latest change: 2021-07-26

=================================================================================================  ===========  =================================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =================================================================================================
`87f408b1e7 <https://github.com/apache/airflow/commit/87f408b1e78968580c760acb275ae5bb042161db>`_  2021-07-26   ``Prepares docs for Rc2 release of July providers (#17116)``
`569659db38 <https://github.com/apache/airflow/commit/569659db38de8d708200b909e0cb410abc8772fa>`_  2021-07-26   ``Updating Amazon-AWS example DAGs to use XComArgs (#16868)``
`763919d415 <https://github.com/apache/airflow/commit/763919d4152ffa13433e2489fec85ed286b7b196>`_  2021-07-25   ``Adding custom Salesforce connection type + SalesforceToS3Operator updates (#17162)``
`8b100fcb42 <https://github.com/apache/airflow/commit/8b100fcb427dc8e6f511e6ce2deddb2e04909291>`_  2021-07-21   ``ECSOperator / pass context to self.xcom_pull as it was missing (when using reattach) (#17141)``
`f44d7bd9cf <https://github.com/apache/airflow/commit/f44d7bd9cfe00b1409db78c2a644516b0ab003e9>`_  2021-07-21   ``Made S3ToRedshiftOperator transaction safe (#17117)``
`32582b5bf1 <https://github.com/apache/airflow/commit/32582b5bf1432e7c7603b959a675cf7edd76c9e6>`_  2021-07-21   ``Adding SalesforceToS3Operator to Amazon Provider (#17094)``
`bb1d79cb81 <https://github.com/apache/airflow/commit/bb1d79cb81c5a5a80f97ab4fecfa7db7a52c7b4b>`_  2021-07-19   ``Fixed template_fields_renderers for Amazon provider (#17087)``
`8f77a54b53 <https://github.com/apache/airflow/commit/8f77a54b53664c3b6dbcf2c955975ad121463550>`_  2021-07-19   ``removing try-catch block (#17081)``
`cda78333b4 <https://github.com/apache/airflow/commit/cda78333b4ce9304abe315ab1afe41efe17fd2da>`_  2021-07-18   ``Added docs & doc ref's for AWS transfer operators between SFTP & S3 (#16964)``
`d02ded65ea <https://github.com/apache/airflow/commit/d02ded65eaa7d2281e249b3fa028605d1b4c52fb>`_  2021-07-15   ``Fixed wrongly escaped characters in amazon's changelog (#17020)``
`b916b75079 <https://github.com/apache/airflow/commit/b916b7507921129dc48d6add1bdc4b923b60c9b9>`_  2021-07-15   ``Prepare documentation for July release of providers. (#17015)``
`fc0250f1d5 <https://github.com/apache/airflow/commit/fc0250f1d5c43784f353dbdf4a34089aa96c28e5>`_  2021-07-15   ``Allow attaching to previously launched task in ECSOperator (#16685)``
`d3f300fba8 <https://github.com/apache/airflow/commit/d3f300fba8c252cac79a1654fddb91532f44c656>`_  2021-07-11   ``Fix wrong template_fields_renderers for AWS operators (#16820)``
`f0df184e4d <https://github.com/apache/airflow/commit/f0df184e4db940f7e1b9248b5f5843d494034112>`_  2021-07-07   ``Update AWS Base hook to use refreshable credentials (#16770) (#16771)``
`ffe8fab653 <https://github.com/apache/airflow/commit/ffe8fab6536ac4eec076d48548d7b2e814a55b1f>`_  2021-07-03   ``Added select_query to the templated fields in RedshiftToS3Operator (#16767)``
`866a601b76 <https://github.com/apache/airflow/commit/866a601b76e219b3c043e1dbbc8fb22300866351>`_  2021-06-28   ``Removes pylint from our toolchain (#16682)``
`0d80383bdd <https://github.com/apache/airflow/commit/0d80383bdd506c2eff8ef29d0ff461620a966f86>`_  2021-06-28   ``AWS Hook - allow IDP HTTP retry (#12639) (#16612)``
`3a57d9fc60 <https://github.com/apache/airflow/commit/3a57d9fc604f4478fd51e20287494d0d7a33f0e4>`_  2021-06-26   ``Bump ''sphinxcontrib-spelling'' and minor improvements (#16675)``
`2543c74c19 <https://github.com/apache/airflow/commit/2543c74c1927b751e7492df81d762e61d2a4d5f6>`_  2021-06-24   ``AWS DataSync cancel task on exception (#11011) (#16589)``
`2ab2cbf93d <https://github.com/apache/airflow/commit/2ab2cbf93df9eddfb527fcfd9d7b442678a57662>`_  2021-06-23   ``Update Boto3 API calls in ECSOperator (#16050)``
=================================================================================================  ===========  =================================================================================================

2.0.0
.....

Latest change: 2021-06-21

=================================================================================================  ===========  ===============================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ===============================================================================================
`19ed074e9c <https://github.com/apache/airflow/commit/19ed074e9c696eb4aff25f3a833e7f359f2b1c38>`_  2021-06-21   ``Use safe get with AWS DMS describe replication tasks (#16540)``
`bbc627a3da <https://github.com/apache/airflow/commit/bbc627a3dab17ba4cf920dd1a26dbed6f5cebfd1>`_  2021-06-18   ``Prepares documentation for rc2 release of Providers (#16501)``
`db10c6841b <https://github.com/apache/airflow/commit/db10c6841b6295ab2b116c03025856084885c6a3>`_  2021-06-18   ``Add AWS DMS replication task operators (#15850)``
`1c82b4d015 <https://github.com/apache/airflow/commit/1c82b4d015a1785a881bb916ffa0265249c2cde7>`_  2021-06-17   ``Fix S3ToFTPOperator (#13796)``
`36dc6a8100 <https://github.com/apache/airflow/commit/36dc6a8100c0261270f7f6fa20928508f90bac96>`_  2021-06-16   ``Make job name check optional in SageMakerTrainingOperator (#16327)``
`cbf8001d76 <https://github.com/apache/airflow/commit/cbf8001d7630530773f623a786f9eb319783b33c>`_  2021-06-16   ``Synchronizes updated changelog after buggfix release (#16464)``
`1fba5402bb <https://github.com/apache/airflow/commit/1fba5402bb14b3ffa6429fdc683121935f88472f>`_  2021-06-15   ``More documentation update for June providers release (#16405)``
`9cd7930c34 <https://github.com/apache/airflow/commit/9cd7930c34c2842bef0cdc1748d42e7caa722301>`_  2021-06-13   ``Update copy command for s3 to redshift (#16241)``
`643f3c35a6 <https://github.com/apache/airflow/commit/643f3c35a6ba3def40de7db8e974c72e98cfad44>`_  2021-06-13   ``fix: AwsGlueJobOperator change order of args for load_file (#16216)``
`30708b5b25 <https://github.com/apache/airflow/commit/30708b5b254960395d8061e8c403294b93900c4d>`_  2021-06-13   ``Add support of capacity provider strategy for ECSOperator (#15848)``
`9c94b72d44 <https://github.com/apache/airflow/commit/9c94b72d440b18a9e42123d20d48b951712038f9>`_  2021-06-07   ``Updated documentation for June 2021 provider release (#16294)``
`9fcdf3d4de <https://github.com/apache/airflow/commit/9fcdf3d4deae0dd77c734f5a3520fb75d66d0821>`_  2021-06-02   ``Fix S3 Select payload join (#16189)``
`8d16638285 <https://github.com/apache/airflow/commit/8d16638285687fd0ef41d40340ab1c5bcffd507a>`_  2021-05-29   ``remove retry for now (#16150)``
`5fbc86f036 <https://github.com/apache/airflow/commit/5fbc86f0364fbc4f7c2ba10cae4a13449709730c>`_  2021-05-27   ``Remove the 'not-allow-trailing-slash' rule on S3_hook (#15609)``
`904709d34f <https://github.com/apache/airflow/commit/904709d34fbe0b6062d72932b72954afe13ec148>`_  2021-05-27   ``Check synctatic correctness for code-snippets (#16005)``
`476d0f6e3d <https://github.com/apache/airflow/commit/476d0f6e3d2059f56532cda36cdc51aa86bafb37>`_  2021-05-22   ``Bump pyupgrade v2.13.0 to v2.18.1 (#15991)``
`821ea6fc18 <https://github.com/apache/airflow/commit/821ea6fc187a9780b8fe0dd76f140367681ba065>`_  2021-05-17   ``Fix spacing in ''AwsBatchWaitersHook'' docstring (#15839)``
`1467046058 <https://github.com/apache/airflow/commit/1467046058c40f6d448a24b6cda43c2037180c41>`_  2021-05-14   ``CloudwatchTaskHandler reads timestamp from Cloudwatch events (#15173)``
`dab10d9fae <https://github.com/apache/airflow/commit/dab10d9fae6bfca0f9c0c504b77773d94ccee86d>`_  2021-05-10   ``MongoToS3Operator failed when running with a single query (not aggregate pipeline) (#15680)``
`9c8391a13f <https://github.com/apache/airflow/commit/9c8391a13f6ba29749675cf23f2f874f96b0cc8c>`_  2021-05-10   ``Fix spelling (#15699)``
`37681bca00 <https://github.com/apache/airflow/commit/37681bca0081dd228ac4047c17631867bba7a66f>`_  2021-05-07   ``Auto-apply apply_default decorator (#15667)``
`9953a047c4 <https://github.com/apache/airflow/commit/9953a047c4b0471ceb6effc669dce8d03c2f935b>`_  2021-05-07   ``Add Connection Documentation for the Hive Provider (#15704)``
`0f97a3970d <https://github.com/apache/airflow/commit/0f97a3970d2c652beedbf2fbaa33e2b2bfd69bce>`_  2021-05-04   ``Rename example bucket names to use INVALID BUCKET NAME by default (#15651)``
`db557a8c4a <https://github.com/apache/airflow/commit/db557a8c4a3e1f0d67b2534010e5092be4f4a9fd>`_  2021-05-01   ``Docs: Replace 'airflow' to 'apache-airflow' to install extra (#15628)``
=================================================================================================  ===========  ===============================================================================================

1.4.0
.....

Latest change: 2021-05-01

=================================================================================================  ===========  ==========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==========================================================================
`807ad32ce5 <https://github.com/apache/airflow/commit/807ad32ce59e001cb3532d98a05fa7d0d7fabb95>`_  2021-05-01   ``Prepares provider release after PIP 21 compatibility (#15576)``
`814e471d13 <https://github.com/apache/airflow/commit/814e471d137aad68bd64a21d20736e7b88403f97>`_  2021-04-29   ``Update pre-commit checks (#15583)``
`bf2b48174a <https://github.com/apache/airflow/commit/bf2b48174a1ccfe398eefba7f04a5cacac421266>`_  2021-04-27   ``Add Connection Documentation for Providers (#15499)``
`4b031d39e1 <https://github.com/apache/airflow/commit/4b031d39e12110f337151cda6693e2541bf71c2c>`_  2021-04-27   ``Make Airflow code Pylint 2.8 compatible (#15534)``
`657384615f <https://github.com/apache/airflow/commit/657384615fafc060f9e2ed925017306705770355>`_  2021-04-27   ``Fix 'logging.exception' redundancy (#14823)``
`d598630cd7 <https://github.com/apache/airflow/commit/d598630cd7a2e80fd84c499cfcba37ad2b6888f3>`_  2021-04-26   ``Fix AthenaSensor calling AthenaHook incorrectly (#15427)``
`71c673e427 <https://github.com/apache/airflow/commit/71c673e427a89cae2a9f3174c32c5c85556d6342>`_  2021-04-22   ``Update Docstrings of Modules with Missing Params (#15391)``
`a82de56b9d <https://github.com/apache/airflow/commit/a82de56b9d16713cbba4dee42d6797cc12b59cfe>`_  2021-04-13   ``S3Hook.load_file should accept Path object in addition to str (#15232)``
`b4770725a3 <https://github.com/apache/airflow/commit/b4770725a3aa03bd50a0a8c8e01db667bff93862>`_  2021-04-12   ``Add links to new modules for deprecated modules (#15316)``
`da780fc8b1 <https://github.com/apache/airflow/commit/da780fc8b1f041b927f1ed06f737113010d58a61>`_  2021-04-12   ``Fixes doc for SQSSensor (#15323)``
=================================================================================================  ===========  ==========================================================================

1.3.0
.....

Latest change: 2021-04-06

=================================================================================================  ===========  =============================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =============================================================================
`042be2e4e0 <https://github.com/apache/airflow/commit/042be2e4e06b988f5ba2dc146f53774dabc8b76b>`_  2021-04-06   ``Updated documentation for provider packages before April release (#15236)``
`266384a63f <https://github.com/apache/airflow/commit/266384a63f4693b667f308d49fcbed9a10a41fce>`_  2021-04-05   ``Fix string concatenation using 'f-strings' (#15200)``
`eda538f56c <https://github.com/apache/airflow/commit/eda538f56cb2dc2728d303acddb42841fe419c36>`_  2021-04-02   ``AWS: Do not log info when SSM & SecretsManager secret not found (#15120)``
`9b76b94c94 <https://github.com/apache/airflow/commit/9b76b94c940d472290861930a1d5860b43b3b2b2>`_  2021-04-02   ``A bunch of template_fields_renderers additions (#15130)``
`6822665102 <https://github.com/apache/airflow/commit/6822665102c973d6e4d5892564294489ca094580>`_  2021-04-01   ``Send region_name into parant class of AwsGlueJobHook (#14251)``
`5379698892 <https://github.com/apache/airflow/commit/53796988929d7b5de98cd322fdea9e0a8edec0a1>`_  2021-03-27   ``Improve docstrings for various modules (#15047)``
`614be87b23 <https://github.com/apache/airflow/commit/614be87b23199acd67e69677cfdb6ae4ed023b69>`_  2021-03-26   ``Added retry to ECS Operator (#14263)``
`a7ca1b3b0b <https://github.com/apache/airflow/commit/a7ca1b3b0bdf0b7677e53be1b11e833714dfbbb4>`_  2021-03-26   ``Fix Sphinx Issues with Docstrings (#14968)``
`0f327788b5 <https://github.com/apache/airflow/commit/0f327788b5b0887c463cb83dd8f732245da96577>`_  2021-03-24   ``doc: Fix typo in 'secrets_manager.py' docstring (#14943)``
`93982e3bb6 <https://github.com/apache/airflow/commit/93982e3bb6386e3db02bd5e4a8e61f74bdc12d94>`_  2021-03-22   ``Make script_args templated in AwsGlueJobOperator (#14925)``
`e172bd0e16 <https://github.com/apache/airflow/commit/e172bd0e16d5b13105734fe9eb8effc44d593c29>`_  2021-03-22   ``Update docstrings to adhere to sphinx standards (#14918)``
`68e4c4dcb0 <https://github.com/apache/airflow/commit/68e4c4dcb0416eb51a7011a3bb040f1e23d7bba8>`_  2021-03-20   ``Remove Backport Providers (#14886)``
`a3778127ed <https://github.com/apache/airflow/commit/a3778127ed98c131e1465125de672f202961b3c5>`_  2021-03-15   ``Add FTPToS3Operator (#13707)``
`b896aee8cc <https://github.com/apache/airflow/commit/b896aee8cca6fe678e96e0eadf39ce956f9525fc>`_  2021-03-07   ``Cache Hook when initializing 'CloudFormationCreateStackSensor' (#14638)``
`e7bb17aeb8 <https://github.com/apache/airflow/commit/e7bb17aeb83b2218620c5320241b0c9f902d74ff>`_  2021-03-06   ``Use built-in 'cached_property' on Python 3.8 where possible (#14606)``
`697abf399d <https://github.com/apache/airflow/commit/697abf399de107eb4bafb730acf23d868e107a08>`_  2021-03-05   ``S3DataSource is not required (#14220)``
`8ced652ecf <https://github.com/apache/airflow/commit/8ced652ecf847ed668e5eed27e3e47a51a27b1c8>`_  2021-02-28   ``Implemented S3 Bucket Tagging (#14402)``
=================================================================================================  ===========  =============================================================================

1.2.0
.....

Latest change: 2021-02-27

=================================================================================================  ===========  ==========================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ==========================================================================
`589d6dec92 <https://github.com/apache/airflow/commit/589d6dec922565897785bcbc5ac6bb3b973d7f5d>`_  2021-02-27   ``Prepare to release the next wave of providers: (#14487)``
`13854c32a3 <https://github.com/apache/airflow/commit/13854c32a38787af6d8a52ab2465cb6185c0b74c>`_  2021-02-27   ``Adding support to put extra arguments for Glue Job. (#14027)``
`0d6cae4172 <https://github.com/apache/airflow/commit/0d6cae4172ff185ec4c0fc483bf556ce3252b7b0>`_  2021-02-24   ``Avoid using threads in S3 remote logging uplod (#14414)``
`ca35bd7f7f <https://github.com/apache/airflow/commit/ca35bd7f7f6bc2fb4f2afd7762114ce262c61941>`_  2021-02-21   ``By default PIP will install all packages in .local folder (#14125)``
`1b14726307 <https://github.com/apache/airflow/commit/1b147263076d48772d417c5154f2db86fc6a6877>`_  2021-02-11   ``Allow AWS Operator RedshiftToS3Transfer To Run a Custom Query (#14177)``
`9034f277ef <https://github.com/apache/airflow/commit/9034f277ef935df98b63963c824ba71e0dcd92c7>`_  2021-02-10   ``Document configuration for email backend credentials. (#14006)``
`8c5594b02f <https://github.com/apache/airflow/commit/8c5594b02ffbfc631ebc2366dbde6d8c4e56d550>`_  2021-02-08   ``includes the STS token if STS credentials are used (#11227)``
`cddbf9c11d <https://github.com/apache/airflow/commit/cddbf9c11d092422e6695d7a5a5c859fdf140753>`_  2021-02-06   ``Use MongoDB color for MongoToS3Operator (#14103)``
`10343ec29f <https://github.com/apache/airflow/commit/10343ec29f8f0abc5b932ba26faf49bc63c6bcda>`_  2021-02-05   ``Corrections in docs and tools after releasing provider RCs (#14082)``
=================================================================================================  ===========  ==========================================================================

1.1.0
.....

Latest change: 2021-02-04

=================================================================================================  ===========  =========================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  =========================================================================================
`88bdcfa0df <https://github.com/apache/airflow/commit/88bdcfa0df5bcb4c489486e05826544b428c8f43>`_  2021-02-04   ``Prepare to release a new wave of providers. (#14013)``
`ac2f72c98d <https://github.com/apache/airflow/commit/ac2f72c98dc0821b33721054588adbf2bb53bb0b>`_  2021-02-01   ``Implement provider versioning tools (#13767)``
`01049ddce2 <https://github.com/apache/airflow/commit/01049ddce210f475d6eae9b1cb306f750a1d6dd8>`_  2021-01-31   ``Add aws ses email backend for use with EmailOperator. (#13986)``
`ecfdc60bb6 <https://github.com/apache/airflow/commit/ecfdc60bb607fe0d13fa7e315476c607813abab6>`_  2021-01-29   ``Add bucket_name to template fileds in S3 operators (#13973)``
`d0ab7f6d3a <https://github.com/apache/airflow/commit/d0ab7f6d3a2976167f9c4fb309c502a4f866f983>`_  2021-01-25   ``Add ExasolToS3Operator (#13847)``
`6d55f329f9 <https://github.com/apache/airflow/commit/6d55f329f93c5cd1e94973194c0cd7caa65309e1>`_  2021-01-25   ``AWS Glue Crawler Integration (#13072)``
`f473ca7130 <https://github.com/apache/airflow/commit/f473ca7130f844bc59477674e641b42b80698bb7>`_  2021-01-24   ``Replace 'google_cloud_storage_conn_id' by 'gcp_conn_id' when using 'GCSHook' (#13851)``
`a9ac2b040b <https://github.com/apache/airflow/commit/a9ac2b040b64de1aa5d9c2b9def33334e36a8d22>`_  2021-01-23   ``Switch to f-strings using flynt. (#13732)``
`3fd5ef3555 <https://github.com/apache/airflow/commit/3fd5ef355556cf0ad7896bb570bbe4b2eabbf46e>`_  2021-01-21   ``Add missing logos for integrations (#13717)``
`29730d7200 <https://github.com/apache/airflow/commit/29730d720066a4c16d524e905de8cdf07e8cd129>`_  2021-01-20   ``Add acl_policy to S3CopyObjectOperator (#13773)``
`c065d32189 <https://github.com/apache/airflow/commit/c065d32189bfee80ab938d96ad74f6492e9c9b24>`_  2021-01-19   ``AllowDiskUse parameter and docs in MongotoS3Operator (#12033)``
`ab5fe56ac4 <https://github.com/apache/airflow/commit/ab5fe56ac4bda0d3fcdcbf58ed2632255b7ac713>`_  2021-01-16   ``Fix bug in GCSToS3Operator (#13718)``
`04d278f93f <https://github.com/apache/airflow/commit/04d278f93ffafb40fb6e95b41ecfa5f5cba5ef98>`_  2021-01-13   ``Add S3ToFTPOperator (#11747)``
`8d42d9ed69 <https://github.com/apache/airflow/commit/8d42d9ed69b03b372c6bc01309ef22e01b8db55f>`_  2021-01-11   ``add xcom push for ECSOperator (#12096)``
`308f1d0666 <https://github.com/apache/airflow/commit/308f1d06668ad427fd2483077d8e60f55ee617e6>`_  2021-01-07   ``[AIRFLOW-3723] Add Gzip capability to mongo_to_S3 operator (#13187)``
`f69405fb0b <https://github.com/apache/airflow/commit/f69405fb0b7c236968c730e1ad31a60eea2338c4>`_  2021-01-07   ``Fix S3KeysUnchangedSensor so that template_fields work (#13490)``
`4e479e1e1b <https://github.com/apache/airflow/commit/4e479e1e1b8eea71df48f5cc08a7dd15929ba177>`_  2021-01-06   ``Add S3KeySizeSensor (#13049)``
`f7a1334abe <https://github.com/apache/airflow/commit/f7a1334abe4417409498daad52c97d3f0eb95137>`_  2021-01-02   ``Add 'mongo_collection' to template_fields in MongoToS3Operator (#13361)``
`bd74eb0ca0 <https://github.com/apache/airflow/commit/bd74eb0ca0bb5f81cd98e2c151257a404d4a55a5>`_  2020-12-31   ``Allow Tags on AWS Batch Job Submission (#13396)``
`295d66f914 <https://github.com/apache/airflow/commit/295d66f91446a69610576d040ba687b38f1c5d0a>`_  2020-12-30   ``Fix Grammar in PIP warning (#13380)``
`625576a3af <https://github.com/apache/airflow/commit/625576a3af470cddad250735b74ba11e4880de0a>`_  2020-12-18   ``Fix spelling (#13135)``
`6cf76d7ac0 <https://github.com/apache/airflow/commit/6cf76d7ac01270930de7f105fb26428763ee1d4e>`_  2020-12-18   ``Fix typo in pip upgrade command :( (#13148)``
`5090fb0c89 <https://github.com/apache/airflow/commit/5090fb0c8967d2d8719c6f4a468f2151395b5444>`_  2020-12-15   ``Add script to generate integrations.json (#13073)``
=================================================================================================  ===========  =========================================================================================

1.0.0
.....

Latest change: 2020-12-09

=================================================================================================  ===========  ======================================================================================================================================================================
Commit                                                                                             Committed    Subject
=================================================================================================  ===========  ======================================================================================================================================================================
`32971a1a2d <https://github.com/apache/airflow/commit/32971a1a2de1db0b4f7442ed26facdf8d3b7a36f>`_  2020-12-09   ``Updates providers versions to 1.0.0 (#12955)``
`d5589673a9 <https://github.com/apache/airflow/commit/d5589673a95aaced0b851ea0a4061a010a924a82>`_  2020-12-08   ``Move dummy_operator.py to dummy.py (#11178) (#11293)``
`b40dffa085 <https://github.com/apache/airflow/commit/b40dffa08547b610162f8cacfa75847f3c4ca364>`_  2020-12-08   ``Rename remaing modules to match AIP-21 (#12917)``
`9b39f24780 <https://github.com/apache/airflow/commit/9b39f24780e85f859236672e9060b2fbeee81b36>`_  2020-12-08   ``Add support for dynamic connection form fields per provider (#12558)``
`bd90136aaf <https://github.com/apache/airflow/commit/bd90136aaf5035e3234fe545b79a3e4aad21efe2>`_  2020-11-30   ``Move operator guides to provider documentation packages (#12681)``
`02d94349be <https://github.com/apache/airflow/commit/02d94349be3d201ce9d37d7358573c937fd010df>`_  2020-11-29   ``Don't use time.time() or timezone.utcnow() for duration calculations (#12353)``
`de3b1e687b <https://github.com/apache/airflow/commit/de3b1e687b26c524c6909b7b4dfbb60d25019751>`_  2020-11-28   ``Move connection guides to provider documentation packages (#12653)``
`663259d4b5 <https://github.com/apache/airflow/commit/663259d4b541ab10ce55fec4d2460e23917062c2>`_  2020-11-25   ``Fix AWS DataSync tests failing (#11020)``
`3fa51f94d7 <https://github.com/apache/airflow/commit/3fa51f94d7a17f170ddc31908d36c91f4456a20b>`_  2020-11-24   ``Add check for duplicates in provider.yaml files (#12578)``
`ed09915a02 <https://github.com/apache/airflow/commit/ed09915a02b9b99e60689e647452addaab1688fc>`_  2020-11-23   ``[AIRFLOW-5115] Bugfix for S3KeySensor failing to accept template_fields (#12389)``
`370e7d07d1 <https://github.com/apache/airflow/commit/370e7d07d1ed1a53b73fe878425fdcd4c71a7ed1>`_  2020-11-21   ``Fix Python Docstring parameters (#12513)``
`c34ef853c8 <https://github.com/apache/airflow/commit/c34ef853c890e08f5468183c03dc8f3f3ce84af2>`_  2020-11-20   ``Separate out documentation building per provider  (#12444)``
`0080354502 <https://github.com/apache/airflow/commit/00803545023b096b8db4fbd6eb473843096d7ce4>`_  2020-11-18   ``Update provider READMEs for 1.0.0b2 batch release (#12449)``
`7ca0b6f121 <https://github.com/apache/airflow/commit/7ca0b6f121c9cec6e25de130f86a56d7c7fbe38c>`_  2020-11-18   ``Enable Markdownlint rule MD003/heading-style/header-style (#12427) (#12438)``
`ae7cb4a1e2 <https://github.com/apache/airflow/commit/ae7cb4a1e2a96351f1976cf5832615e24863e05d>`_  2020-11-17   ``Update wrong commit hash in backport provider changes (#12390)``
`6889a333cf <https://github.com/apache/airflow/commit/6889a333cff001727eb0a66e375544a28c9a5f03>`_  2020-11-15   ``Improvements for operators and hooks ref docs (#12366)``
`c94b1241a1 <https://github.com/apache/airflow/commit/c94b1241a144294f5f1c5f461d5e3b92e4a8fc38>`_  2020-11-13   ``Add extra error handling to S3 remote logging (#9908)``
`7825e8f590 <https://github.com/apache/airflow/commit/7825e8f59034645ab3247229be83a3aa90baece1>`_  2020-11-13   ``Docs installation improvements (#12304)``
`250436d962 <https://github.com/apache/airflow/commit/250436d962c8c950d38c1eb5e54a998891648cc9>`_  2020-11-10   ``Fix spelling in Python files (#12230)``
`85a18e13d9 <https://github.com/apache/airflow/commit/85a18e13d9dec84275283ff69e34704b60d54a75>`_  2020-11-09   ``Point at pypi project pages for cross-dependency of provider packages (#12212)``
`59eb5de78c <https://github.com/apache/airflow/commit/59eb5de78c70ee9c7ae6e4cba5c7a2babb8103ca>`_  2020-11-09   ``Update provider READMEs for up-coming 1.0.0beta1 releases (#12206)``
`b2a28d1590 <https://github.com/apache/airflow/commit/b2a28d1590410630d66966aa1f2b2a049a8c3b32>`_  2020-11-09   ``Moves provider packages scripts to dev (#12082)``
`fcb6b00efe <https://github.com/apache/airflow/commit/fcb6b00efef80c81272a30cfc618202a29e0c6a9>`_  2020-11-08   ``Add authentication to AWS with Google credentials (#12079)``
`fb6bddba0c <https://github.com/apache/airflow/commit/fb6bddba0c9e3e7ef2610b4fb3f73622e48d7ea0>`_  2020-11-07   ``In AWS Secrets backend, a lookup is optional (#12143)``
`cf9437d79f <https://github.com/apache/airflow/commit/cf9437d79f9658d1309e4bfe847fe63d52ec7b99>`_  2020-11-06   ``Simplify string expressions (#12123)``
`41bf172c1d <https://github.com/apache/airflow/commit/41bf172c1dc75099f4f9d8b3f3350b4b1f523ef9>`_  2020-11-04   ``Simplify string expressions (#12093)``
`4e8f9cc8d0 <https://github.com/apache/airflow/commit/4e8f9cc8d02b29c325b8a5a76b4837671bdf5f68>`_  2020-11-03   ``Enable Black - Python Auto Formmatter (#9550)``
`8c42cf1b00 <https://github.com/apache/airflow/commit/8c42cf1b00c90f0d7f11b8a3a455381de8e003c5>`_  2020-11-03   ``Use PyUpgrade to use Python 3.6 features (#11447)``
`5e77a61543 <https://github.com/apache/airflow/commit/5e77a61543d26e5466d885d639247aa5189c011d>`_  2020-11-02   ``Docstring fix for S3DeleteBucketOperator (#12049)``
`8222851348 <https://github.com/apache/airflow/commit/8222851348aa81424c9bdcea994e25e0d6692709>`_  2020-10-29   ``Add Template Fields to RedshiftToS3Operator & S3ToRedshiftOperator (#11844)``
`db121f726b <https://github.com/apache/airflow/commit/db121f726b3c7a37aca1ea05eb4714f884456005>`_  2020-10-28   ``Add truncate table (before copy) option to S3ToRedshiftOperator (#9246)``
`5a439e84eb <https://github.com/apache/airflow/commit/5a439e84eb6c0544dc6c3d6a9f4ceeb2172cd5d0>`_  2020-10-26   ``Prepare providers release 0.0.2a1 (#11855)``
`8afdb6ac6a <https://github.com/apache/airflow/commit/8afdb6ac6a7997cb14806bc2734c81c00ed8da97>`_  2020-10-26   ``Fix spellings (#11825)``
`872b1566a1 <https://github.com/apache/airflow/commit/872b1566a11cb73297e657ff325161721b296574>`_  2020-10-25   ``Generated backport providers readmes/setup for 2020.10.29 (#11826)``
`6ce855af11 <https://github.com/apache/airflow/commit/6ce855af118daeaa4c249669079ab9d9aad23945>`_  2020-10-24   ``Fix spelling (#11821)``
`3934ef2249 <https://github.com/apache/airflow/commit/3934ef22494db6d9613c229aaa82ea6a366b7c2f>`_  2020-10-24   ``Remove redundant builtins imports (#11809)``
`4c8e033c0e <https://github.com/apache/airflow/commit/4c8e033c0ee7d28963d504a9216205155f20f58f>`_  2020-10-24   ``Fix spelling and grammar (#11814)``
`4830687453 <https://github.com/apache/airflow/commit/48306874538eea7cfd42358d5ebb59705204bfc4>`_  2020-10-24   ``Use Python 3 style super classes (#11806)``
`0df60b7736 <https://github.com/apache/airflow/commit/0df60b773671ecf8d4e5f582ac2be200cf2a2edd>`_  2020-10-23   ``Add reattach flag to ECSOperator (#10643)``
`b9d677cdd6 <https://github.com/apache/airflow/commit/b9d677cdd660e0be8278a64658e73359276a9682>`_  2020-10-22   ``Add type hints to  aws provider (#11531)``
`349b0811c3 <https://github.com/apache/airflow/commit/349b0811c3022605426ba57d30936240a7c2848a>`_  2020-10-20   ``Add D200 pydocstyle check (#11688)``
`674368f66c <https://github.com/apache/airflow/commit/674368f66cf61b2a105f326f23868ac3aee08807>`_  2020-10-19   ``Fixes MySQLToS3 float to int conversion (#10437)``
`0823d46a7f <https://github.com/apache/airflow/commit/0823d46a7f267f2e45195a175021825367938add>`_  2020-10-16   ``Add type annotations for AWS operators and hooks (#11434)``
`16e7129719 <https://github.com/apache/airflow/commit/16e7129719f1c0940aef2a93bed81368e997a746>`_  2020-10-13   ``Added support for provider packages for Airflow 2.0 (#11487)``
`d38a0a781e <https://github.com/apache/airflow/commit/d38a0a781e123c8c50313efdb23f767d6678afe0>`_  2020-10-12   ``added type hints for aws cloud formation (#11470)``
`d305876bee <https://github.com/apache/airflow/commit/d305876bee328287ff391a29cc1cd632468cc731>`_  2020-10-12   ``Remove redundant None provided as default to dict.get() (#11448)``
`c3e340584b <https://github.com/apache/airflow/commit/c3e340584bf1892c4f73aa9e7495b5823dab0c40>`_  2020-10-11   ``Change prefix of AwsDynamoDB hook module (#11209)``
`42a23d16fe <https://github.com/apache/airflow/commit/42a23d16fe9b2f165b0805fb767ecbb825c93657>`_  2020-10-11   ``Update MySQLToS3Operator's s3_bucket to template_fields (#10778)``
`422b61a9dd <https://github.com/apache/airflow/commit/422b61a9dd95ab9d00b239daa14d87d7cae5ae73>`_  2020-10-09   ``Adding ElastiCache Hook for creating, describing and deleting replication groups (#8701)``
`dd98b21494 <https://github.com/apache/airflow/commit/dd98b21494ff6036242b63268140abe1294b3657>`_  2020-10-06   ``Add acl_policy parameter to GCSToS3Operator (#10804) (#10829)``
`32b3cfbcf0 <https://github.com/apache/airflow/commit/32b3cfbcf0209cb062dd641c1232ab25d02d4d6d>`_  2020-10-06   ``Strict type check for all hooks in amazon (#11250)``
`6d573e8abb <https://github.com/apache/airflow/commit/6d573e8abbf87e3c7281347e03d428a6e5baccd4>`_  2020-10-03   ``Add s3 key to template fields for s3/redshift transfer operators (#10890)``
`0a0e1af800 <https://github.com/apache/airflow/commit/0a0e1af80038ef89974c3c8444461fe867945daa>`_  2020-10-03   ``Fix Broken Markdown links in Providers README TOC (#11249)``
`ca4238eb4d <https://github.com/apache/airflow/commit/ca4238eb4d9a2aef70eb641343f59ee706d27d13>`_  2020-10-02   ``Fixed month in backport packages to October (#11242)``
`5220e4c384 <https://github.com/apache/airflow/commit/5220e4c3848a2d2c81c266ef939709df9ce581c5>`_  2020-10-02   ``Prepare Backport release 2020.09.07 (#11238)``
`00ffedb8c4 <https://github.com/apache/airflow/commit/00ffedb8c402eb5638782628eb706a5f28215eac>`_  2020-09-30   ``Add amazon glacier to GCS transfer operator (#10947)``
`e3f96ce7a8 <https://github.com/apache/airflow/commit/e3f96ce7a8ac098aeef5e9930e6de6c428274d57>`_  2020-09-24   ``Fix incorrect Usage of Optional[bool] (#11138)``
`f3e87c5030 <https://github.com/apache/airflow/commit/f3e87c503081a3085dff6c7352640d7f08beb5bc>`_  2020-09-22   ``Add D202 pydocstyle check (#11032)``
`b61225a885 <https://github.com/apache/airflow/commit/b61225a8850b20be17842c2428b91d873584c4da>`_  2020-09-21   ``Add D204 pydocstyle check (#11031)``
`2410f592a4 <https://github.com/apache/airflow/commit/2410f592a4ab160b377f1a9e5de3b7262b9851cc>`_  2020-09-19   ``Get Airflow configs with sensitive data from AWS Systems Manager (#11023)``
`2bf7b7cac7 <https://github.com/apache/airflow/commit/2bf7b7cac7858f5a6a495f1a9eb4780ec84f95b4>`_  2020-09-19   ``Add typing to amazon provider EMR (#10910)``
`9edfcb7ac4 <https://github.com/apache/airflow/commit/9edfcb7ac46917836ec956264da8876e58d92392>`_  2020-09-19   ``Support extra_args in S3Hook and GCSToS3Operator (#11001)``
`4e1f3a69db <https://github.com/apache/airflow/commit/4e1f3a69db8614c302e4916332555034053b935c>`_  2020-09-14   ``[AIRFLOW-10645] Add AWS Secrets Manager Hook (#10655)``
`e9add79160 <https://github.com/apache/airflow/commit/e9add79160e3a16bb348e30f4e83386a371dbc1e>`_  2020-09-14   ``Fix Failing static tests on Master (#10927)``
`383a118d2d <https://github.com/apache/airflow/commit/383a118d2df618e46d81c520cd2c4a31d81b33dd>`_  2020-09-14   ``Add more type annotations to AWS hooks (#10671)``
`9549274d11 <https://github.com/apache/airflow/commit/9549274d110f689a0bd709db829a4d69e274eed9>`_  2020-09-09   ``Upgrade black to 20.8b1 (#10818)``
`2934220dc9 <https://github.com/apache/airflow/commit/2934220dc98e295764f7791d33e121629ed2fbbb>`_  2020-09-08   ``Always return a list from S3Hook list methods (#10774)``
`f40ac9b151 <https://github.com/apache/airflow/commit/f40ac9b151124dbcd87197d6ae38c85191d41f38>`_  2020-09-01   ``Add placement_strategy option (#9444)``
`e4878e6775 <https://github.com/apache/airflow/commit/e4878e6775bbe5cb2a1d786e57e009271b78bba0>`_  2020-08-31   ``fix type hints for s3 hook read_key method (#10653)``
`2ca615cffe <https://github.com/apache/airflow/commit/2ca615cffefe97dfa38e1b7f60d9ed33c6628992>`_  2020-08-29   ``Update Google Cloud branding (#10642)``
`8969b7185e <https://github.com/apache/airflow/commit/8969b7185ebc3c90168ce9a2fb97dfbc74d2bed9>`_  2020-08-28   ``Removed bad characters from AWS operator (#10590)``
`8349061f9c <https://github.com/apache/airflow/commit/8349061f9cb01a92c87edd349cc844c4053851e8>`_  2020-08-26   ``Improve Docstring for AWS Athena Hook/Operator (#10580)``
`fdd9b6f65b <https://github.com/apache/airflow/commit/fdd9b6f65b608c516b8a062b058972d9a45ec9e3>`_  2020-08-25   ``Enable Black on Providers Packages (#10543)``
`3696c34c28 <https://github.com/apache/airflow/commit/3696c34c28c6bc7b442deab999d9ecba24ed0e34>`_  2020-08-24   ``Fix typo in the word "release" (#10528)``
`3734876d98 <https://github.com/apache/airflow/commit/3734876d9898067ee933b84af522d53df6160d7f>`_  2020-08-24   ``Implement impersonation in google operators (#10052)``
`ee7ca128a1 <https://github.com/apache/airflow/commit/ee7ca128a17937313566f2badb6cc569c614db94>`_  2020-08-22   ``Fix broken Markdown refernces in Providers README (#10483)``
`c6358045f9 <https://github.com/apache/airflow/commit/c6358045f9d61af63c96833cb6682d6f382a6408>`_  2020-08-22   ``Fixes S3ToRedshift COPY query (#10436)``
`7c206a82a6 <https://github.com/apache/airflow/commit/7c206a82a6f074abcc4898a005ecd2c84a920054>`_  2020-08-22   ``Replace assigment with Augmented assignment (#10468)``
`27d08b76a2 <https://github.com/apache/airflow/commit/27d08b76a2d171d716a1599157a8a60a121dbec6>`_  2020-08-21   ``Amazon SES Hook (#10391)``
`dea345b05c <https://github.com/apache/airflow/commit/dea345b05c2cd226e70f97a3934d7456aa1cc754>`_  2020-08-17   ``Fix AwsGlueJobSensor to stop running after the Glue job finished (#9022)``
`f6734b3b85 <https://github.com/apache/airflow/commit/f6734b3b850d33d3712763f93c114e80f5af9ffb>`_  2020-08-12   ``Enable Sphinx spellcheck for doc generation (#10280)``
`82f744b871 <https://github.com/apache/airflow/commit/82f744b871bb2c5e9a2d628e1c45ae16c1244240>`_  2020-08-11   ``Add type annotations to AwsGlueJobHook, RedshiftHook modules (#10286)``
`19bc97d0ce <https://github.com/apache/airflow/commit/19bc97d0ce436a6ec9d8e9a5adcd48c0a769d01f>`_  2020-08-10   ``Revert "Add Amazon SES hook (#10004)" (#10276)``
`f06fe616e6 <https://github.com/apache/airflow/commit/f06fe616e66256bdc53710de505c2c6b1bd21528>`_  2020-08-10   ``Add Amazon SES hook (#10004)``
`0c77ea8a3c <https://github.com/apache/airflow/commit/0c77ea8a3c417805f66d10f0c757ca218bf8dee0>`_  2020-08-06   ``Add type annotations to S3 hook module (#10164)``
`24c8e4c2d6 <https://github.com/apache/airflow/commit/24c8e4c2d6e359ecc2c7d6275dccc68de4a82832>`_  2020-08-06   ``Changes to all the constructors to remove the args argument (#10163)``
`9667314b2f <https://github.com/apache/airflow/commit/9667314b2fb879edc451793a8350123507e1cfd6>`_  2020-08-05   ``Add correct signatures for operators in amazon provider package (#10167)``
`000287753b <https://github.com/apache/airflow/commit/000287753b478f29e6c25442ac253e3a6c8e8c87>`_  2020-08-03   ``Improve Typing coverage of amazon/aws/athena (#10025)``
`53ada6e791 <https://github.com/apache/airflow/commit/53ada6e7911f411e80ebb00be9f07a7cc0788d01>`_  2020-08-03   ``Add S3KeysUnchangedSensor (#9817)``
`aeea71274d <https://github.com/apache/airflow/commit/aeea71274d4527ff2351102e94aa38bda6099e7f>`_  2020-08-02   ``Remove 'args' parameter from provider operator constructors (#10097)``
`2b8dea64e9 <https://github.com/apache/airflow/commit/2b8dea64e9e8716fba8c38a1b439f7835bbd2918>`_  2020-08-01   ``Fix typo in Athena sensor retries (#10079)``
`1508c43ec9 <https://github.com/apache/airflow/commit/1508c43ec9594e801b415dd82472fa017791b759>`_  2020-07-29   ``Adding new SageMaker operator for ProcessingJobs (#9594)``
`7d24b088cd <https://github.com/apache/airflow/commit/7d24b088cd736cfa18f9214e4c9d6ce2d5865f3d>`_  2020-07-25   ``Stop using start_date in default_args in example_dags (2) (#9985)``
`8b10a4b35e <https://github.com/apache/airflow/commit/8b10a4b35e45d536a6475bfe1491ee75fad50186>`_  2020-07-25   ``Stop using start_date in default_args in example_dags (#9982)``
`33f0cd2657 <https://github.com/apache/airflow/commit/33f0cd2657b2e77ea3477e0c93f13f1474be628e>`_  2020-07-22   ``apply_default keeps the function signature for mypy (#9784)``
`e7c87fe453 <https://github.com/apache/airflow/commit/e7c87fe453c6a70ed087c7ffbccaacbf0d2831b9>`_  2020-07-20   ``Refactor AwsBaseHook._get_credentials (#9878)``
`2577f9334a <https://github.com/apache/airflow/commit/2577f9334a5cb71cccd97e62b0ae2d097cb99e1a>`_  2020-07-16   ``Fix S3FileTransformOperator to support S3 Select transformation only (#8936)``
`52b6efe1ec <https://github.com/apache/airflow/commit/52b6efe1ecaae74b9c2497f565e116305d575a76>`_  2020-07-15   ``Add option to delete by prefix to S3DeleteObjectsOperator (#9350)``
`553bb7af7c <https://github.com/apache/airflow/commit/553bb7af7cb7a50f7141b5b89297713cee6d19f6>`_  2020-07-13   ``Keep functions signatures in decorators (#9786)``
`2f31b3060e <https://github.com/apache/airflow/commit/2f31b3060ed8274d5d1b1db7349ce607640b9199>`_  2020-07-08   ``Get Airflow configs with sensitive data from Secret Backends (#9645)``
`07b81029eb <https://github.com/apache/airflow/commit/07b81029ebc2a296fb54181f2cec11fcc7704d9d>`_  2020-07-08   ``Allow AWSAthenaHook to get more than 1000/first page of results (#6075)``
`564192c162 <https://github.com/apache/airflow/commit/564192c1625a552456cebb3751978c08eebdb2a1>`_  2020-07-08   ``Add AWS StepFunctions integrations to the aws provider (#8749)``
`ecce1ace7a <https://github.com/apache/airflow/commit/ecce1ace7a277c948c61d7d4cbfc8632cc216559>`_  2020-07-08   ``[AIRFLOW-XXXX] Remove unnecessary docstring in AWSAthenaOperator``
`a79e2d4c4a <https://github.com/apache/airflow/commit/a79e2d4c4aa105f3fac5ae6a28e29af9cd572407>`_  2020-07-06   ``Move provider's log task handlers to the provider package (#9604)``
`ee20086b8c <https://github.com/apache/airflow/commit/ee20086b8c499fa40dcaac71652f21b466e7f80f>`_  2020-07-02   ``Move S3TaskHandler to the AWS provider package (#9602)``
`40add26d45 <https://github.com/apache/airflow/commit/40add26d459c2511a6d9d305ae7300f0d6104211>`_  2020-06-29   ``Remove almost all references to airflow.contrib (#9559)``
`c858babddf <https://github.com/apache/airflow/commit/c858babddf8b18b417993b5bfefec1c5635510da>`_  2020-06-26   ``Remove kwargs from Super calls in AWS Secrets Backends (#9523)``
`87fdbd0708 <https://github.com/apache/airflow/commit/87fdbd0708d942af98d35604fe5962962e25d246>`_  2020-06-25   ``Use literal syntax instead of function calls to create data structure (#9516)``
`c7a454aa32 <https://github.com/apache/airflow/commit/c7a454aa32bf33133d042e8438ac259b32144b21>`_  2020-06-22   ``Add AWS ECS system test (#8888)``
`df8efd04f3 <https://github.com/apache/airflow/commit/df8efd04f394afc4b5affb677bc78d8b7bd5275a>`_  2020-06-21   ``Enable & Fix "Docstring Content Issues" PyDocStyle Check (#9460)``
`e13a14c873 <https://github.com/apache/airflow/commit/e13a14c8730f4f633d996dd7d3468fe827136a84>`_  2020-06-21   ``Enable & Fix Whitespace related PyDocStyle Checks (#9458)``
`d0e7db4024 <https://github.com/apache/airflow/commit/d0e7db4024806af35e3c9a2cae460fdeedd4d2ec>`_  2020-06-19   ``Fixed release number for fresh release (#9408)``
`12af6a0800 <https://github.com/apache/airflow/commit/12af6a08009b8776e00d8a0aab92363eb8c4e8b1>`_  2020-06-19   ``Final cleanup for 2020.6.23rc1 release preparation (#9404)``
`992a18c84a <https://github.com/apache/airflow/commit/992a18c84a355d13e821c703e7364f12233c37dc>`_  2020-06-19   ``Move MySqlToS3Operator to transfers (#9400)``
`a60f589aa2 <https://github.com/apache/airflow/commit/a60f589aa251cc3df6bec5b306ad4a7f736f539f>`_  2020-06-19   ``Add MySqlToS3Operator (#9054)``
`c7e5bce57f <https://github.com/apache/airflow/commit/c7e5bce57fe7f51cefce4f8a41ce408ac5675d13>`_  2020-06-19   ``Prepare backport release candidate for 2020.6.23rc1 (#9370)``
`40bf8f28f9 <https://github.com/apache/airflow/commit/40bf8f28f97f17f40d993d207ea740eba54593ee>`_  2020-06-18   ``Detect automatically the lack of reference to the guide in the operator descriptions (#9290)``
`f6bd817a3a <https://github.com/apache/airflow/commit/f6bd817a3aac0a16430fc2e3d59c1f17a69a15ac>`_  2020-06-16   ``Introduce 'transfers' packages (#9320)``
`58a8ec0e46 <https://github.com/apache/airflow/commit/58a8ec0e46f624ee0369dd156dd8fb4f81884a21>`_  2020-06-16   ``AWSBatchOperator <> ClientHook relation changed to composition (#9306)``
`a80cd25e8e <https://github.com/apache/airflow/commit/a80cd25e8eb7f8b5d89af26cdcd62a5bbe44d65c>`_  2020-06-15   ``Close/Flush byte stream in s3 hook load_string and load_bytes (#9211)``
`ffb8574037 <https://github.com/apache/airflow/commit/ffb85740373f7adb70d28ec7d5a8886380170e5e>`_  2020-06-14   ``Decrypt secrets from SystemsManagerParameterStoreBackend (#9214)``
`a69b031f20 <https://github.com/apache/airflow/commit/a69b031f20c5a1cd032f9873394374f661811e8f>`_  2020-06-10   ``Add S3ToRedshift example dag and system test (#8877)``
`17adcea835 <https://github.com/apache/airflow/commit/17adcea835cb7b0cf2d8da0ac7dda5549cfa3e45>`_  2020-06-02   ``Fix handling of subprocess error handling in s3_file_transform and gcs (#9106)``
`357e11e0cf <https://github.com/apache/airflow/commit/357e11e0cfb4c02833018e073bc4f5e5b52fae4f>`_  2020-05-29   ``Add Delete/Create S3 bucket operators (#8895)``
`1ed171bfb2 <https://github.com/apache/airflow/commit/1ed171bfb265ded8674058bdc425640d25f1f4fc>`_  2020-05-28   ``Add script_args for S3FileTransformOperator (#9019)``
`0b0e4f7a4c <https://github.com/apache/airflow/commit/0b0e4f7a4cceff3efe15161fb40b984782760a34>`_  2020-05-26   ``Preparing for RC3 relase of backports (#9026)``
`00642a46d0 <https://github.com/apache/airflow/commit/00642a46d019870c4decb3d0e47c01d6a25cb88c>`_  2020-05-26   ``Fixed name of 20 remaining wrongly named operators. (#8994)``
`1d36b0303b <https://github.com/apache/airflow/commit/1d36b0303b8632fce6de78ca4e782ae26ee06fea>`_  2020-05-23   ``Fix references in docs (#8984)``
`f946f96da4 <https://github.com/apache/airflow/commit/f946f96da45d8e6101805450d8cab7ccb2774ad0>`_  2020-05-23   ``Old json boto compat removed from dynamodb_to_s3 operator (#8987)``
`375d1ca229 <https://github.com/apache/airflow/commit/375d1ca229464617780623c61c6e8a1bf570c87f>`_  2020-05-19   ``Release candidate 2 for backport packages 2020.05.20 (#8898)``
`12c5e5d8ae <https://github.com/apache/airflow/commit/12c5e5d8ae25fa633efe63ccf4db389e2b796d79>`_  2020-05-17   ``Prepare release candidate for backport packages (#8891)``
`f3521fb0e3 <https://github.com/apache/airflow/commit/f3521fb0e36733d8bd356123e56a453fd37a6dca>`_  2020-05-16   ``Regenerate readme files for backport package release (#8886)``
`f4edd90a94 <https://github.com/apache/airflow/commit/f4edd90a94b8f91bbefbbbfba367372399559596>`_  2020-05-16   ``Speed up TestAwsLambdaHook by not actually running a function (#8882)``
`92585ca4cb <https://github.com/apache/airflow/commit/92585ca4cb375ac879f4ab331b3a063106eb7b92>`_  2020-05-15   ``Added automated release notes generation for backport operators (#8807)``
`85bbab27db <https://github.com/apache/airflow/commit/85bbab27dbb4f55f6f322b894fe3d54797076c15>`_  2020-05-15   ``Add EMR operators howto docs (#8863)``
`e61b9bb9bb <https://github.com/apache/airflow/commit/e61b9bb9bbe6d8a0621310f3583483b9135c6770>`_  2020-05-13   ``Add AWS EMR System tests (#8618)``
`ed3f5131a2 <https://github.com/apache/airflow/commit/ed3f5131a27e2ef0422f2495a4532630a6204f82>`_  2020-05-13   ``Correctly pass sleep time from AWSAthenaOperator down to the hook. (#8845)``
`7236862a1f <https://github.com/apache/airflow/commit/7236862a1f5361b5e99c03dd63dae9b966efcd24>`_  2020-05-12   ``[AIRFLOW-2310] Enable AWS Glue Job Integration (#6007)``
`d590e5e767 <https://github.com/apache/airflow/commit/d590e5e7679322bebb1472fa8c7ec6d183e4154a>`_  2020-05-11   ``Add option to propagate tags in ECSOperator (#8811)``
`0c3db84c3c <https://github.com/apache/airflow/commit/0c3db84c3ce5107f53ed5ecc48edfdfe1b97feff>`_  2020-05-11   ``[AIRFLOW-7068] Create EC2 Hook, Operator and Sensor (#7731)``
`cbebed2b4d <https://github.com/apache/airflow/commit/cbebed2b4d0bd1e0984c331c0270e83bf8df8540>`_  2020-05-10   ``Allow passing backend_kwargs to AWS SSM client (#8802)``
`c7788a6894 <https://github.com/apache/airflow/commit/c7788a6894cb79c22153434dd9b977393b8236be>`_  2020-05-10   ``Add imap_attachment_to_s3 example dag and system test (#8669)``
`ff5b70149b <https://github.com/apache/airflow/commit/ff5b70149bf51012156378c8fc8b072c7c280d9d>`_  2020-05-07   ``Add google_api_to_s3_transfer example dags and system tests (#8581)``
`4421f011ee <https://github.com/apache/airflow/commit/4421f011eeec2d1022a39933e27f530fb9f9c1b1>`_  2020-05-01   ``Improve template capabilities of EMR job and step operators (#8572)``
`379a884d64 <https://github.com/apache/airflow/commit/379a884d645a4d73db1c81e3450adc82571989ea>`_  2020-04-28   ``fix: aws hook should work without conn id (#8534)``
`74bc316c56 <https://github.com/apache/airflow/commit/74bc316c56192f14677e9406d3878887a836062b>`_  2020-04-27   ``[AIRFLOW-4438] Add Gzip compression to S3_hook (#8571)``
`7ea66a1a95 <https://github.com/apache/airflow/commit/7ea66a1a9594704869e82513d3a06fe35b6109b2>`_  2020-04-26   ``Add example DAG for ECSOperator (#8452)``
`b6434dedf9 <https://github.com/apache/airflow/commit/b6434dedf974085e5f8891446fa63104836c8fdf>`_  2020-04-24   ``[AIRFLOW-7111] Add generate_presigned_url method to S3Hook (#8441)``
`becedd5af8 <https://github.com/apache/airflow/commit/becedd5af8df01a0210e0a3fa78e619785f39908>`_  2020-04-19   ``Remove unrelated EC2 references in ECSOperator (#8451)``
`ab1290cb0c <https://github.com/apache/airflow/commit/ab1290cb0c5856fa85c8596bfdf780fcdfd99c31>`_  2020-04-13   ``Make launch_type parameter optional (#8248)``
`87969a350d <https://github.com/apache/airflow/commit/87969a350ddd41e9e77776af6d780b31e363eaca>`_  2020-04-09   ``[AIRFLOW-6515] Change Log Levels from Info/Warn to Error (#8170)``
`b46d6c0602 <https://github.com/apache/airflow/commit/b46d6c060280da59193a28cf67e791eb825cb51c>`_  2020-04-08   ``Add support for AWS Secrets Manager as Secrets Backend (#8186)``
`68d1714f29 <https://github.com/apache/airflow/commit/68d1714f296989b7aad1a04b75dc033e76afb747>`_  2020-04-04   ``[AIRFLOW-6822] AWS hooks should cache boto3 client (#7541)``
`8a02402576 <https://github.com/apache/airflow/commit/8a02402576f83869d5134b4bddef5d73c15a8320>`_  2020-03-31   ``Rename CloudBaseHook to GoogleBaseHook and move it to google.common (#8011)``
`7239d9a82d <https://github.com/apache/airflow/commit/7239d9a82dbb3b9bdf27b531daa70338af9dd796>`_  2020-03-28   ``Get Airflow Variables from AWS Systems Manager Parameter Store (#7945)``
`eb4af4f944 <https://github.com/apache/airflow/commit/eb4af4f944c77e67e167bbb6b0a2aaf075a95b50>`_  2020-03-28   ``Make BaseSecretsBackend.build_path generic (#7948)``
`438da7241e <https://github.com/apache/airflow/commit/438da7241eb537e3ef5ae711629446155bf738a3>`_  2020-03-28   ``[AIRFLOW-5825] SageMakerEndpointOperator is not idempotent (#7891)``
`686d7d50bd <https://github.com/apache/airflow/commit/686d7d50bd21622724d6818021355bc6885fd3de>`_  2020-03-25   ``Standardize SecretBackend class names (#7846)``
`eef87b9953 <https://github.com/apache/airflow/commit/eef87b9953347a65421f315a07dbef37ded9df66>`_  2020-03-23   ``[AIRFLOW-7105] Unify Secrets Backend method interfaces (#7830)``
`5648dfbc30 <https://github.com/apache/airflow/commit/5648dfbc300337b10567ef4e07045ea29d33ec06>`_  2020-03-23   ``Add missing call to Super class in 'amazon', 'cloudant & 'databricks' providers (#7827)``
`a360024123 <https://github.com/apache/airflow/commit/a36002412334c445e4eab41fdbb85ef31b6fd384>`_  2020-03-19   ``[AIRFLOW-5705] Make AwsSsmSecretsBackend consistent with VaultBackend (#7753)``
`2a54512d78 <https://github.com/apache/airflow/commit/2a54512d785ba603ba71381dc3dfa049e9f74063>`_  2020-03-17   ``[AIRFLOW-5705] Fix bugs in AWS SSM Secrets Backend (#7745)``
`a8b5fc74d0 <https://github.com/apache/airflow/commit/a8b5fc74d07e50c91bb64cb66ca1a450aa5ce6e1>`_  2020-03-16   ``[AIRFLOW-4175] S3Hook load_file should support ACL policy paramete (#7733)``
`e31e9ddd23 <https://github.com/apache/airflow/commit/e31e9ddd2332e5d92422baf668acee441646ad68>`_  2020-03-14   ``[AIRFLOW-5705] Add secrets backend and support for AWS SSM (#6376)``
`3bb60afc7b <https://github.com/apache/airflow/commit/3bb60afc7b8319996385d681faac342afe2b3bd2>`_  2020-03-13   ``[AIRFLOW-6975] Base AWSHook AssumeRoleWithSAML (#7619)``
`c0c5f11ad1 <https://github.com/apache/airflow/commit/c0c5f11ad11a5a38e0553c1a36aa75eb83efae51>`_  2020-03-12   ``[AIRFLOW-6884] Make SageMakerTrainingOperator idempotent (#7598)``
`b7cdda1c64 <https://github.com/apache/airflow/commit/b7cdda1c64595bc7f85519337029de259e573fce>`_  2020-03-10   ``[AIRFLOW-4438] Add Gzip compression to S3_hook (#7680)``
`42eef38217 <https://github.com/apache/airflow/commit/42eef38217e709bc7a7f71bf0286e9e61293a43e>`_  2020-03-07   ``[AIRFLOW-6877] Add cross-provider dependencies as extras (#7506)``
`9a94ab246d <https://github.com/apache/airflow/commit/9a94ab246db8c09aa83bb6a6d245b1ca9563bcd9>`_  2020-03-01   ``[AIRFLOW-6962] Fix compeleted to completed (#7600)``
`1b38f6d9b6 <https://github.com/apache/airflow/commit/1b38f6d9b6710bd5e25fc16883599f1842ab7cb9>`_  2020-02-29   ``[AIRFLOW-5908] Add download_file to S3 Hook (#6577)``
`3ea3e1a2b5 <https://github.com/apache/airflow/commit/3ea3e1a2b580b7ed10efe668de0cc37b03673500>`_  2020-02-26   ``[AIRFLOW-6824] EMRAddStepsOperator problem with multi-step XCom (#7443)``
`6eaa7e3b18 <https://github.com/apache/airflow/commit/6eaa7e3b1845644d5ec65a00a997f4029bec9628>`_  2020-02-25   ``[AIRFLOW-5924] Automatically unify bucket name and key in S3Hook (#6574)``
`3320e432a1 <https://github.com/apache/airflow/commit/3320e432a129476dbc1c55be3b3faa3326a635bc>`_  2020-02-24   ``[AIRFLOW-6817] Lazy-load 'airflow.DAG' to keep user-facing API untouched (#7517)``
`7d0e7122dd <https://github.com/apache/airflow/commit/7d0e7122dd14576d834c6f66fe919a72b100b7f8>`_  2020-02-24   ``[AIRFLOW-6830] Add Subject/MessageAttributes to SNS hook and operator (#7451)``
`4d03e33c11 <https://github.com/apache/airflow/commit/4d03e33c115018e30fa413c42b16212481ad25cc>`_  2020-02-22   ``[AIRFLOW-6817] remove imports from 'airflow/__init__.py', replaced implicit imports with explicit imports, added entry to 'UPDATING.MD' - squashed/rebased (#7456)``
`47a922b864 <https://github.com/apache/airflow/commit/47a922b86426968bfa07cc7892d2eeeca761d884>`_  2020-02-21   ``[AIRFLOW-6854] Fix missing typing_extensions on python 3.8 (#7474)``
`9cbd7de6d1 <https://github.com/apache/airflow/commit/9cbd7de6d115795aba8bfb8addb060bfdfbdf87b>`_  2020-02-18   ``[AIRFLOW-6792] Remove _operator/_hook/_sensor in providers package and add tests (#7412)``
`58c3542ed2 <https://github.com/apache/airflow/commit/58c3542ed25061320ce61dbe0adf451a44c738dd>`_  2020-02-12   ``[AIRFLOW-5231] Fix S3Hook.delete_objects method (#7375)``
`b7aa778b38 <https://github.com/apache/airflow/commit/b7aa778b38df2f116a1c20031e72fea8b97315bf>`_  2020-02-10   ``[AIRFLOW-6767] Correct name for default Athena workgroup (#7394)``
`9282185e66 <https://github.com/apache/airflow/commit/9282185e6624e64bb7f17447f81c1b2d1bb4d56d>`_  2020-02-09   ``[AIRFLOW-6761] Fix WorkGroup param in AWSAthenaHook (#7386)``
`94fccca970 <https://github.com/apache/airflow/commit/94fccca97030ee59d89f302a98137b17e7b01a33>`_  2020-02-04   ``[AIRFLOW-XXXX] Add pre-commit check for utf-8 file encoding (#7347)``
`f3ad5cf618 <https://github.com/apache/airflow/commit/f3ad5cf6185b9d406d0fb0a4ecc0b5536f79217a>`_  2020-02-03   ``[AIRFLOW-4681] Make sensors module pylint compatible (#7309)``
`88e40c714d <https://github.com/apache/airflow/commit/88e40c714d2853aa8966796945b2907c263fed08>`_  2020-02-03   ``[AIRFLOW-6716] Fix AWS Datasync Example DAG (#7339)``
`a311d3d82e <https://github.com/apache/airflow/commit/a311d3d82e0c2e32bcb56e29f33c95ed0a2a2ddc>`_  2020-02-03   ``[AIRFLOW-6718] Fix more occurrences of utils.dates.days_ago (#7341)``
`cb766b05b1 <https://github.com/apache/airflow/commit/cb766b05b17b80fd54a5ce6ac3ee35a631115000>`_  2020-02-03   ``[AIRFLOW-XXXX] Fix Static Checks on CI (#7342)``
`97a429f9d0 <https://github.com/apache/airflow/commit/97a429f9d0cf740c5698060ad55f11e93cb57b55>`_  2020-02-02   ``[AIRFLOW-6714] Remove magic comments about UTF-8 (#7338)``
`7527eddc5e <https://github.com/apache/airflow/commit/7527eddc5e9729aa7e732209a07d57985f6c73e4>`_  2020-02-02   ``[AIRFLOW-4364] Make all code in airflow/providers/amazon pylint compatible (#7336)``
`cf141506a2 <https://github.com/apache/airflow/commit/cf141506a25dbba279b85500d781f7e056540721>`_  2020-02-02   ``[AIRFLOW-6708] Set unique logger names (#7330)``
`63aa3db88f <https://github.com/apache/airflow/commit/63aa3db88f8824efe79622301efd9f8ba75b991c>`_  2020-02-02   ``[AIRFLOW-6258] Add CloudFormation operators to AWS providers (#6824)``
`af4157fdef <https://github.com/apache/airflow/commit/af4157fdeffc0c18492b518708c0db44815067ab>`_  2020-02-02   ``[AIRFLOW-6672] AWS DataSync - better logging of error message (#7288)``
`373c6aa4a2 <https://github.com/apache/airflow/commit/373c6aa4a208284b5ff72987e4bd8f4e2ada1a1b>`_  2020-01-30   ``[AIRFLOW-6682] Move GCP classes to providers package (#7295)``
`83c037873f <https://github.com/apache/airflow/commit/83c037873ff694eed67ba8b30f2d9c88b2c7c6f2>`_  2020-01-30   ``[AIRFLOW-6674] Move example_dags in accordance with AIP-21 (#7287)``
`1988a97e8f <https://github.com/apache/airflow/commit/1988a97e8f687e28a5a39b29677fb514e097753c>`_  2020-01-28   ``[AIRFLOW-6659] Move AWS Transfer operators to providers package (#7274)``
`ab10443e96 <https://github.com/apache/airflow/commit/ab10443e965269efe9c1efaf5fa33bcdbe609f13>`_  2020-01-28   ``[AIRFLOW-6424] Added a operator to modify EMR cluster (#7213)``
`40246132a7 <https://github.com/apache/airflow/commit/40246132a7ef3b07fe3173c6e7646ed6b53aad6e>`_  2020-01-28   ``[AIRFLOW-6654] AWS DataSync - bugfix when creating locations (#7270)``
`82c0e5aff6 <https://github.com/apache/airflow/commit/82c0e5aff6004f636b98e207c3caec40b403fbbe>`_  2020-01-28   ``[AIRFLOW-6655] Move AWS classes to providers (#7271)``
`599e4791c9 <https://github.com/apache/airflow/commit/599e4791c91cff411b1bf1c45555db5094c2b420>`_  2020-01-18   ``[AIRFLOW-6541] Use EmrJobFlowSensor for other states (#7146)``
`c319e81cae <https://github.com/apache/airflow/commit/c319e81cae1de31ad1373903252d8608ffce1fba>`_  2020-01-17   ``[AIRFLOW-6572] Move AWS classes to providers.amazon.aws package (#7178)``
`941a070578 <https://github.com/apache/airflow/commit/941a070578bc7d9410715b89658548167352cc4d>`_  2020-01-15   ``[AIRFLOW-6570] Add dag tag for all example dag (#7176)``
`78d8fe6944 <https://github.com/apache/airflow/commit/78d8fe6944b689b9b0af99255286e34e06eedec3>`_  2020-01-08   ``[AIRFLOW-6245] Add custom waiters for AWS batch jobs (#6811)``
`e0b0227257 <https://github.com/apache/airflow/commit/e0b022725749181bd4e30933e4a0ffefb993eede>`_  2019-12-28   ``[AIRFLOW-6319] Add support for AWS Athena workgroups (#6871)``
`57da456854 <https://github.com/apache/airflow/commit/57da45685457520d51a0967e2aeb5e5ff162dfa7>`_  2019-12-24   ``[AIRFLOW-6333] Bump Pylint to 2.4.4 & fix/disable new checks (#6888)``
`cf647c27e0 <https://github.com/apache/airflow/commit/cf647c27e0f35bbd1183bfcf87a106cbdb69d3fa>`_  2019-12-18   ``[AIRFLOW-6038] AWS DataSync reworked (#6773)``
`7502cad284 <https://github.com/apache/airflow/commit/7502cad2844139d57e4276d971c0706a361d9dbe>`_  2019-12-17   ``[AIRFLOW-6206] Move and rename AWS batch operator [AIP-21] (#6764)``
`c4c635df69 <https://github.com/apache/airflow/commit/c4c635df6906f56e01724573923e19763bb0da62>`_  2019-12-17   ``[AIRFLOW-6083] Adding ability to pass custom configuration to lambda client. (#6678)``
`4fb498f87e <https://github.com/apache/airflow/commit/4fb498f87ef89acc30f2576ebc5090ab0653159e>`_  2019-12-09   ``[AIRFLOW-6072] aws_hook: Outbound http proxy setting and other enhancements (#6686)``
`a1e2f86352 <https://github.com/apache/airflow/commit/a1e2f863526973b17892ec31caf09eded95c1cd2>`_  2019-11-20   ``[AIRFLOW-6021] Replace list literal with list constructor (#6617)``
`baae140847 <https://github.com/apache/airflow/commit/baae140847cdf9d84e905fb6d1f119d6950eecf9>`_  2019-11-19   ``[AIRFLOW-5781] AIP-21 Migrate AWS Kinesis to /providers/amazon/aws (#6588)``
`504cfbac1a <https://github.com/apache/airflow/commit/504cfbac1a4ec2e2fd169523ed357808f63881bb>`_  2019-11-18   ``[AIRFLOW-5783] AIP-21 Move aws redshift into providers structure (#6539)``
`992f0e3acf <https://github.com/apache/airflow/commit/992f0e3acf11163294508858515a5f79116e3ad8>`_  2019-11-12   ``AIRFLOW-5824: AWS DataSync Hook and Operators added (#6512)``
`c015eb2f64 <https://github.com/apache/airflow/commit/c015eb2f6496b9721afda9e85d5d4af3bbe0696b>`_  2019-11-10   ``[AIRFLOW-5786] Migrate AWS SNS to /providers/amazon/aws (#6502)``
`3d76fb4bf2 <https://github.com/apache/airflow/commit/3d76fb4bf25e5b7d3d30e0d64867b5999b77f0b0>`_  2019-11-09   ``[AIRFLOW-5782] Migrate AWS Lambda to /providers/amazon/aws [AIP-21] (#6518)``
=================================================================================================  ===========  ======================================================================================================================================================================
