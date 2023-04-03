<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

| Package    | Version | SHA256                                                           |
|------------|---------|------------------------------------------------------------------|
| google-ads | 20.0.0  | e9031119833a1c9e1fe5bf1bd61d9b36313dcb3b7fc6ab6220f34f03ab1f1a5f |


 * Google Ads were imported with cut "google.ads" prefix:
   `import google.ads.googleads` becomes `import airflow.providers.google_vendor.googleads`
 * Only v12 version of the API was imported, v11 and v13 were removed from the initial commit
