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

## Additional info

### Breaking change in `AutoMLBatchPredictOperator`
Class `AutoMLBatchPredictOperator` property `params` is renamed to `prediction_params`.
To keep old behaviour, please rename `params` to `prediction_params` when initializing an instance of `AutoMLBatchPredictOperator`.

Property `params` still exists, but as a property inherited from parent's class `BaseOperator`.
Property `params` has nothing to do with prediction, use `prediction_params` instead.
