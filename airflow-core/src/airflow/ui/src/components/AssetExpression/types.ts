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

type Asset = {
  asset: {
    group: string;
    id: number;
    name: string;
    uri: string;
  };
};

type Alias = {
  alias: {
    group: string;
    name: string;
  };
};

export type NextRunEvent = { id: number; lastUpdate: string | null; name: string | null; uri: string };

export type AssetSummary = Alias | Asset;

export type ExpressionType =
  | Alias
  | Asset
  | {
      all?: Array<AssetSummary | ExpressionType>;
      any?: Array<AssetSummary | ExpressionType>;
    };
