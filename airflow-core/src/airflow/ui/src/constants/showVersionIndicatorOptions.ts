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
import { createListCollection } from "@chakra-ui/react";

export enum VersionIndicatorOptions {
  ALL = "all",
  BUNDLE_VERSION = "bundle",
  DAG_VERSION = "dag",
  NONE = "none",
}

const validOptions = new Set<string>(Object.values(VersionIndicatorOptions));

export const isVersionIndicatorOption = (value: unknown): value is VersionIndicatorOptions =>
  typeof value === "string" && validOptions.has(value);

export const showVersionIndicatorOptions = createListCollection({
  items: [
    { label: "dag:panel.showVersionIndicator.options.showAll", value: VersionIndicatorOptions.ALL },
    {
      label: "dag:panel.showVersionIndicator.options.showBundleVersion",
      value: VersionIndicatorOptions.BUNDLE_VERSION,
    },
    {
      label: "dag:panel.showVersionIndicator.options.showDagVersion",
      value: VersionIndicatorOptions.DAG_VERSION,
    },
    { label: "dag:panel.showVersionIndicator.options.hideAll", value: VersionIndicatorOptions.NONE },
  ],
});
