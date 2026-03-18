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
import { createListCollection } from "@chakra-ui/react/collection";
import type { TFunction } from "i18next";

export const createDagSortOptions = (translate: TFunction) =>
  createListCollection({
    items: [
      {
        label: translate("sort.displayName.asc"),
        value: "dag_display_name",
      },
      {
        label: translate("sort.displayName.desc"),
        value: "-dag_display_name",
      },
      {
        label: translate("sort.nextDagRun.asc"),
        value: "next_dagrun",
      },
      {
        label: translate("sort.nextDagRun.desc"),
        value: "-next_dagrun",
      },
      {
        label: translate("sort.lastRunState.asc"),
        value: "last_run_state",
      },
      {
        label: translate("sort.lastRunState.desc"),
        value: "-last_run_state",
      },
      {
        label: translate("sort.lastRunStartDate.asc"),
        value: "last_run_start_date",
      },
      {
        label: translate("sort.lastRunStartDate.desc"),
        value: "-last_run_start_date",
      },
    ],
  });
