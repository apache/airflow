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

export const dagSortOptions = createListCollection({
  items: [
    { label: "Sort by Display Name (A-Z)", value: "dag_display_name" },
    { label: "Sort by Display Name (Z-A)", value: "-dag_display_name" },
    { label: "Sort by Next DAG Run (Earliest-Latest)", value: "next_dagrun" },
    { label: "Sort by Next DAG Run (Latest-Earliest)", value: "-next_dagrun" },
    { label: "Sort by Latest Run State (A-Z)", value: "last_run_state" },
    { label: "Sort by Latest Run State (Z-A)", value: "-last_run_state" },
    {
      label: "Sort by Latest Run Start Date (Earliest-Latest)",
      value: "last_run_start_date",
    },
    {
      label: "Sort by Latest Run Start Date (Latest-Earliest)",
      value: "-last_run_start_date",
    },
  ],
});
