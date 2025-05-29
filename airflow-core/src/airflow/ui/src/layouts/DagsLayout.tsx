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
import { Box } from "@chakra-ui/react";
import type { PropsWithChildren } from "react";
import { useTranslation } from "react-i18next";

import { NavTabs } from "./Details/NavTabs";

export const DagsLayout = ({ children }: PropsWithChildren) => {
  const { t: translate } = useTranslation("common");

  const tabs = [
    { label: translate("nav.dags"), value: "/dags" },
    { label: translate("dagRun_other"), value: "/dag_runs" },
    { label: translate("taskInstance_other"), value: "/task_instances" },
  ];

  return (
    <Box>
      <NavTabs tabs={tabs} />
      {children}
    </Box>
  );
};
