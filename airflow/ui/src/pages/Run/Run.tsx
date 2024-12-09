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
import { LiaSlashSolid } from "react-icons/lia";
import { useParams, Link as RouterLink } from "react-router-dom";

import {
  useDagRunServiceGetDagRun,
  useDagServiceGetDagDetails,
} from "openapi/queries";
import { Breadcrumb } from "src/components/ui";
import { DetailsLayout } from "src/layouts/Details/DetailsLayout";

import { Header } from "./Header";

const tabs = [
  { label: "Task Instances", value: "" },
  { label: "Events", value: "events" },
  { label: "Code", value: "code" },
];

export const Run = () => {
  const { dagId = "", runId = "" } = useParams();

  const {
    data: dagRun,
    error,
    isLoading,
  } = useDagRunServiceGetDagRun({
    dagId,
    dagRunId: runId,
  });

  const {
    data: dag,
    error: dagError,
    isLoading: isLoadinDag,
  } = useDagServiceGetDagDetails({
    dagId,
  });

  return (
    <DetailsLayout
      dag={dag}
      error={error ?? dagError}
      isLoading={isLoading || isLoadinDag}
      tabs={tabs}
    >
      <Breadcrumb.Root mb={3} separator={<LiaSlashSolid />}>
        <Breadcrumb.Link asChild color="fg.info">
          <RouterLink to="/dags">Dags</RouterLink>
        </Breadcrumb.Link>
        <Breadcrumb.Link asChild color="fg.info">
          <RouterLink to={`/dags/${dagId}`}>
            {dag?.dag_display_name ?? dagId}
          </RouterLink>
        </Breadcrumb.Link>
        <Breadcrumb.CurrentLink>{runId}</Breadcrumb.CurrentLink>
      </Breadcrumb.Root>
      {dagRun === undefined ? undefined : <Header dagRun={dagRun} />}
    </DetailsLayout>
  );
};
